package services

// DAS service module
// combined service module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// global variables used in this module
var _phedexNodes PhedexNodes

// Dataset4SiteRelease returns dataset for given site and release
func (LocalAPIs) Dataset4SiteRelease(dasquery dasql.DASQuery) []mongo.DASRecord {
	return dataset4siteRelease(dasquery)
}

// Dataset4SiteReleaseParent returns dataset for given site release parent
func (LocalAPIs) Dataset4SiteReleaseParent(dasquery dasql.DASQuery) []mongo.DASRecord {
	return dataset4siteRelease(dasquery)
}

// Child4SiteReleaseDataset returns child dataset for site, release and dataset
func (LocalAPIs) Child4SiteReleaseDataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	inst := dasquery.Instance
	var out []mongo.DASRecord
	// find children of given dataset
	dataset := spec["dataset"].(string)
	release := spec["release"].(string)
	site := spec["site"].(string)
	api := "datasetchildren"
	furl := fmt.Sprintf("%s/%s?dataset=%s", DBSUrl(inst), api, dataset)
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	// collect dbs urls to fetch versions for given set of datasets
	api = "releaseversions"
	var dbsUrls []string
	for _, rec := range records {
		if rec["child_dataset"] == nil {
			continue
		}
		dataset := rec["child_dataset"].(string)
		furl = fmt.Sprintf("%s/%s?dataset=%s", DBSUrl(inst), api, dataset)
		if !utils.InList(furl, dbsUrls) {
			dbsUrls = append(dbsUrls, furl)
		}
	}
	var datasets []string
	// collect children datasets
	for _, rec := range processUrls("dbs3", api, dbsUrls) {
		if rec["url"] == nil {
			continue
		}
		url := rec["url"].(string)
		furl = fmt.Sprintf("%s/%s?dataset=", DBSUrl(inst), api)
		dataset := strings.Trim(url, furl)
		if !strings.HasPrefix(dataset, "/") {
			dataset = fmt.Sprintf("/%s", dataset)
		}
		if rec["release_version"] == nil {
			continue
		}
		for _, rel := range rec["release_version"].([]interface{}) {
			if rel == nil {
				continue
			}
			if rel.(string) == release {
				datasets = append(datasets, dataset)
			}
		}
	}
	// create list of PhEDEx urls with given set of datasets and phedex node
	api = "blockReplicas"
	node := phedexNode(site)
	var phedexUrls []string
	for _, dataset := range datasets {
		furl = fmt.Sprintf("%s/%s?dataset=%s&%s", PhedexUrl(), api, dataset, node)
		if !utils.InList(furl, phedexUrls) {
			phedexUrls = append(phedexUrls, furl)
		}
	}
	var datasetsAtSite []string
	// filter children on given site
	for _, rec := range processUrls("phedex", api, phedexUrls) {
		if rec["name"] == nil {
			continue
		}
		block := rec["name"].(string)
		dataset := strings.Split(block, "#")[0]
		if !utils.InList(dataset, datasetsAtSite) {
			datasetsAtSite = append(datasetsAtSite, dataset)
		}
	}
	// prepare final records
	for _, d := range datasetsAtSite {
		rec := make(mongo.DASRecord)
		rec["name"] = d
		out = append(out, rec)
	}
	return out
}

func rec2num(rec interface{}) int64 {
	var out int64
	switch val := rec.(type) {
	case int64:
		out = val
	case json.Number:
		v, e := val.Int64()
		if e != nil {
			log.Println("Unable to convert json.Number to int64", rec, e)
		}
		out = v
	}
	return out
}

// Site4Block returns site info for given block
func (LocalAPIs) Site4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	var out []mongo.DASRecord
	spec := dasquery.Spec
	block := spec["block"].(string)
	// Phedex part find block replicas for given dataset
	api := "blockReplicas"
	furl := fmt.Sprintf("%s/%s?block=%s", PhedexUrl(), api, url.QueryEscape(block))
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "") // "" specify optional args
	records := PhedexUnmarshal(api, resp.Data)
	for _, rec := range records {
		if rec["replica"] == nil {
			continue
		}
		replicas := rec["replica"].([]interface{})
		rec := make(mongo.DASRecord)
		for _, val := range replicas {
			row := val.(map[string]interface{})
			var node, se string
			switch v := row["node"].(type) {
			case string:
				node = v
			}
			switch v := row["se"].(type) {
			case string:
				se = v
			}
			rec["site"] = []mongo.DASRecord{{"name": node, "se": se}}
			out = append(out, rec)
		}
	}
	return out
}

// Replica Rucio data structure
type Replica struct {
	Site    string
	Kind    string
	ALength float64
	Length  float64
}

// Block Rucio data structure
type Block struct {
	Name     string
	Replicas []Replica
	Files    []string
}

// helper function to get back block name from rucio url
func getBlockNameFromUrl(rurl string) string {
	var blk string
	if strings.Contains(rurl, "replicas/cms/") {
		// url/replicas/cms/blk/datasets
		parts := strings.Split(rurl, "replicas/cms/")
		if len(parts) > 1 {
			arr := strings.Split(parts[1], "/datasets")
			blk = arr[0]
		}
	} else if strings.Contains(rurl, "dids/cms/") {
		// url/dids/cms/blk/dids
		parts := strings.Split(rurl, "dids/cms/")
		if len(parts) > 1 {
			arr := strings.Split(parts[1], "/dids")
			blk = arr[0]
		}
	}
	b, err := url.QueryUnescape(blk)
	if err == nil {
		return b
	}
	return blk
}

// helper function to determine RSE type from its name
func kindType(rse string) string {
	name := strings.ToLower(rse)
	if strings.Contains(name, "_tape") || strings.Contains(name, "_mss") || strings.Contains(name, "_export") {
		return "TAPE"
	}
	return "DISK"
}

func rucioInfo(dasquery dasql.DASQuery, blockNames []string) (mongo.DASRecord, map[string]Block) {
	// our output
	blocks := make(map[string]Block)

	// loop for every block and request replicas and files info
	var furl string
	chout := make(chan utils.ResponseType)
	defer close(chout)
	umap := map[string]int{}
	client := utils.HttpClient()
	for _, blkName := range blockNames {
		blocks[blkName] = Block{Name: blkName}

		// http://cms-rucio.cern.ch/replicas/cms/{block['name']}/datasets
		furl = fmt.Sprintf("%s/replicas/cms/%s/datasets", RucioUrl(), url.QueryEscape(blkName))
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(client, furl, "", chout)
	}

	// collect results from block URL calls
	sDict := make(map[string]string)
	exit := false
	for {
		select {
		case r := <-chout:
			records := RucioUnmarshal(dasquery, "full_record", r.Data)
			// get block name from r.URL
			blkName := getBlockNameFromUrl(r.Url)
			for _, rec := range records {
				if rec == nil {
					continue
				}
				bRecord := blocks[blkName]
				// collect block replicas info
				// {"accessed_at": null, "name": "blk_name", "rse": "T2_US_Purdue", "created_at": "Thu, 07 May 2020 08:49:50 UTC", "bytes": 4594317, "state": "AVAILABLE", "updated_at": "Tue, 30 Jun 2020 19:05:27 UTC", "available_length": 1, "length": 1, "scope": "cms", "available_bytes": 4594317, "rse_id": "be0c1696016e4297a1573425d4a9b0a6"}
				var rse string
				if rec["rse"] != nil {
					rse = rec["rse"].(string)
				}
				kind := kindType(rse)
				sDict[rse] = kind
				// replicas dict contains rse, available_length, length
				var aLength, length float64
				if rec["available_length"] != nil {
					aLength = rec["available_length"].(float64)
				}
				if rec["length"] != nil {
					length = rec["length"].(float64)
				}
				replica := Replica{Site: rse, ALength: aLength, Length: length, Kind: kind}
				replicas := bRecord.Replicas
				replicas = append(replicas, replica)
				bRecord.Replicas = replicas
				blocks[blkName] = bRecord
			}
			// remove from umap, indicate that we processed it
			delete(umap, r.Url) // remove Url from map
		default:
			if len(umap) == 0 { // no more requests, merge data records
				exit = true
			}
			time.Sleep(time.Duration(1) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	// construct siteInfo dict
	siteInfo := make(mongo.DASRecord)
	for se, kind := range sDict {
		blockCount := 0
		blockPresent := 0
		blockComplete := 0
		blockFileCount := 0
		availableFileCount := 0
		for _, b := range blocks {
			blockCount += 1
			for _, r := range b.Replicas {
				if se != r.Site {
					continue
				}
				blockPresent += 1
				if r.ALength == r.Length {
					blockComplete += 1
				}
				blockFileCount += int(r.Length)
				availableFileCount += int(r.ALength)
			}
		}
		rec := mongo.DASRecord{"files": 0, "blocks": int64(blockCount), "block_present": int64(blockPresent), "block_complete": int64(blockComplete), "block_file_count": int64(blockFileCount), "available_file_count": int64(availableFileCount), "kind": kind, "se": se}
		siteInfo[se] = rec
	}
	if utils.VERBOSE > 0 {
		data, _ := json.MarshalIndent(siteInfo, "", "  ")
		if utils.WEBSERVER == 0 {
			fmt.Println("siteInfo", string(data))
		} else {
			log.Println("siteInfo", string(data))
		}
	}
	return siteInfo, blocks

}

func rucioInfoMID(dasquery dasql.DASQuery, blockNames []string) (mongo.DASRecord, map[string]Block) {
	// our output
	blocks := make(map[string]Block)

	// loop for every block and request replicas and files info
	var furl string
	chout := make(chan utils.ResponseType)
	defer close(chout)
	umap := map[string]int{}
	client := utils.HttpClient()
	for _, blkName := range blockNames {
		blocks[blkName] = Block{Name: blkName}

		// http://cms-rucio.cern.ch/replicas/cms/{block['name']}/datasets
		furl = fmt.Sprintf("%s/replicas/cms/%s/datasets", RucioUrl(), url.QueryEscape(blkName))
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(client, furl, "", chout)

		// http://cms-rucio.cern.ch/dids/cms/{block['name']}/dids
		furl = fmt.Sprintf("%s/dids/cms/%s/dids", RucioUrl(), url.QueryEscape(blkName))
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(client, furl, "", chout)
	}

	// collect results from block URL calls
	sDict := make(map[string]string)
	exit := false
	for {
		select {
		case r := <-chout:
			records := RucioUnmarshal(dasquery, "full_record", r.Data)
			// get block name from r.URL
			blkName := getBlockNameFromUrl(r.Url)
			for _, rec := range records {
				bRecord := blocks[blkName]
				if strings.Contains(r.Url, "replicas/cms") {
					// collect block replicas info
					// {"accessed_at": null, "name": "blk_name", "rse": "T2_US_Purdue", "created_at": "Thu, 07 May 2020 08:49:50 UTC", "bytes": 4594317, "state": "AVAILABLE", "updated_at": "Tue, 30 Jun 2020 19:05:27 UTC", "available_length": 1, "length": 1, "scope": "cms", "available_bytes": 4594317, "rse_id": "be0c1696016e4297a1573425d4a9b0a6"}
					rse := rec["rse"].(string)
					kind := kindType(rse)
					sDict[rse] = kind
					// replicas dict contains rse, available_length, length
					aLength := rec["available_length"].(float64)
					length := rec["length"].(float64)
					replica := Replica{Site: rse, ALength: aLength, Length: length, Kind: kind}
					replicas := bRecord.Replicas
					replicas = append(replicas, replica)
					bRecord.Replicas = replicas
					blocks[blkName] = bRecord
				} else if strings.Contains(r.Url, "dids/cms") {
					// collect block file info
					// {"adler32": "5e3fa286", "name": "file.root", "bytes": 4594317, "scope": "cms", "type": "FILE", "md5": null}
					fname := rec["name"].(string)
					files := bRecord.Files
					files = append(files, fname)
					bRecord.Files = files
					blocks[blkName] = bRecord
				}
			}
			// remove from umap, indicate that we processed it
			delete(umap, r.Url) // remove Url from map
		default:
			if len(umap) == 0 { // no more requests, merge data records
				exit = true
			}
			time.Sleep(time.Duration(1) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	// construct siteInfo dict
	siteInfo := make(mongo.DASRecord)
	for se, kind := range sDict {
		blockCount := 0
		blockPresent := 0
		blockComplete := 0
		fileCount := 0
		blockFileCount := 0
		availableFileCount := 0
		for _, b := range blocks {
			blockCount += 1
			fileCount += len(b.Files)
			for _, r := range b.Replicas {
				if se != r.Site {
					continue
				}
				blockPresent += 1
				if r.ALength == r.Length {
					blockComplete += 1
				}
				blockFileCount += int(r.Length)
				availableFileCount += int(r.ALength)
			}
		}
		rec := mongo.DASRecord{"files": int64(fileCount), "blocks": int64(blockCount), "block_present": int64(blockPresent), "block_complete": int64(blockComplete), "block_file_count": int64(blockFileCount), "available_file_count": int64(availableFileCount), "kind": kind, "se": se}
		siteInfo[se] = rec
	}
	return siteInfo, blocks

}

// Site4DatasetPct returns site info for given dataset
func (LocalAPIs) Site4DatasetPct(dasquery dasql.DASQuery) []mongo.DASRecord {

	spec := dasquery.Spec
	inst := dasquery.Instance
	// DBS part, find total number of blocks and files for given dataset
	dataset := spec["dataset"].(string)
	api := "filesummaries"
	furl := fmt.Sprintf("%s/%s?dataset=%s&validFileOnly=1", DBSUrl(inst), api, dataset)
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	var totblocks, totfiles int64
	if len(records) == 0 {
		return []mongo.DASRecord{}
	}
	totblocks = rec2num(records[0]["num_block"])
	totfiles = rec2num(records[0]["num_file"])

	// to proceed with Rucio we need to know all blocks for a given dataset
	// we obtain this list from DBS
	api = "blocks"
	furl = fmt.Sprintf("%s/%s?dataset=%s", DBSUrl(inst), api, dataset)
	resp = utils.FetchResponse(client, furl, "") // "" specify optional args
	records = DBSUnmarshal(api, resp.Data)
	var blocks []string
	for _, rec := range records {
		brec := rec["block_name"]
		if brec != nil {
			blk := rec["block_name"].(string)
			blocks = append(blocks, blk)
		}
	}

	// obtan Rucio information
	siteInfo, _ := rucioInfo(dasquery, blocks)

	// construct final representation for sites
	var pfiles, pblks string
	var out []mongo.DASRecord
	for key, val := range siteInfo {
		row := val.(mongo.DASRecord)
		nfiles := rec2num(row["files"])
		if totfiles > 0 {
			pfiles = fmt.Sprintf("%5.2f%%", 100*float64(nfiles)/float64(totfiles))
		} else {
			pfiles = "N/A"
			pblks = "N/A"
		}
		if totblocks > 0 {
			nblks := rec2num(row["blocks"])
			pblks = fmt.Sprintf("%5.2f%%", 100*float64(nblks)/float64(totblocks))
		} else {
			pfiles = "N/A"
			pblks = "N/A"
		}
		ratio := float64(rec2num(row["block_present"])) / float64(rec2num(row["blocks"]))
		bc := fmt.Sprintf("%5.2f%%", 100*ratio)
		ratio = float64(rec2num(row["available_file_count"])) / float64(rec2num(row["block_file_count"]))
		rf := fmt.Sprintf("%5.2f%%", 100*ratio)
		if utils.VERBOSE > 0 {
			if utils.WEBSERVER == 0 {
				fmt.Printf("site: %s siteInfo: %+v\n", key, row)
			} else {
				log.Printf("site: %s siteInfo: %+v\n", key, row)
			}
		}
		// put into file das record, internal type must be list
		rec := make(mongo.DASRecord)
		rec["site"] = []mongo.DASRecord{{"name": key,
			"dataset_fraction": pfiles, "block_fraction": pblks, "block_completion": bc,
			"se": row["se"].(string), "replica_fraction": rf, "kind": row["kind"].(string)}}
		out = append(out, rec)
	}
	return out
}

// Site4Dataset returns site info for given dataset
func (LocalAPIs) Site4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	return []mongo.DASRecord{}
}

// Site4Dataset_phedex returns site info for given dataset
func (LocalAPIs) Site4Dataset_phedex(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	inst := dasquery.Instance
	// DBS part, find total number of blocks and files for given dataset
	dataset := spec["dataset"].(string)
	api := "filesummaries"
	furl := fmt.Sprintf("%s/%s?dataset=%s&validFileOnly=1", DBSUrl(inst), api, dataset)
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	var totblocks, totfiles int64
	if len(records) == 0 {
		return []mongo.DASRecord{}
	}
	totblocks = rec2num(records[0]["num_block"])
	totfiles = rec2num(records[0]["num_file"])
	// Phedex part find block replicas for given dataset
	api = "blockReplicas"
	furl = fmt.Sprintf("%s/%s?dataset=%s", PhedexUrl(), api, dataset)
	resp = utils.FetchResponse(client, furl, "") // "" specify optional args
	records = PhedexUnmarshal(api, resp.Data)
	siteInfo := make(mongo.DASRecord)
	var bComplete, nfiles, nblks, bfiles int64
	bfiles = 0
	for _, rec := range records {
		if rec["files"] == nil || rec["replica"] == nil {
			continue
		}
		bfiles += rec2num(rec["files"])
		replicas := rec["replica"].([]interface{})
		for _, val := range replicas {
			row := val.(map[string]interface{})
			var node, se, complete string
			switch v := row["node"].(type) {
			case string:
				node = v
			}
			switch v := row["se"].(type) {
			case string:
				se = v
			}
			switch v := row["complete"].(type) {
			case string:
				complete = v
			}
			if complete == "y" {
				bComplete = 1
			} else {
				bComplete = 0
			}
			nfiles = rec2num(row["files"])
			skeys := utils.MapKeys(siteInfo)
			if utils.InList(node, skeys) {
				sInfo := siteInfo[node].(mongo.DASRecord)
				nfiles += rec2num(sInfo["files"])
				nblks = rec2num(sInfo["blocks"]) + 1
				bc := rec2num(sInfo["block_complete"])
				if complete == "y" {
					bComplete = bc + 1
				} else {
					bComplete = bc
				}
			} else {
				nblks = 1
			}
			siteInfo[node] = mongo.DASRecord{"files": nfiles, "blocks": nblks, "block_complete": bComplete, "se": se, "kind": _phedexNodes.NodeType(node)}
		}
	}
	//     if utils.VERBOSE > 0 {
	//         fmt.Println("### bfiles", bfiles)
	//         for s, v := range siteInfo {
	//             fmt.Println("### site", s, v)
	//         }
	//     }
	var pfiles, pblks string
	var out []mongo.DASRecord
	for key, val := range siteInfo {
		row := val.(mongo.DASRecord)
		nfiles := rec2num(row["files"])
		if totfiles > 0 {
			pfiles = fmt.Sprintf("%5.2f%%", 100*float64(nfiles)/float64(totfiles))
		} else {
			pfiles = "N/A"
			pblks = "N/A"
		}
		if totblocks > 0 {
			nblks := rec2num(row["blocks"])
			pblks = fmt.Sprintf("%5.2f%%", 100*float64(nblks)/float64(totblocks))
		} else {
			pfiles = "N/A"
			pblks = "N/A"
		}
		ratio := float64(rec2num(row["block_complete"])) / float64(rec2num(row["blocks"]))
		bc := fmt.Sprintf("%5.2f%%", 100*ratio)
		rf := fmt.Sprintf("%5.2f%%", 100*float64(nfiles)/float64(bfiles))
		if utils.VERBOSE > 0 {
			fmt.Println("### site", key, "nfiles", nfiles, "bfiles", bfiles)
		}
		// put into file das record, internal type must be list
		rec := make(mongo.DASRecord)
		rec["site"] = []mongo.DASRecord{{"name": key,
			"dataset_fraction": pfiles, "block_fraction": pblks, "block_completion": bc,
			"se": row["se"].(string), "replica_fraction": rf, "kind": row["kind"].(string)}}
		out = append(out, rec)
	}
	return out
}

// helper function to filter files which belong to given site
func filterFiles(files []string, site string) []string {
	var out, urls []string
	api := "fileReplicas"
	node := phedexNode(site)
	for _, fname := range files {
		furl := fmt.Sprintf("%s/%s?lfn=%s&%s", PhedexUrl(), api, fname, node)
		urls = append(urls, furl)
	}
	for _, rec := range processUrls("phedex", api, urls) {
		if rec["name"] == nil {
			continue
		}
		fname := rec["name"].(string)
		out = append(out, fname)
	}
	return out
}

// helper function to get list of files for given dataset/block and run/site
func files4dbRunsSite(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	api := "files"
	urls := dbsUrls(dasquery, api)
	files := processUrls("dbs3", api, urls)
	var fileList []string
	for _, rec := range files {
		if rec != nil && rec["logical_file_name"] != nil {
			fname := rec["logical_file_name"].(string)
			fileList = append(fileList, fname)
		}
	}
	// check files in Phedex for give site (should take it form spec)
	site := spec["site"].(string)
	var dataset string
	if v, ok := spec["dataset"]; ok {
		dataset = v.(string)
	} else if v, ok := spec["block"]; ok {
		dataset = strings.Split(v.(string), "#")[0]
	}
	//     for _, fname := range filterFiles(fileList, site) {
	for _, fname := range filterFilesInRucio(dasquery, fileList, dataset, site) {
		row := make(mongo.DASRecord)
		// put into file das record, internal type must be list
		row["file"] = []mongo.DASRecord{{"name": fname}}
		out = append(out, row)
	}
	return out
}

// Files4DatasetRunsSite combined APIs to lookup file list for give dataset/run/site
func (LocalAPIs) Files4DatasetRunsSite(dasquery dasql.DASQuery) []mongo.DASRecord {
	return files4dbRunsSite(dasquery)
}

// Files4BlockRunsSite combined APIs to lookup file list for give block/run/site
func (LocalAPIs) Files4BlockRunsSite(dasquery dasql.DASQuery) []mongo.DASRecord {
	return files4dbRunsSite(dasquery)
}

type RucioRecordRSE struct {
	DIDs           []map[string]string `json:"dids"`
	Domain         interface{}         `json:"domain"`
	AllStates      bool                `json:"all_states"`
	ResolveArchive bool                `json:"resolve_archive"`
	ResolveParents bool                `json:"resolve_parents"`
	RSE            string              `json:"rse_expression"`
}

// helper function to filter files which belong to given site using Rucio API
func filterFilesInRucio(dasquery dasql.DASQuery, files []string, dataset, site string) []string {
	var out []string
	rec := make(map[string]string)
	rec["name"] = dataset
	rec["scope"] = "cms"
	var dids []map[string]string
	dids = append(dids, rec)
	spec := RucioRecordRSE{DIDs: dids, Domain: nil, RSE: site}
	// make POST request to Rucio to obtain list of files for given RSE request record
	args, err := json.Marshal(spec)
	if err != nil {
		log.Printf("ERROR: unable to unmarshal spec %+v, error %v\n", spec, err)
		return out
	}
	furl := fmt.Sprintf("%s/replicas/list", RucioUrl())
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, string(args)) // POST request
	records := RucioUnmarshal(dasquery, "full_record", resp.Data)
	for _, r := range records {
		if v, ok := r["name"]; ok {
			fname := v.(string)
			if utils.FindInList(fname, files) {
				out = append(out, fname)
			}
		}
	}
	return out
}

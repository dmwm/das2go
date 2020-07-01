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
	resp := utils.FetchResponse(furl, "") // "" specify optional args
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
	resp := utils.FetchResponse(furl, "") // "" specify optional args
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

// Site4DatasetPct returns site info for given dataset
func (LocalAPIs) Site4DatasetPct(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	inst := dasquery.Instance
	// DBS part, find total number of blocks and files for given dataset
	dataset := spec["dataset"].(string)
	api := "filesummaries"
	furl := fmt.Sprintf("%s/%s?dataset=%s&validFileOnly=1", DBSUrl(inst), api, dataset)
	resp := utils.FetchResponse(furl, "") // "" specify optional args
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
	resp = utils.FetchResponse(furl, "") // "" specify optional args
	records = DBSUnmarshal(api, resp.Data)
	var blocks []string
	for _, rec := range records {
		brec := rec["block_name"]
		if brec != nil {
			blk := rec["block_name"].(string)
			blocks = append(blocks, blk)
		}
	}

	// Rucio part I: we obtain list of file replicas for our dataset
	// the following rucio api gives list of file records
	// https://cms-rucio.cern.ch/replicas/cms/$dataset
	// we create siteFileInfo dict which keeps number of files per site
	api = "site4dataset"
	furl = fmt.Sprintf("%s/replicas/cms/%s", RucioUrl(), url.QueryEscape(dataset))
	resp = utils.FetchResponse(furl, "") // "" specify optional args
	records = RucioUnmarshal(dasquery, api, resp.Data)
	siteInfo := make(mongo.DASRecord)
	sFileInfo := make(map[string][]interface{})
	siteKindInfo := make(map[string]string)
	for _, rec := range records {
		if rec["rses"] == nil {
			continue
		}
		for rse, ientries := range rec["rses"].(map[string]interface{}) {
			entries := ientries.([]interface{})
			if v, ok := sFileInfo[rse]; ok {
				for _, entry := range v {
					entries = append(entries, entry)
				}
			}
			sFileInfo[rse] = entries
		}
		if rec["pfns"] != nil {
			switch record := rec["pfns"].(type) {
			case map[string]interface{}:
				for _, v := range record {
					switch s := v.(type) {
					case map[string]interface{}:
						if s["rse"] != nil {
							rse := s["rse"].(string)
							siteKindInfo[rse] = fmt.Sprintf("%v", s["type"])
						}
					}
				}
			}
		}
	}
	siteFileInfo := make(map[string]int64)
	for k, rseFiles := range sFileInfo {
		rec := make(map[string]int)
		for _, entry := range rseFiles {
			key := entry.(string)
			rec[key] = 1
		}
		siteFileInfo[k] = int64(len(rec))
	}

	// Rucio part II: we obtain information about blocks concurrently
	// the following rucio api gives list of block records at a site
	// https://cms-rucio.cern.ch/replicas/cms/$block/datasets
	// we create siteFileInfo dict which keeps number of files per site
	chout := make(chan utils.ResponseType)
	umap := map[string]int{}
	for _, blk := range blocks {
		furl = fmt.Sprintf("%s/replicas/cms/%s/datasets", RucioUrl(), url.QueryEscape(blk))
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(furl, "", chout)
	}
	api = "site4dataset"
	siteBlockInfo := make(map[string]int64)
	siteBlockCompleteInfo := make(map[string]int64)
	exit := false
	//     var bfiles int64 // count number of available files in all blocks on a site
	for {
		select {
		case r := <-chout:
			records = RucioUnmarshal(dasquery, "full_record", r.Data)
			for _, rec := range records {
				if rec["rse"] == nil {
					continue
				}
				rse := rec["rse"].(string)
				if v, ok := siteBlockInfo[rse]; ok {
					siteBlockInfo[rse] = v + 1
				} else {
					siteBlockInfo[rse] = 1
				}
				var aLength, length int64
				if rec["available_length"] != nil {
					vvv := rec["available_length"]
					switch v := vvv.(type) {
					case float64:
						//                         bfiles += int64(v)
						aLength = int64(v)
					}
				}
				if rec["length"] != nil {
					vvv := rec["length"]
					switch v := vvv.(type) {
					case float64:
						length = int64(v)
					}
				}
				var bComplete int64
				if aLength == length {
					bComplete = 1
				} else {
					bComplete = 0
				}
				if v, ok := siteBlockCompleteInfo[rse]; ok {
					siteBlockCompleteInfo[rse] = v + bComplete
				} else {
					siteBlockCompleteInfo[rse] = bComplete
				}
			}
			// remove from umap, indicate that we processed it
			delete(umap, r.Url) // remove Url from map
		default:
			if len(umap) == 0 { // no more requests, merge data records
				exit = true
			}
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	defer close(chout)
	// construct siteInfo dict from siteFileInfo and siteBlockInfo
	// siteInfo[node] = mongo.DASRecord{"files": nfiles, "blocks": nblks, "block_complete": bComplete, "se": se, "kind": _phedexNodes.NodeType(node)}
	var kind string
	var bComplete, nfiles, nblks int64
	for se, _ := range siteFileInfo {
		bComplete = 0
		nfiles = 0
		nblks = 0
		nfiles = 0
		kind = ""
		if v, ok := siteFileInfo[se]; ok {
			nfiles = v
		}
		if v, ok := siteBlockInfo[se]; ok {
			nblks = v
		}
		if _, ok := siteKindInfo[se]; ok {
			kind = siteKindInfo[se]
		}
		if v, ok := siteBlockCompleteInfo[se]; ok {
			bComplete = v
		}
		siteInfo[se] = mongo.DASRecord{"files": nfiles, "blocks": nblks, "kind": kind, "se": se, "block_complete": bComplete}
	}

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
		//         rf := fmt.Sprintf("%5.2f%%", 100*float64(nfiles)/float64(bfiles))
		rf := fmt.Sprintf("%5.2f%%", 100*float64(nfiles)/float64(nblks))
		if utils.VERBOSE > 0 {
			fmt.Println("### site", key, "nfiles", nfiles, "nblocks", nblks)
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
	spec := dasquery.Spec
	inst := dasquery.Instance
	// DBS part, find total number of blocks and files for given dataset
	dataset := spec["dataset"].(string)
	api := "filesummaries"
	furl := fmt.Sprintf("%s/%s?dataset=%s&validFileOnly=1", DBSUrl(inst), api, dataset)
	resp := utils.FetchResponse(furl, "") // "" specify optional args
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
	resp = utils.FetchResponse(furl, "") // "" specify optional args
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
	if utils.VERBOSE > 0 {
		fmt.Println("### bfiles", bfiles)
		for s, v := range siteInfo {
			fmt.Println("### site", s, v)
		}
	}
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
	for _, fname := range filterFiles(fileList, site) {
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

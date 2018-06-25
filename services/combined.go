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
	furl := fmt.Sprintf("%s/%s?dataset=%s", dbsUrl(inst), api, dataset)
	resp := utils.FetchResponse(furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	// collect dbs urls to fetch versions for given set of datasets
	api = "releaseversions"
	var dbsUrls []string
	for _, rec := range records {
		dataset := rec["child_dataset"].(string)
		furl = fmt.Sprintf("%s/%s?dataset=%s", dbsUrl(inst), api, dataset)
		if !utils.InList(furl, dbsUrls) {
			dbsUrls = append(dbsUrls, furl)
		}
	}
	var datasets []string
	// collect children datasets
	for _, rec := range processUrls("dbs3", api, dbsUrls) {
		url := rec["url"].(string)
		furl = fmt.Sprintf("%s/%s?dataset=", dbsUrl(inst), api)
		dataset := strings.Trim(url, furl)
		if !strings.HasPrefix(dataset, "/") {
			dataset = fmt.Sprintf("/%s", dataset)
		}
		for _, rel := range rec["release_version"].([]interface{}) {
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
		furl = fmt.Sprintf("%s/%s?dataset=%s&%s", phedexUrl(), api, dataset, node)
		if !utils.InList(furl, phedexUrls) {
			phedexUrls = append(phedexUrls, furl)
		}
	}
	var datasetsAtSite []string
	// filter children on given site
	for _, rec := range processUrls("phedex", api, phedexUrls) {
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
	furl := fmt.Sprintf("%s/%s?block=%s", phedexUrl(), api, url.QueryEscape(block))
	resp := utils.FetchResponse(furl, "") // "" specify optional args
	records := PhedexUnmarshal(api, resp.Data)
	for _, rec := range records {
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

// Site4Dataset returns site info for given dataset
func (LocalAPIs) Site4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	inst := dasquery.Instance
	// DBS part, find total number of blocks and files for given dataset
	dataset := spec["dataset"].(string)
	api := "filesummaries"
	furl := fmt.Sprintf("%s/%s?dataset=%s&validFileOnly=1", dbsUrl(inst), api, dataset)
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
	furl = fmt.Sprintf("%s/%s?dataset=%s", phedexUrl(), api, dataset)
	resp = utils.FetchResponse(furl, "") // "" specify optional args
	records = PhedexUnmarshal(api, resp.Data)
	siteInfo := make(mongo.DASRecord)
	var bComplete, nfiles, nblks, bfiles int64
	bfiles = 0
	for _, rec := range records {
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
	var pfiles, pblks string
	var out []mongo.DASRecord
	for key, val := range siteInfo {
		row := val.(mongo.DASRecord)
		if totfiles > 0 {
			nfiles := rec2num(row["files"])
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
		// put into file das record, internal type must be list
		rec := make(mongo.DASRecord)
		rec["site"] = []mongo.DASRecord{{"name": key,
			"dataset_fraction": pfiles, "block_fraction": pblks, "block_completion": bc,
			"se": row["se"].(string), "replica_fraction": rf, "kind": row["kind"].(string)}}
		out = append(out, rec)
	}
	return out
}

// Lumi4Dataset returns lumi info for given dataset
func (LocalAPIs) Lumi4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	var out []mongo.DASRecord
	out = append(out, mongo.DASErrorRecord("combined_lumi4dataset API is not implemented", utils.CombinedErrorName, utils.CombinedError))
	return out
}

// helper function to filter files which belong to given site
func filterFiles(files []string, site string) []string {
	var out, urls []string
	api := "fileReplicas"
	node := phedexNode(site)
	for _, fname := range files {
		furl := fmt.Sprintf("%s/%s?lfn=%s&%s", phedexUrl(), api, fname, node)
		urls = append(urls, furl)
	}
	for _, rec := range processUrls("phedex", api, urls) {
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
		fname := rec["logical_file_name"].(string)
		fileList = append(fileList, fname)
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

package services

// DAS service module
// combined service module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"fmt"
	"github.com/vkuznet/das2go/dasql"
	"github.com/vkuznet/das2go/mongo"
	"github.com/vkuznet/das2go/utils"
	"strings"
)

// global variables used in this module
var _phedexNodes PhedexNodes

// L_combined_dataset4site_release returns dataset for given site and release
func (LocalAPIs) L_combined_dataset4site_release(dasquery dasql.DASQuery) []mongo.DASRecord {
	return dataset4siteRelease(dasquery)
}

// L_combined_dataset4site_release_parent returns dataset for given site release parent
func (LocalAPIs) L_combined_dataset4site_release_parent(dasquery dasql.DASQuery) []mongo.DASRecord {
	return dataset4siteRelease(dasquery)
}

// L_combined_child4site_release_dataset returns child dataset for site, release and dataset
func (LocalAPIs) L_combined_child4site_release_dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
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

// L_combined_site4dataset returns site info for given dataset
func (LocalAPIs) L_combined_site4dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	inst := dasquery.Instance
	// DBS part, find total number of blocks and files for given dataset
	dataset := spec["dataset"].(string)
	api := "filesummaries"
	furl := fmt.Sprintf("%s/%s?dataset=%s", dbsUrl(inst), api, dataset)
	resp := utils.FetchResponse(furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	var totblocks, totfiles float64
	totblocks = records[0]["num_block"].(float64)
	totfiles = records[0]["num_file"].(float64)
	// Phedex part find block replicas for given dataset
	api = "blockReplicas"
	furl = fmt.Sprintf("%s/%s?dataset=%s", phedexUrl(), api, dataset)
	resp = utils.FetchResponse(furl, "") // "" specify optional args
	records = PhedexUnmarshal(api, resp.Data)
	siteInfo := make(mongo.DASRecord)
	var b_complete, nfiles, nblks, bfiles float64
	bfiles = 0
	for _, rec := range records {
		bfiles += rec["files"].(float64)
		replicas := rec["replica"].([]interface{})
		for _, val := range replicas {
			row := val.(map[string]interface{})
			node := row["node"].(string)
			se := row["se"].(string)
			complete := row["complete"].(string)
			if complete == "y" {
				b_complete = 1
			} else {
				b_complete = 0
			}
			nfiles = row["files"].(float64)
			skeys := utils.MapKeys(siteInfo)
			if utils.InList(node, skeys) {
				sInfo := siteInfo[node].(mongo.DASRecord)
				nfiles += sInfo["files"].(float64)
				nblks = sInfo["blocks"].(float64) + 1
				bc := sInfo["block_complete"].(float64)
				if complete == "y" {
					b_complete = bc + 1
				} else {
					b_complete = bc
				}
			} else {
				nblks = 1
			}
			siteInfo[node] = mongo.DASRecord{"files": nfiles, "blocks": nblks, "block_complete": b_complete, "se": se, "kind": _phedexNodes.NodeType(node)}
		}
	}
	var pfiles, pblks string
	var out []mongo.DASRecord
	for key, val := range siteInfo {
		row := val.(mongo.DASRecord)
		if totfiles > 0 {
			nfiles := row["files"].(float64)
			pfiles = fmt.Sprintf("%5.2f%%", 100*nfiles/totfiles)
		} else {
			pfiles = "N/A"
			pblks = "N/A"
		}
		if totblocks > 0 {
			nblks := row["blocks"].(float64)
			pblks = fmt.Sprintf("%5.2f%%", 100*nblks/totblocks)
		} else {
			pfiles = "N/A"
			pblks = "N/A"
		}
		ratio := row["block_complete"].(float64) / row["blocks"].(float64)
		bc := fmt.Sprintf("%5.2f%%", 100*ratio)
		rf := fmt.Sprintf("%5.2f%%", 100*nfiles/bfiles)
		// put into file das record, internal type must be list
		rec := make(mongo.DASRecord)
		rec["site"] = []mongo.DASRecord{{"name": key,
			"dataset_fraction": pfiles, "block_fraction": pblks, "block_completion": bc,
			"se": row["se"].(string), "replica_fraction": rf, "kind": row["kind"].(string)}}
		out = append(out, rec)
	}
	return out
}

// L_combined_lumi4dataset returns lumi info for given dataset
func (LocalAPIs) L_combined_lumi4dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	var out []mongo.DASRecord
	out = append(out, mongo.DASErrorRecord("combined_lumi4dataset API is not implemented"))
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
func files4db_runs_site(dasquery dasql.DASQuery) []mongo.DASRecord {
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

// L_combined_files4dataset_runs_site combined APIs to lookup file list for give dataset/run/site
func (LocalAPIs) L_combined_files4dataset_runs_site(dasquery dasql.DASQuery) []mongo.DASRecord {
	return files4db_runs_site(dasquery)
}

// L_combined_files4block_runs_site combined APIs to lookup file list for give block/run/site
func (LocalAPIs) L_combined_files4block_runs_site(dasquery dasql.DASQuery) []mongo.DASRecord {
	return files4db_runs_site(dasquery)
}

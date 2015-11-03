/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: combined services module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"mongo"
	"strings"
	"utils"
)

// combined service APIs
func (LocalAPIs) L_combined_dataset4site_release(spec bson.M) []mongo.DASRecord {
	return dataset4site_release(spec)
}
func (LocalAPIs) L_combined_dataset4site_release_parent(spec bson.M) []mongo.DASRecord {
	return dataset4site_release(spec)
}
func (LocalAPIs) L_combined_child4site_release_dataset(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	// find children of given dataset
	dataset := spec["dataset"].(string)
	release := spec["release"].(string)
	site := spec["site"].(string)
	api := "datasetchildren"
	furl := fmt.Sprintf("%s/%s?dataset=%s", dbsUrl(), api, dataset)
	resp := utils.FetchResponse(furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	// collect only children from given release
	var datasets []string
	for _, rec := range records {
		dataset := rec["child_dataset"].(string)
		api = "releaseversions"
		furl = fmt.Sprintf("%s/%s?dataset=%s", dbsUrl(), api, dataset)
		resp = utils.FetchResponse(furl, "") // "" specify optional args
		for _, row := range DBSUnmarshal(api, resp.Data) {
			for _, rel := range row["release_version"].([]interface{}) {
				if rel.(string) == release {
					datasets = append(datasets, dataset)
				}
			}
		}
	}
	// create list of PhEDEx urls with given set of datasets and phedex node
	api = "blockReplicas"
	node := phedexNode(site)
	var urls []string
	for _, dataset := range datasets {
		furl = fmt.Sprintf("%s/%s?dataset=%s&%s", phedexUrl(), api, dataset, node)
		if !utils.InList(furl, urls) {
			urls = append(urls, furl)
		}
	}
	var datasetsAtSite []string
	// filter children on given site
	for _, rec := range processUrls("phedex", api, urls) {
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
func (LocalAPIs) L_combined_site4dataset(spec bson.M) []mongo.DASRecord {
	// DBS part, find total number of blocks and files for given dataset
	dataset := spec["dataset"].(string)
	api := "filesummaries"
	furl := fmt.Sprintf("%s/%s?dataset=%s", dbsUrl(), api, dataset)
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
				nfiles += siteInfo["files"].(float64)
				nblks = siteInfo["blocks"].(float64) + 1
				bc := siteInfo["block_complete"].(float64)
				if complete == "y" {
					b_complete = bc + 1
				} else {
					b_complete = bc
				}
			} else {
				nblks = 1
			}
			siteInfo[node] = mongo.DASRecord{"files": nfiles, "blocks": nblks, "block_complete": b_complete, "se": se}
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
		rec["site"] = []mongo.DASRecord{mongo.DASRecord{"name": key,
			"dataset_fraction": pfiles, "block_fraction": pblks, "block_completion": bc,
			"se": row["se"].(string), "replica_fraction": rf}}
		out = append(out, rec)
	}
	return out
}

// Seems to me it is too much to look-up, user can use file,lumi or block,run,lumi for dataset APIs
func (LocalAPIs) L_combined_lumi4dataset(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	panic("Not implemented")
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
func files4db_runs_site(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	api := "files"
	urls := dbs_urls(spec, api)
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
		row["file"] = []mongo.DASRecord{mongo.DASRecord{"name": fname}}
		out = append(out, row)
	}
	return out
}

// combined APIs to lookup file list for give dataset/run/site
func (LocalAPIs) L_combined_files4dataset_runs_site(spec bson.M) []mongo.DASRecord {
	return files4db_runs_site(spec)
}

// combined APIs to lookup file list for give block/run/site
func (LocalAPIs) L_combined_files4block_runs_site(spec bson.M) []mongo.DASRecord {
	return files4db_runs_site(spec)
}

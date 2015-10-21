/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: this module contains all helper functions used in DAS services (Local APIs)
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"mongo"
	"net/url"
	"regexp"
	"strings"
	"time"
	"utils"
)

type LocalAPIs struct{}

func dbsUrl() string {
	return "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
}
func phedexUrl() string {
	return "https://cmsweb.cern.ch/phedex/datasvc/json/prod"
}

// Here I list __ONLY__ exceptional apis due to mistake in DAS maps
func DASLocalAPIs() []string {
	out := []string{
		// dbs3 APIs which should be treated as local_api, but they have
		// url: http://.... in their map instead of local_api
		"file_run_lumi4dataset", "file_run_lumi4block",
		"file_lumi4dataset", "file_lumi4block", "run_lumi4dataset", "run_lumi4block",
		"block_run_lumi4dataset", "file4dataset_run_lumi", "blocks4tier_dates",
		"lumi4block_run"}
	return out
}

// helper function to find file,run,lumis for given dataset or block
func find_blocks(spec bson.M) []string {
	var out []string
	blk := spec["block"]
	if blk != nil {
		out = append(out, blk.(string))
		return out
	}
	dataset := spec["dataset"].(string)
	api := "blocks"
	furl := fmt.Sprintf("%s/%s?dataset=%s", dbsUrl(), api, dataset)
	resp := utils.FetchResponse(furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	for _, rec := range records {
		out = append(out, rec["block_name"].(string))
	}
	return out
}

// helper function to process given set of urls and unmarshal results
// from all url calls
func processUrls(system, api string, urls []string) []mongo.DASRecord {
	var out_records []mongo.DASRecord
	out := make(chan utils.ResponseType)
	umap := map[string]int{}
	for _, furl := range urls {
		umap[furl] = 1                // keep track of processed urls below
		go utils.Fetch(furl, "", out) // "" specify optional args
	}
	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			// process data
			var records []mongo.DASRecord
			if system == "dbs3" {
				records = DBSUnmarshal(api, r.Data)
			} else if system == "phedex" {
				records = PhedexUnmarshal(api, r.Data)
			}
			for _, rec := range records {
				rec["url"] = r.Url
				out_records = append(out_records, rec)
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
	return out_records
}

// helper function to get run arguments for given spec
// we extract run parameter from spec and construct run_num arguments for DBS
func runArgs(spec bson.M) string {
	// get runs from spec
	runs := spec["run"]
	runs_args := ""
	if runs != nil {
		switch value := runs.(type) {
		case []string:
			for _, val := range value {
				runs_args = fmt.Sprintf("%s&run_num=%s", runs_args, val)
			}
		case string:
			runs_args = fmt.Sprintf("%s&run_num=%s", runs_args, value)
		default:
			panic(fmt.Sprintf("Unknown type for runs=%s, type=%T", runs, runs))
		}
	}
	return runs_args
}

// helper function to get file status from the spec
func fileStatus(spec bson.M) bool {
	status := spec["status"]
	if status != nil {
		val := status.(string)
		if strings.ToLower(val) == "valid" {
			return true
		}
	}
	return false
}

// helper function to get DBS urls for given spec and api
func dbs_urls(spec bson.M, api string) []string {
	// get runs from spec
	runs_args := runArgs(spec)
	valid_file := fileStatus(spec)

	// find all blocks for given dataset or block
	var urls []string
	for _, blk := range find_blocks(spec) {
		myurl := fmt.Sprintf("%s/%s?block_name=%s", dbsUrl(), api, url.QueryEscape(blk))
		if len(runs_args) > 0 {
			myurl += runs_args // append run arguments
		}
		if valid_file {
			myurl += fmt.Sprintf("&validFileOnly=1") // append validFileOnly=1
		}
		urls = append(urls, myurl)
	}
	return utils.List2Set(urls)
}

// helper function to get file,run,lumi triplets
func file_run_lumi(spec bson.M, fields []string) []mongo.DASRecord {
	var out []mongo.DASRecord

	// use filelumis DBS API output to get
	// run_num, logical_file_name, lumi_secion_num from provided fields
	api := "filelumis"
	urls := dbs_urls(spec, api)
	filelumis := processUrls("dbs3", api, urls)
	for _, rec := range filelumis {
		row := make(mongo.DASRecord)
		for _, key := range fields {
			// put into file das record, internal type must be list
			if key == "run_num" {
				row["run"] = []mongo.DASRecord{mongo.DASRecord{"run_number": rec[key]}}
			} else if key == "lumi_section_num" {
				row["lumi"] = []mongo.DASRecord{mongo.DASRecord{"number": rec[key]}}
			} else if key == "logical_file_name" {
				row["file"] = []mongo.DASRecord{mongo.DASRecord{"name": rec[key]}}
			}
		}
		out = append(out, row)
	}
	return out
}

// helper function to get dataset for release
func dataset4release(spec bson.M) []string {
	var out []string
	api := "datasets"
	release := spec["release"].(string)
	furl := fmt.Sprintf("%s/%s?release_version=%s", dbsUrl(), api, release)
	parent := spec["parent"]
	if parent != nil {
		furl = fmt.Sprintf("%s&parent_dataset=%s", furl, parent.(string))
	}
	status := spec["status"]
	if status != nil {
		furl = fmt.Sprintf("%s&dataset_access_type=%s", furl, status.(string))
	}
	resp := utils.FetchResponse(furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	for _, rec := range records {
		dataset := rec["name"].(string)
		if !utils.InList(dataset, out) {
			out = append(out, dataset)
		}
	}
	return out
}

// helper function to construct Phedex node API argument from given site
func phedexNode(site string) string {
	var node string
	nodeMatch, _ := regexp.MatchString("^T[0-9]_[A-Z]+(_)[A-Z]+", site)
	seMatch, _ := regexp.MatchString("^[a-z]+(\\.)[a-z]+(\\.)", site)
	if nodeMatch {
		node = fmt.Sprintf("node=%s", site)
		if !strings.HasSuffix(node, "*") {
			node += "*"
		}
	} else if seMatch {
		node = fmt.Sprintf("se=%s", site)
	} else {
		panic(fmt.Sprintf("ERROR: unable to match site name %s", site))
	}
	return node
}

// helper function to find datasets for given site and release
func dataset4site_release(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	var urls, datasets []string
	api := "blockReplicas"
	node := phedexNode(spec["site"].(string))
	for _, dataset := range dataset4release(spec) {
		furl := fmt.Sprintf("%s/%s?dataset=%s&%s", phedexUrl(), api, dataset, node)
		if !utils.InList(furl, urls) {
			urls = append(urls, furl)
		}
	}
	for _, rec := range processUrls("phedex", api, urls) {
		block := rec["name"].(string)
		dataset := strings.Split(block, "#")[0]
		if !utils.InList(dataset, datasets) {
			datasets = append(datasets, dataset)
		}
	}
	for _, name := range datasets {
		rec := make(mongo.DASRecord)
		row := make(mongo.DASRecord)
		row["name"] = name
		rec["dataset"] = []mongo.DASRecord{row}
		out = append(out, rec)
	}
	return out
}
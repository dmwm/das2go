/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: Services module
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

func DASLocalAPIs() []string {
	out := []string{"file_run_lumi4dataset", "file_run_lumi4block",
		"file_lumi4dataset", "file_lumi4block", "run_lumi4dataset", "run_lumi4block",
		"block_run_lumi4dataset", "file4dataset_run_lumi", "blocks4tier_dates",
		"dataset4block", "lumi4block_run"}
	return out
}

// Local DBS3 APIs
func (LocalAPIs) L_dbs3_dataset4block(spec bson.M) []mongo.DASRecord {
	block := spec["block"].(string)
	dataset := strings.Split(block, "#")[0]
	var out []mongo.DASRecord
	rec := make(mongo.DASRecord)
	row := make(mongo.DASRecord)
	row["name"] = dataset
	rec["dataset"] = []mongo.DASRecord{row}
	out = append(out, rec)
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
	resp := utils.FetchResponse(furl)
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
	rmax := 3 // maximum number of retries
	for _, furl := range urls {
		umap[furl] = 0 // number of retries per url
		go utils.Fetch(furl, out)
	}
	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			if r.Error != nil {
				retry := umap[r.Url]
				if retry < rmax {
					retry += 1
					// incremenet sleep duration with every retry
					sleep := time.Duration(retry) * time.Second
					time.Sleep(sleep)
					umap[r.Url] = retry
				} else {
					delete(umap, r.Url) // remove Url from map
				}
			} else {
				// process data
				var records []mongo.DASRecord
				if system == "dbs3" {
					records = DBSUnmarshal(api, r.Data)
				} else if system == "phedex" {
					records = PhedexUnmarshal(api, r.Data)
				}
				for _, r := range records {
					out_records = append(out_records, r)
				}
				// remove from umap, indicate that we processed it
				delete(umap, r.Url) // remove Url from map
			}
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
func run_args(spec bson.M) string {
	// get runs from spec
	runs := spec["run"]
	runs_args := ""
	if runs != nil {
		for _, val := range runs.([]string) {
			runs_args = fmt.Sprintf("%s&run_num=%s", runs_args, val)
		}
	}
	return runs_args
}

// helper function to get DBS urls for given spec and api
func dbs_urls(spec bson.M, api string) []string {
	// get runs from spec
	runs_args := run_args(spec)

	// find all blocks for given dataset or block
	var urls []string
	for _, blk := range find_blocks(spec) {
		myurl := fmt.Sprintf("%s/%s?block_name=%s", dbsUrl(), api, url.QueryEscape(blk))
		if len(runs_args) > 0 {
			myurl += runs_args // append run arguments
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

// DBS3 local APIs
func (LocalAPIs) L_dbs3_run_lumi4dataset(spec bson.M) []mongo.DASRecord {
	fields := []string{"run_num", "lumi_section_num"}
	return file_run_lumi(spec, fields)
}
func (LocalAPIs) L_dbs3_run_lumi4block(spec bson.M) []mongo.DASRecord {
	fields := []string{"run_num", "lumi_section_num"}
	return file_run_lumi(spec, fields)
}

func (LocalAPIs) L_dbs3_file_lumi4dataset(spec bson.M) []mongo.DASRecord {
	fields := []string{"logical_file_name", "lumi_section_num"}
	return file_run_lumi(spec, fields)
}
func (LocalAPIs) L_dbs3_file_lumi4block(spec bson.M) []mongo.DASRecord {
	fields := []string{"logical_file_name", "lumi_section_num"}
	return file_run_lumi(spec, fields)
}

func (LocalAPIs) L_dbs3_file_run_lumi4dataset(spec bson.M) []mongo.DASRecord {
	fields := []string{"logical_file_name", "run_num", "lumi_section_num"}
	return file_run_lumi(spec, fields)
}
func (LocalAPIs) L_dbs3_file_run_lumi4block(spec bson.M) []mongo.DASRecord {
	fields := []string{"logical_file_name", "run_num", "lumi_section_num"}
	return file_run_lumi(spec, fields)
}

// TODO: APIs below needs to be implemented
func (LocalAPIs) L_dbs3_block_run_lumi4dataset(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_dbs3_file4dataset_run_lumi(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_dbs3_blocks4tier_dates(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_dbs3_lumi4block_run(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}

// Combined APIs
func (LocalAPIs) L_combined_dataset4site_release(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_combined_dataset4site_release_parent(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_combined_child4site_release_dataset(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_combined_site4dataset(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_combined_lumi4dataset(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}

// helper function to filter files which belong to given site
func filterFiles(files []string, site string) []string {
	var out, urls []string
	api := "fileReplicas"
	var node string
	nodeMatch, _ := regexp.MatchString("^T[0-9]_[A-Z]+(_)[A-Z]+", site)
	seMatch, _ := regexp.MatchString("^[a-z]+(\\.)[a-z]+(\\.)", site)
	if nodeMatch {
		node = fmt.Sprintf("node=%s", site)
	} else if seMatch {
		node = fmt.Sprintf("se=%s", site)
	} else {
		panic(fmt.Sprintf("ERROR: unable to match site name %s", site))
	}
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

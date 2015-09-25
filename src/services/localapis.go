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
	"strings"
	"time"
	"utils"
)

type LocalAPIs struct{}

func dbsurl() string {
	return "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
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
	rec["dataset"] = row
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
	furl := fmt.Sprintf("%s/%s?dataset=%s", dbsurl(), api, dataset)
	resp := utils.FetchResponse(furl)
	records := DBSUnmarshal(api, resp.Data)
	for _, rec := range records {
		out = append(out, rec["block_name"].(string))
	}
	return out
}
func processUrls(api string, urls []string) []mongo.DASRecord {
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
				records := DBSUnmarshal(api, r.Data)
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
func file_run_lumi(spec bson.M, fields []string) []mongo.DASRecord {
	var out []mongo.DASRecord

	// get runs from spec
	runs := spec["run"]
	runs_args := ""
	if runs != nil {
		for _, val := range runs.([]int) {
			runs_args = fmt.Sprintf("%s&run_num=%d", runs_args, val)
		}
	}

	// find all blocks for given dataset or block
	var urls []string
	api := "filelumis"
	for _, blk := range find_blocks(spec) {
		myurl := fmt.Sprintf("%s/%s?block_name=%s", dbsurl(), api, url.QueryEscape(blk))
		if len(runs_args) > 0 {
			myurl += runs_args // append run arguments
		}
		urls = append(urls, myurl)
	}
	filelumis := processUrls(api, urls)
	// use filelumis DBS API output to get
	// run_num, logical_file_name, lumi_secion_num from provided fields
	for _, rec := range filelumis {
		row := make(mongo.DASRecord)
		for _, key := range fields {
			if key == "run_num" {
				row["run"] = mongo.DASRecord{"run_number": rec[key]}
			} else if key == "lumi_section_num" {
				row["lumi"] = mongo.DASRecord{"number": rec[key]}
			} else if key == "logical_file_name" {
				row["file"] = mongo.DASRecord{"name": rec[key]}
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
func (LocalAPIs) L_combined_files4dataset_runs_site(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}
func (LocalAPIs) L_combined_files4block_runs_site(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	return out
}

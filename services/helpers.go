package services

// DAS service module
// this module contains all helper functions used in DAS services, e.g. Local APIs
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

var UrlMap map[string]string

// DBSUrl returns DBS URL
func DBSUrl(inst string) string {
	url := bareDbsUrl(inst)
	if strings.HasPrefix(url, "http") {
		return url
	}
	return utils.AdjustUrl(fmt.Sprintf("%s/%s", FrontendURL, url))
}
func bareDbsUrl(inst string) string {
	v := utils.GetEnv("DBS_URL")
	if v != "" {
		if strings.Contains(v, "DBSReader") {
			return v
		}
		return fmt.Sprintf("%s/dbs/%s/DBSReader", v, inst)
	}
	if val, ok := UrlMap["dbs3"]; ok {
		v = val
	}
	if strings.Contains(v, "DBSReader") {
		return v
	}
	surl := fmt.Sprintf("%s/dbs/%s/DBSReader", v, inst)
	return surl
}

// PhedexUrl returns Phedex URL
func PhedexUrl() string {
	url := barePhedexUrl()
	if strings.HasPrefix(url, "http") {
		return url
	}
	return utils.AdjustUrl(fmt.Sprintf("%s/%s", FrontendURL, url))
}
func barePhedexUrl() string {
	v := utils.GetEnv("PHEDEX_URL")
	if v != "" {
		if strings.Contains(v, "phedex") {
			return v
		}
		return fmt.Sprintf("%s/phedex/datasvc/json/prod", v)
	}
	if val, ok := UrlMap["phedex"]; ok {
		v = val
	}
	if strings.Contains(v, "phedex") {
		return v
	}
	surl := fmt.Sprintf("%s/phedex/datasvc/json/prod", v)
	return surl
}

// SitedbUrl returns Sitedb URL
func SitedbUrl() string {
	url := bareSitedbUrl()
	if strings.HasPrefix(url, "http") {
		return url
	}
	return utils.AdjustUrl(fmt.Sprintf("%s/%s", FrontendURL, url))
}
func bareSitedbUrl() string {
	v := utils.GetEnv("SITEDB_URL")
	if v != "" {
		if strings.Contains(v, "sitedb") {
			return v
		}
		return fmt.Sprintf("%s/sitedb/data/prod", v)
	}
	if val, ok := UrlMap["sitedb"]; ok {
		v = val
	}
	if strings.Contains(v, "sitedb") {
		return v
	}
	surl := fmt.Sprintf("%s/sitedb/data/prod", v)
	return surl
}

// CricUrl returns Cric URL
func CricUrl(api string) string {
	v := utils.GetEnv("CRIC_URL")
	surl := "https://cms-cric.cern.ch"
	if val, ok := UrlMap["cric"]; ok {
		if val != "" {
			surl = val
		}
	}
	if v != "" {
		surl = v
	}
	if strings.Contains(api, "site") {
		return fmt.Sprintf("%s/api/cms/site/query", surl)
	}
	return fmt.Sprintf("%s/api/accounts/user/query", surl)
}

// RucioUrl returns Rucio url
func RucioUrl() string {
	url := bareRucioUrl()
	if strings.HasPrefix(url, "http") {
		return url
	}
	// here we hard-code rucio url since it is used across DAS APIs.
	// For das web it will be fetched from dasmaps DB, or config.
	// For dasgoclient we should have a valid default
	if RucioURL == "" {
		return utils.AdjustUrl(fmt.Sprintf("https://cms-rucio.cern.ch/%s", url))
	}
	return utils.AdjustUrl(fmt.Sprintf("%s/%s", RucioURL, url))
}
func bareRucioUrl() string {
	v := utils.GetEnv("RUCIO_URL")
	if v != "" {
		return v
	}
	if val, ok := UrlMap["rucio"]; ok {
		v = val
	}
	return v
}

// helper function to find file,run,lumis for given dataset or block
func findBlocks(dasquery dasql.DASQuery) []string {
	spec := dasquery.Spec
	inst := dasquery.Instance
	var out []string
	blk := spec["block"]
	if blk != nil {
		out = append(out, blk.(string))
		return out
	}
	dataset := spec["dataset"].(string)
	api := "blocks"
	furl := fmt.Sprintf("%s/%s?dataset=%s", DBSUrl(inst), api, dataset)
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	for _, rec := range records {
		v := rec["block_name"]
		if v != nil {
			out = append(out, v.(string))
		}
	}
	return out
}

// helper function to process given set of urls and unmarshal results
// from all url calls
func processUrls(dasquery dasql.DASQuery, system, api string, urls []string) []mongo.DASRecord {
	var outRecords []mongo.DASRecord
	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	client := utils.HttpClient()
	for _, furl := range urls {
		umap[furl] = 1                        // keep track of processed urls below
		go utils.Fetch(client, furl, "", out) // "" specify optional args
	}
	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			// process data
			var records []mongo.DASRecord
			if system == "dbs3" || system == "dbs" {
				records = DBSUnmarshal(api, r.Data)
			} else if system == "phedex" {
				records = PhedexUnmarshal(api, r.Data)
			}
			for _, rec := range records {
				rec["url"] = r.Url
				outRecords = append(outRecords, rec)
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
	return outRecords
}

// helper function to get run arguments for given spec
// we extract run parameter from spec and construct run_num arguments for DBS
func runArgs(dasquery dasql.DASQuery) string {
	// get runs from spec
	spec := dasquery.Spec
	runs := spec["run"]
	runsArgs := ""
	if runs != nil {
		switch value := runs.(type) {
		case []string:
			for _, val := range value {
				runsArgs = fmt.Sprintf("%s&run_num=%s", runsArgs, val)
			}
		case string:
			runsArgs = fmt.Sprintf("%s&run_num=%s", runsArgs, value)
		default:
			log.Printf("ERROR: unknown type %T, runs %v\n", runs, runs)
			return runsArgs
		}
	}
	return runsArgs
}

// helper function to get file status from the spec
func fileStatus(dasquery dasql.DASQuery) bool {
	spec := dasquery.Spec
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
func dbsUrls(dasquery dasql.DASQuery, api string) []string {
	inst := dasquery.Instance
	// get runs from spec
	runsArgs := runArgs(dasquery)
	validFile := fileStatus(dasquery)

	// find all blocks for given dataset or block
	var urls []string
	for _, blk := range findBlocks(dasquery) {
		myurl := fmt.Sprintf("%s/%s?block_name=%s", DBSUrl(inst), api, url.QueryEscape(blk))
		if len(runsArgs) > 0 {
			myurl += runsArgs // append run arguments
		}
		if validFile {
			if !strings.Contains(myurl, "validFileOnly") {
				myurl += fmt.Sprintf("&validFileOnly=1") // append validFileOnly=1
			}
		}
		urls = append(urls, myurl)
	}
	return utils.List2Set(urls)
}

// helper function to get file,run,lumi triplets
func fileRunLumi(dasquery dasql.DASQuery, keys []string) []mongo.DASRecord {
	var out []mongo.DASRecord

	// use filelumis DBS API output to get
	// run_num, logical_file_name, lumi_secion_num from provided fields
	api := "filelumis"
	urls := dbsUrls(dasquery, api)
	filelumis := processUrls(dasquery, "dbs3", api, urls)
	for _, rec := range filelumis {
		if _, ok := rec["error"]; ok {
			out = append(out, rec)
		}
		row := make(mongo.DASRecord)
		for _, key := range keys {
			// put into file das record, internal type must be list
			if key == "run_num" {
				row["run"] = []mongo.DASRecord{{"run_number": rec[key]}}
			} else if key == "lumi_section_num" {
				row["lumi"] = []mongo.DASRecord{{"number": rec[key]}}
			} else if key == "event_count" {
				row["events"] = []mongo.DASRecord{{"number": rec[key]}}
			} else if key == "logical_file_name" {
				row["file"] = []mongo.DASRecord{{"name": rec[key]}}
			}
		}
		out = append(out, row)
	}
	if len(keys) == 2 {
		if keys[0] == "run_num" && keys[1] == "lumi_section_num" {
			return OrderByRunLumis(out)
		} else if keys[0] == "lumi_section_num" && keys[1] == "run_num" {
			return OrderByRunLumis(out)
		}
	}
	return out
}

// OrderByRunLumis helper function to sort records by run and then merge lumis within a run
func OrderByRunLumis(records []mongo.DASRecord) []mongo.DASRecord {
	var out []mongo.DASRecord
	rmap := make(map[json.Number][]json.Number)
	for _, r := range records {
		var lumiList []json.Number
		switch v := mongo.GetValue(r, "lumi.number").(type) {
		case json.Number:
			lumiList = append(lumiList, v)
		case []interface{}:
			for _, vv := range v {
				lumiList = append(lumiList, vv.(json.Number))
			}
		}
		//         lumiList := mongo.GetValue(r, "lumi.number").([]interface{})
		run := mongo.GetValue(r, "run.run_number").(json.Number)
		lumis, ok := rmap[run]
		if ok {
			for _, v := range lumiList {
				//                 lumis = append(lumis, v.(json.Number))
				lumis = append(lumis, v)
			}
			rmap[run] = lumis
		} else {
			var lumiValues []json.Number
			for _, v := range lumiList {
				//                 lumiValues = append(lumiValues, v.(json.Number))
				lumiValues = append(lumiValues, v)
			}
			rmap[run] = lumiValues
		}
	}
	for run, lumis := range rmap {
		var runlist, lumilist []mongo.DASRecord
		runlist = append(runlist, mongo.DASRecord{"run_number": run})
		lumilist = append(lumilist, mongo.DASRecord{"number": lumis})
		// final record should have list to be consistent with DAS record output
		rec := mongo.DASRecord{"run": runlist, "lumi": lumilist}
		out = append(out, rec)
	}
	return out
}

// helper function to get dataset for release
func dataset4release(dasquery dasql.DASQuery) []string {
	spec := dasquery.Spec
	inst := dasquery.Instance
	var out []string
	api := "datasets"
	release := spec["release"].(string)
	furl := fmt.Sprintf("%s/%s?release_version=%s", DBSUrl(inst), api, release)
	parent := spec["parent"]
	if parent != nil {
		furl = fmt.Sprintf("%s&parent_dataset=%s", furl, parent.(string))
	}
	status := spec["status"]
	if status != nil {
		furl = fmt.Sprintf("%s&dataset_access_type=%s", furl, status.(string))
	}
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	for _, rec := range records {
		if rec["name"] == nil {
			continue
		}
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
	nodeMatch := utils.PatternSite.MatchString(site)
	seMatch := utils.PatternSE.MatchString(site)
	if nodeMatch {
		node = fmt.Sprintf("node=%s", site)
		if !strings.HasSuffix(node, "*") {
			node += "*"
		}
	} else if seMatch {
		node = fmt.Sprintf("se=%s", site)
	} else {
		log.Println("ERROR: unable to match site name", site)
		return ""
	}
	return node
}

// helper function to find datasets for given site and release
func dataset4siteRelease(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	var urls, datasets []string
	api := "blockReplicas"
	var node string
	if spec["site"] != nil {
		node = phedexNode(spec["site"].(string))
	}
	for _, dataset := range dataset4release(dasquery) {
		furl := fmt.Sprintf("%s/%s?dataset=%s&%s", PhedexUrl(), api, dataset, node)
		if !utils.InList(furl, urls) {
			urls = append(urls, furl)
		}
	}
	for _, rec := range processUrls(dasquery, "phedex", api, urls) {
		if rec["name"] == nil {
			continue
		}
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

// PhedexNodes struct caches PhEDEX nodes and periodically update them
type PhedexNodes struct {
	nodes  []mongo.DASRecord
	tstamp int64
}

// Nodes API periodically fetches PhEDEx nodes info
// if records still alive (fetched less than a day ago) we use the cache
func (p *PhedexNodes) Nodes() []mongo.DASRecord {
	if len(p.nodes) != 0 && (time.Now().Unix()-p.tstamp) < 24*60*60 {
		return p.nodes
	}
	api := "nodes"
	furl := fmt.Sprintf("%s/%s", PhedexUrl(), api)
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "") // "" specify optional args
	p.nodes = PhedexUnmarshal(api, resp.Data)
	p.tstamp = time.Now().Unix()
	return p.nodes
}

// NodeType API returns type of given node
func (p *PhedexNodes) NodeType(site string) string {
	nodeMatch := utils.PatternSite.MatchString(site)
	seMatch := utils.PatternSE.MatchString(site)
	nodes := p.Nodes()
	var siteName, seName, kind string
	for _, rec := range nodes {
		switch v := rec["se"].(type) {
		case string:
			seName = v
		default:
			seName = ""
		}
		switch v := rec["name"].(type) {
		case string:
			siteName = v
		default:
			siteName = ""
		}
		switch v := rec["kind"].(type) {
		case string:
			kind = v
		default:
			kind = ""
		}
		if nodeMatch && siteName == site {
			return kind
		} else if seMatch && seName == site {
			return kind
		}
	}
	return ""
}

package services

// DAS service module
// ReqMgr module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// helper function to load ReqMgr data stream
func loadReqMgrData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	if api == "configIDs" || api == "datasetByPrepID" || api == "outputdataset" || api == "inputdataset" {
		var rec mongo.DASRecord
		// to prevent json.Unmarshal behavior to convert all numbers to float
		// we'll use json decode method with instructions to use numbers as is
		buf := bytes.NewBuffer(data)
		dec := json.NewDecoder(buf)
		dec.UseNumber()
		err := dec.Decode(&rec)

		// original way to decode data
		// err := json.Unmarshal(data, &rec)
		if err != nil {
			msg := fmt.Sprintf("ReqMgr unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
			if utils.VERBOSE > 0 {
				log.Printf("ERROR: ReqMgr unable to unmarshal, data %+v, api %v, error %v\n", string(data), api, err)
			}
			out = append(out, mongo.DASErrorRecord(msg, utils.ReqMgrErrorName, utils.ReqMgrError))
		}
		out = append(out, rec)
	} else if api == "recentDatasetByPrepID" {
		var datasets []string
		err := json.Unmarshal(data, &datasets)
		if err != nil {
			msg := fmt.Sprintf("ReqMgr unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
			if utils.VERBOSE > 0 {
				log.Printf("ERROR: ReqMgr unable to unmarshal, data %+v, api %v, error %v\n", string(data), api, err)
			}
			out = append(out, mongo.DASErrorRecord(msg, utils.ReqMgrErrorName, utils.ReqMgrError))
		}
		for _, d := range datasets {
			rec := make(mongo.DASRecord)
			rec["name"] = d
			out = append(out, rec)
		}
	} else {
		err := json.Unmarshal(data, &out)
		if err != nil {
			msg := fmt.Sprintf("ReqMgr unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
			if utils.VERBOSE > 0 {
				log.Printf("ERROR: ReqMgr unable to unmarshal, data %+v, api %v, error %v\n", string(data), api, err)
			}
			out = append(out, mongo.DASErrorRecord(msg, utils.ReqMgrErrorName, utils.ReqMgrError))
		}
	}
	return out
}

// ReqMgrUnmarshal unmarshals ReqMgr data stream and return DAS records based on api
func ReqMgrUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadReqMgrData(api, data)
	var out []mongo.DASRecord
	if api == "inputdataset" {
		for _, rec := range records {
			val := rec["InputDatasets"]
			if val != nil {
				datasets := val.([]string)
				rec["name"] = datasets[0]
			}
			out = append(out, rec)
		}
		return out
	} else if api == "outputdataset" {
		for _, rec := range records {
			val := rec["OutputDatasets"]
			if val != nil {
				datasets := val.([]string)
				rec["name"] = datasets[0]
			}
			out = append(out, rec)
		}
		return out
	} else if api == "configIDs" {
		for _, rec := range records {
			for key, val := range rec {
				crec := make(mongo.DASRecord)
				crec["request_name"] = key
				crec["config_files"] = val
				out = append(out, crec)
			}
		}
		return out
	} else if api == "datasetByPrepID" {
		for _, rec := range records {
			for _, rows := range rec {
				for _, rrr := range rows.([]interface{}) {
					for _, o := range rrr.(map[string]interface{}) {
						a := o.(map[string]interface{})
						v := a["OutputDatasets"]
						switch datasets := v.(type) {
						case []interface{}:
							for _, d := range datasets {
								crec := make(mongo.DASRecord)
								crec["name"] = d
								out = append(out, crec)
							}
						}
					}
				}
			}
		}
		return out
	}
	return records
}

/*
 * LOCAL APIs
 */

type ReqMgrInfo struct {
	RequestName string
	ConfigIDs   []string
	ConfigIDMap map[string]string
	Tasks       []string
}

// helper function to find ReqMgr ids
func findReqMgrIds(dasquery dasql.DASQuery, base, dataset string) ([]ReqMgrInfo, map[string][]string) {
	var inputOut, outputOut, ids, urls []string
	var rurl string
	var reqmgrInfo []ReqMgrInfo
	idict := make(map[string][]string)
	rmap := make(map[string]string)

	// check that given dataset pass dataset pattern
	matched, err := regexp.MatchString("/[\\w-]+/[\\w-]+/[A-Z-]+", dataset)
	if err != nil || !matched {
		log.Printf("ERROR: unable to validate dataset %v, error %v\n", dataset, err)
		return reqmgrInfo, idict
	}

	rurl = fmt.Sprintf("%s/reqmgr2/data/request?outputdataset=%s", base, dataset)
	urls = append(urls, rurl)
	rurl = fmt.Sprintf("%s/reqmgr2/data/request?inputdataset=%s", base, dataset)
	urls = append(urls, rurl)
	umap := map[string]int{}
	ch := make(chan utils.ResponseType)
	defer close(ch)
	client := utils.HttpClient()
	for _, u := range urls {
		umap[u] = 1 // keep track of processed urls below
		go utils.Fetch(client, u, "", ch)
	}
	exit := false
	for {
		select {
		case r := <-ch:
			var data mongo.DASRecord
			view := ""
			if strings.Contains(strings.ToLower(r.Url), "inputdataset") {
				view = "input"
			}
			if strings.Contains(strings.ToLower(r.Url), "outputdataset") {
				view = "output"
			}
			err := json.Unmarshal(r.Data, &data)
			if err == nil {
				result := data["result"]
				if result != nil {
					rows := result.([]interface{})
					for _, rec := range rows {
						row := rec.(map[string]interface{})
						for reqName, d := range row {
							rinfo := ReqMgrInfo{RequestName: reqName}
							data := d.(map[string]interface{})
							for kkk, vvv := range data {
								if strings.Contains(kkk, "ConfigCacheID") {
									switch val := vvv.(type) {
									case string:
										if len(val) == 32 {
											if view == "input" && !utils.InList(val, inputOut) {
												inputOut = append(inputOut, val)
											}
											if view == "output" && !utils.InList(val, outputOut) {
												outputOut = append(outputOut, val)
											}
											if !utils.InList(val, ids) {
												ids = append(ids, val)
											}
											rmap[val] = kkk
										}
									}
								}
								// extract configs from Task parts of FJR document
								if strings.Contains(kkk, "Task") {
									switch data := vvv.(type) {
									case map[string]interface{}:
										var taskName string
										if tname, ok := data["TaskName"]; ok {
											taskName = fmt.Sprintf("%s", tname)
										}
										for k, v := range data {
											if k == "ConfigCacheID" {
												switch tid := v.(type) {
												case string:
													ids = append(ids, tid)
													rmap[tid] = taskName
												}
											}
										}
									}
								}
							}
							rinfo.ConfigIDs = utils.List2Set(ids)
							rinfo.ConfigIDMap = rmap
							reqmgrInfo = append(reqmgrInfo, rinfo)
						}
					}
				}
			}
			idict["byinputdataset"] = inputOut
			idict["byoutputdataset"] = outputOut
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
	return reqmgrInfo, idict
}

// Configs reqmgr APIs to lookup configs for given dataset
// The logic: we look-up ReqMgr ids for given dataset and scan them
// if id has length 32 we use configFile URL, otherwise we look-up record
// in couchdb and fetch ConfigIDs to construct configFile URL
func (LocalAPIs) Configs(dasquery dasql.DASQuery) []mongo.DASRecord {
	return reqmgrConfigs(dasquery)
}

func reqmgrConfigs(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	base := "https://cmsweb.cern.ch:8443"
	// find ReqMgr Ids for given dataset
	dataset := spec["dataset"].(string)
	reqmgrInfo, idict := findReqMgrIds(dasquery, base, dataset)
	var urls, rurls, uids []string
	var rurl string
	for _, req := range reqmgrInfo {
		for _, v := range req.ConfigIDs {
			if len(v) == 32 {
				rurl = fmt.Sprintf("%s/couchdb/reqmgr_config_cache/%s/configFile", base, v)
				urls = append(urls, rurl)
			} else {
				rurl = fmt.Sprintf("%s/couchdb/reqmgr_config_cache/%s", base, v)
				rurls = append(rurls, rurl)
			}
		}
	}

	// if we have reqmgr urls we must resolve it they lead to actual config files
	umap := map[string]int{}
	exit := false
	ch := make(chan utils.ResponseType)
	defer close(ch)
	client := utils.HttpClient()
	for _, u := range rurls {
		umap[u] = 1 // keep track of processed urls below
		go utils.Fetch(client, u, "", ch)
	}
	for {
		select {
		case r := <-ch:
			var data mongo.DASRecord
			err := json.Unmarshal(r.Data, &data)
			if err == nil {
				for key, val := range data {
					if strings.Contains(key, "ConfigCacheID") {
						rurl = fmt.Sprintf("%s/couchdb/reqmgr_config_cache/%s/configFile", base, val)
						if !utils.InList(rurl, urls) {
							urls = append(urls, rurl)
							uids = append(uids, fmt.Sprintf("%s", val))
						}
					}
				}
			}
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

	// Construct final record
	var out []mongo.DASRecord
	for _, req := range reqmgrInfo {
		rec := make(mongo.DASRecord)
		rec["dataset"] = dataset
		rec["name"] = req.RequestName
		//         rec["ids"] = req.ConfigIDs
		// construct human readble representation of config ids, i.e.
		// we take request config id map and check if proper key exists for given id
		var configIds []string
		for _, k := range req.ConfigIDs {
			if v, ok := req.ConfigIDMap[k]; ok {
				configIds = append(configIds, fmt.Sprintf("%s::%s", k, v))
			} else {
				configIds = append(configIds, k)
			}
		}
		rec["ids"] = configIds
		rec["idict"] = idict
		var outputUrls, inputUrls []string
		for _, uid := range idict["byinputdataset"] {
			for _, rurl := range urls {
				if strings.Contains(rurl, uid) {
					// we should ensure that rurl covers req ConfigIDs
					for _, rid := range req.ConfigIDs {
						if strings.Contains(rurl, rid) {
							inputUrls = append(inputUrls, rurl)
						}
					}
				}
			}
		}
		for _, uid := range idict["byoutputdataset"] {
			for _, rurl := range urls {
				if strings.Contains(rurl, uid) {
					// we should ensure that rurl covers req ConfigIDs
					for _, rid := range req.ConfigIDs {
						if strings.Contains(rurl, rid) {
							outputUrls = append(outputUrls, rurl)
						}
					}
				}
			}
		}
		rec["urls"] = mongo.DASRecord{"output": outputUrls, "input": inputUrls}
		out = append(out, rec)
	}
	return out
}

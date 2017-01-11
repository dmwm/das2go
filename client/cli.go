package client

// das2go - Go implementation of Data Aggregation System (DAS) for CMS
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"encoding/json"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/vkuznet/das2go/das"
	"github.com/vkuznet/das2go/dasmaps"
	"github.com/vkuznet/das2go/dasql"
	"github.com/vkuznet/das2go/mongo"
	"github.com/vkuznet/das2go/services"
	"github.com/vkuznet/das2go/utils"
	"strings"
	"time"
)

// Process function process' given query and return back results
func Process(query, inst string, jsonout bool) {
	var dmaps dasmaps.DASMaps
	dmaps.LoadMaps("mapping", "db")
	if inst == "" {
		inst = "prod/global"
	}
	dasquery, err := dasql.Parse(query, inst, dmaps.DASKeys())
	if utils.VERBOSE > 0 {
		fmt.Println(dasquery, err)
	}

	// find out list of APIs/CMS services which can process this query request
	maps := dmaps.FindServices(dasquery.Instance, dasquery.Fields, dasquery.Spec)
	var srvs, pkeys []string
	urls := make(map[string]string)
	var localApis []mongo.DASRecord
	var furl string
	// loop over services and fetch data
	for _, dmap := range maps {
		args := ""
		system, _ := dmap["system"].(string)
		if system == "runregistry" {
			switch v := dasquery.Spec["run"].(type) {
			case string:
				args = fmt.Sprintf("{\"filter\": {\"number\": \">= %s and <= %s\"}}", v, v)
			case []string:
				args = fmt.Sprintf("{\"filter\": {\"number\": \">= %s and <= %s\"}}", v[0], v[len(v)-1])
			}
			furl, _ = dmap["url"].(string)
		} else if system == "reqmgr" || system == "mcm" {
			furl = das.FormRESTUrl(dasquery, dmap)
		} else {
			furl = das.FormUrlCall(dasquery, dmap)
		}
		if furl == "local_api" && !dasmaps.MapInList(dmap, localApis) {
			localApis = append(localApis, dmap)
		} else if furl != "" {
			if _, ok := urls[furl]; !ok {
				urls[furl] = args
			}
		}
		srv := fmt.Sprintf("%s:%s", dmap["system"], dmap["urn"])
		srvs = append(srvs, srv)
		lkeys := strings.Split(dmap["lookup"].(string), ",")
		for _, pkey := range lkeys {
			for _, item := range dmap["das_map"].([]interface{}) {
				rec := item.(mongo.DASRecord)
				daskey := rec["das_key"].(string)
				reckey := rec["rec_key"].(string)
				if daskey == pkey {
					pkeys = append(pkeys, reckey)
					break
				}
			}
		}
	}
	if utils.VERBOSE > 0 {
		fmt.Println("srvs", srvs, pkeys)
		fmt.Println("urls", urls)
		fmt.Println("localApis", localApis)
	}
	if len(urls) > 0 {
		dasrecords := processURLs(dasquery, urls, maps, dmaps, pkeys)
		var keys []string
		for _, pkey := range pkeys {
			for _, kkk := range strings.Split(pkey, ".") {
				if !utils.InList(kkk, keys) {
					keys = append(keys, kkk)
					if len(keys) == 1 {
						keys = append(keys, "[0]") // to hadle DAS records lists
					}
				}
			}
		}
		for _, rec := range dasrecords {
			if jsonout {
				out, err := json.Marshal(rec)
				if err == nil {
					fmt.Println(string(out))
				} else {
					fmt.Println("DAS record", rec, "fail to mashal it to JSON stream")
				}
				continue
			}
			rbytes, err := mongo.GetBytesFromDASRecord(rec)
			if err != nil {
				if utils.VERBOSE > 0 {
					fmt.Println("Fail to parse DAS record", pkeys, keys, err, rec)
				}
				fmt.Println(rec)
			} else {
				val, _, _, err := jsonparser.Get(rbytes, keys...)
				if err == nil {
					fmt.Println(string(val))
				} else {
					if utils.VERBOSE > 0 {
						fmt.Println("Fail to parse DAS record", pkeys, keys, err, rec)
					}
					fmt.Println(rec)
				}
			}
		}
	}
}

// helper function to process given set of URLs associted with dasquery
func processURLs(dasquery dasql.DASQuery, urls map[string]string, maps []mongo.DASRecord, dmaps dasmaps.DASMaps, pkeys []string) []mongo.DASRecord {
	// defer function will propagate panic message to higher level
	//     defer utils.ErrPropagate("processUrls")

	var dasrecords []mongo.DASRecord
	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	for furl, args := range urls {
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(furl, args, out)
	}

	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			system := ""
			expire := 0
			urn := ""
			for _, dmap := range maps {
				surl := dasmaps.GetString(dmap, "url")
				// TMP fix, until we fix Phedex data to use JSON
				if strings.Contains(surl, "phedex") {
					surl = strings.Replace(surl, "xml", "json", -1)
				}
				// here we check that request Url match DAS map one either by splitting
				// base from parameters or making a match for REST based urls
				stm := dasmaps.GetString(dmap, "system")
				inst := dasquery.Instance
				if inst != "prod/global" && stm == "dbs3" {
					surl = strings.Replace(surl, "prod/global", inst, -1)
				}
				if strings.Split(r.Url, "?")[0] == surl || strings.HasPrefix(r.Url, surl) || r.Url == surl {
					urn = dasmaps.GetString(dmap, "urn")
					system = dasmaps.GetString(dmap, "system")
					expire = dasmaps.GetInt(dmap, "expire")
				}
			}
			// process data records
			notations := dmaps.FindNotations(system)
			records := services.Unmarshal(system, urn, r.Data, notations)
			records = services.AdjustRecords(dasquery, system, urn, records, expire, pkeys)

			// add records
			for _, rec := range records {
				dasrecords = append(dasrecords, rec)
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
	return dasrecords
}

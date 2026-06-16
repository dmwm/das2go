package services

// DAS service module
// Rucio module
//
// Copyright (c) 2018 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// helper function to load data stream and return DAS records
func loadRucioData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord

	// Rucio uses application/x-json-stream content type which yields dict records from the server
	var row []byte
	for _, r := range data {
		if string(r) == "\n" {
			var rec mongo.DASRecord
			err := json.Unmarshal(row, &rec)
			if err != nil {
				msg := fmt.Sprintf("Rucio unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(row), err)
				if utils.VERBOSE > 0 {
					log.Printf("ERROR: Rucio unable to unmarshal, data %+v, api %v, error %v\n", string(row), api, err)
				}
				out = append(out, mongo.DASErrorRecord(msg, utils.RucioErrorName, utils.RucioError))
			}
			out = append(out, rec)
			row = []byte{}
		} else {
			row = append(row, r)
		}
	}
	// last record from Rucio does not have '\n' so we need to collect it
	if len(row) > 0 {
		var rec mongo.DASRecord
		err := json.Unmarshal(row, &rec)
		if err != nil {
			msg := fmt.Sprintf("Rucio unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(row), err)
			if utils.VERBOSE > 0 {
				log.Printf("ERROR: Rucio unable to unmarshal, data %+v, row %+v, api %v, error %v\n", string(data), string(row), api, err)
			}
			out = append(out, mongo.DASErrorRecord(msg, utils.RucioErrorName, utils.RucioError))
		}
		out = append(out, rec)
	}
	return out
}

// RucioUnmarshal unmarshals Rucio data stream and return DAS records based on api
func RucioUnmarshal(dasquery dasql.DASQuery, api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	records := loadRucioData(api, data)
	specs := dasquery.Spec
	rmap := make(mongo.DASRecord)
	if api == "block4block" {
		if val, ok := specs["block"]; ok {
			block := fmt.Sprintf("%s", val)
			if info, ok := rucioBlockReplicaInfoFromRecords(block, "", records); ok {
				rec := mongo.DASRecord{"name": block}
				for k, v := range info {
					rec[k] = v
				}
				out = append(out, rec)
			}
		}
		return out
	}
	for _, rec := range records {
		if api == "rses" {
			if val, ok := specs["site"]; ok {
				if rec["rse"] != nil {
					rse := rec["rse"].(string)
					site := fmt.Sprintf("%s", val)
					if strings.Contains(site, "*") {
						site = strings.Replace(site, "*", ".*", -1)
					} else {
						site = fmt.Sprintf("%s.*", site)
					}
					matched, _ := regexp.MatchString(site, rse)
					if matched {
						rec["name"] = rse
						out = append(out, rec)
					}
				}
			}
		} else if api == "site4dataset" || api == "site4block" || api == "site4file" {
			if rec["states"] != nil {
				states := rec["states"].(map[string]interface{})
				for rse, _ := range states {
					// we need to create a new map record since we'll reassign
					// rse name as a main key
					newrec := make(map[string]interface{})
					for k, v := range rec {
						newrec[k] = v
					}
					newrec["name"] = rse
					out = append(out, newrec)
				}
			}
		} else if api == "dataset4site" {
			if rec["name"] != nil {
				blk := rec["name"].(string)
				arr := strings.Split(blk, "#")
				rmap[arr[0]] = 1
			}
		} else if api == "block4site" {
			if rec["name"] != nil {
				out = append(out, rec)
			}
		} else if api == "rules4dataset" || api == "rules4block" || api == "rules4file" {
			out = append(out, rec)
		} else if api == "block4dataset" {
			if rec["name"] != nil {
				block := rec["name"].(string)
				if info, ok := rucioBlockReplicaInfo(block, ""); ok {
					for k, v := range info {
						rec[k] = v
					}
				}
			}
			out = append(out, rec)
		} else if api == "block4dataset_site" {
			if val, ok := specs["site"]; ok && rec["name"] != nil {
				site := fmt.Sprintf("%s", val)
				block := rec["name"].(string)
				if info, ok := rucioBlockReplicaInfo(block, site); ok {
					for k, v := range info {
						rec[k] = v
					}
					out = append(out, rec)
				}
			}
		} else if api == "full_record" {
			out = append(out, rec)
		} else if api == "file4dataset_site" || api == "file4block_site" {
			if val, ok := specs["site"]; ok {
				site := fmt.Sprintf("%s", val)
				if rec["states"] != nil {
					states := rec["states"].(map[string]interface{})
					var sites []string
					for k, _ := range states {
						sites = append(sites, k)
					}
					if utils.InList(site, sites) {
						out = append(out, rec)
					}
				}
			}
		} else {
			if rec["states"] != nil {
				states := rec["states"].(map[string]interface{})
				var replicas []mongo.DASRecord
				for k, v := range states {
					rep := mongo.DASRecord{"name": k, "state": v}
					replicas = append(replicas, rep)
				}
				rec["replicas"] = replicas
				out = append(out, rec)
			}
		}
	}
	if api == "dataset4site" {
		for d := range rmap {
			rec := mongo.DASRecord{"name": d}
			out = append(out, rec)
		}
	}
	return out
}

func rucioBlockReplicaInfo(block, site string) (mongo.DASRecord, bool) {
	furl := fmt.Sprintf("%s/replicas/cms/%s/datasets?deep=True", RucioUrl(), url.QueryEscape(block))
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "")
	if resp.Error != nil {
		return nil, false
	}
	return rucioBlockReplicaInfoFromRecords(block, site, loadRucioData("block4dataset_site", resp.Data))
}

func rucioBlockReplicaInfoFromRecords(block, site string, records []mongo.DASRecord) (mongo.DASRecord, bool) {
	info := make(mongo.DASRecord)
	states := make(mongo.DASRecord)
	rses := make(mongo.DASRecord)
	var replicas []mongo.DASRecord
	for _, rec := range records {
		if rec["rse"] == nil {
			continue
		}
		rse := rec["rse"].(string)
		if !rucioSiteMatch(site, rse) {
			continue
		}
		replica := make(mongo.DASRecord)
		for k, v := range rec {
			replica[k] = v
			if k != "name" && k != "scope" {
				info[k] = v
			}
		}
		if rec["state"] != nil {
			states[rse] = rec["state"]
		}
		rses[rse] = []interface{}{}
		replicas = append(replicas, replica)
	}
	if len(replicas) == 0 {
		return nil, false
	}
	info["states"] = states
	info["rses"] = rses
	info["replicas"] = replicas
	return info, true
}

func rucioSiteMatch(site, rse string) bool {
	if site == "" {
		return true
	}
	if strings.Contains(site, "*") {
		pat := "^" + strings.Replace(regexp.QuoteMeta(site), "\\*", ".*", -1) + "$"
		matched, _ := regexp.MatchString(pat, rse)
		return matched
	}
	return site == rse
}

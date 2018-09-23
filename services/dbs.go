package services

// DAS service module
// DBS module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
)

// helper function to load DBS data stream
func loadDBSData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord

	// to prevent json.Unmarshal behavior to convert all numbers to float
	// we'll use json decode method with instructions to use numbers as is
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := dec.Decode(&out)

	// original way to decode data
	// err := json.Unmarshal(data, &out)
	if err != nil {
		msg := fmt.Sprintf("DBS unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		if utils.VERBOSE > 0 {
			logs.WithFields(logs.Fields{
				"Error": err,
				"Api":   api,
				"data":  string(data),
			}).Error("DBS unable to unmarshal the data")
		}
		out = append(out, mongo.DASErrorRecord(msg, utils.DBSErrorName, utils.DBSError))
	}
	return out
}

// DBSUnmarshal unmarshals DBS data stream and return DAS records based on api
func DBSUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadDBSData(api, data)
	var out []mongo.DASRecord
	if api == "dataset_info" || api == "datasets" || api == "datasetlist" {
		for _, rec := range records {
			rec["name"] = rec["dataset"]
			delete(rec, "dataset")
			out = append(out, rec)
		}
		return out
	} else if api == "physicsgroup" {
		for _, rec := range records {
			rec["name"] = rec["physics_group_name"]
			delete(rec, "physics_group_name")
			out = append(out, rec)
		}
		return out
	} else if api == "site4dataset" || api == "site4block" {
		for _, rec := range records {
			r := mongo.DASRecord{"name": rec["origin_site_name"], "dataset": rec["dataset"], "kind": "original placement"}
			out = append(out, r)
		}
		return out
	} else if api == "fileparents" {
		for _, rec := range records {
			switch val := rec["parent_logical_file_name"].(type) {
			case []interface{}:
				for _, v := range val {
					r := make(mongo.DASRecord)
					r["name"] = v.(string)
					out = append(out, r)
				}
			}
		}
		return out
	} else if api == "filechildren" {
		for _, rec := range records {
			switch val := rec["child_logical_file_name"].(type) {
			case []interface{}:
				for _, v := range val {
					r := make(mongo.DASRecord)
					r["name"] = v.(string)
					out = append(out, r)
				}
			}
		}
		return out
	} else if api == "runs_via_dataset" || api == "runs" {
		for _, rec := range records {
			switch val := rec["run_num"].(type) {
			case []interface{}:
				for _, v := range val {
					r := make(mongo.DASRecord)
					r["run_number"] = v
					out = append(out, r)
				}
			}
		}
		return out
	}
	return records
}

/*
 * Local DBS3 APIs
 */

// Dataset4Block find dataset for given block
func (LocalAPIs) Dataset4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
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

// RunLumi4Dataset finds run, lumi for given dataset
func (LocalAPIs) RunLumi4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"run_num", "lumi_section_num"}
	return fileRunLumi(dasquery, keys)
}

// RunLumiEvents4Dataset finds run, lumi for given dataset
func (LocalAPIs) RunLumiEvents4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"run_num", "lumi_section_num", "event_count"}
	return fileRunLumi(dasquery, keys)
}

// RunLumi4Block finds run,lumi for given block
func (LocalAPIs) RunLumi4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"run_num", "lumi_section_num"}
	return fileRunLumi(dasquery, keys)
}

// RunLumiEvents4Block finds run,lumi for given block
func (LocalAPIs) RunLumiEvents4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"run_num", "lumi_section_num", "event_count"}
	return fileRunLumi(dasquery, keys)
}

// FileLumi4Dataset finds file,lumi for given dataset
func (LocalAPIs) FileLumi4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "lumi_section_num"}
	return fileRunLumi(dasquery, keys)
}

// FileLumiEvents4Dataset finds file,lumi for given dataset
func (LocalAPIs) FileLumiEvents4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "lumi_section_num", "event_count"}
	return fileRunLumi(dasquery, keys)
}

// FileLumi4Block finds file,lumi for given block
func (LocalAPIs) FileLumi4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "lumi_section_num"}
	return fileRunLumi(dasquery, keys)
}

// FileLumiEvents4Block finds file,lumi for given block
func (LocalAPIs) FileLumiEvents4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "lumi_section_num", "event_count"}
	return fileRunLumi(dasquery, keys)
}

// FileRunLumi4Dataset finds file,run,lumi for given dataset
func (LocalAPIs) FileRunLumi4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "run_num", "lumi_section_num"}
	return fileRunLumi(dasquery, keys)
}

// FileRunLumiEvents4Dataset finds file,run,lumi for given dataset
func (LocalAPIs) FileRunLumiEvents4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "run_num", "lumi_section_num", "event_count"}
	return fileRunLumi(dasquery, keys)
}

// FileRunLumi4Block finds file,run,lumi for given block
func (LocalAPIs) FileRunLumi4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "run_num", "lumi_section_num"}
	return fileRunLumi(dasquery, keys)
}

// FileRunLumiEvents4Block finds file,run,lumi for given block
func (LocalAPIs) FileRunLumiEvents4Block(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"logical_file_name", "run_num", "lumi_section_num", "event_count"}
	return fileRunLumi(dasquery, keys)
}

// BlockRunLumi4Dataset finds run,lumi for given dataset
func (LocalAPIs) BlockRunLumi4Dataset(dasquery dasql.DASQuery) []mongo.DASRecord {
	var out []mongo.DASRecord
	keys := []string{"block_name", "run_num", "lumi_section_num"}
	// use filelumis DBS API output to get
	// run_num, logical_file_name, lumi_secion_num from provided keys
	api := "filelumis"
	urls := dbsUrls(dasquery, api)
	filelumis := processUrls("dbs3", api, urls)
	for _, rec := range filelumis {
		row := make(mongo.DASRecord)
		for _, key := range keys {
			// put into file das record, internal type must be list
			if key == "run_num" {
				row["run"] = []mongo.DASRecord{{"run_number": rec[key]}}
			} else if key == "lumi_section_num" {
				row["lumi"] = []mongo.DASRecord{{"number": rec[key]}}
			} else if key == "block_name" {
				rurl, err := url.QueryUnescape(rec["url"].(string))
				if err != nil {
					logs.WithFields(logs.Fields{
						"Error":  err,
						"Url":    rurl,
						"Record": rec,
					}).Error("unable to parse url ")
					return out
				}
				arr := strings.Split(rurl, "block_name=")
				blk := arr[1]
				row["block"] = []mongo.DASRecord{{"name": blk}}
			}
		}
		out = append(out, row)
	}
	return out
}

// File4DatasetRunLumi finds file for given dataset, run, lumi
func (LocalAPIs) File4DatasetRunLumi(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	lumi, _ := strconv.ParseFloat(spec["lumi"].(string), 64)
	keys := []string{"logical_file_name", "lumi_section_num"}
	records := fileRunLumi(dasquery, keys)
	for _, rec := range records {
		for _, row := range rec["lumi"].([]mongo.DASRecord) {
			v := row["number"]
			if v != nil {
				lumis := row["number"].([]interface{})
				for _, val := range lumis {
					switch v := val.(type) {
					case json.Number:
						lumiVal, e := v.Float64()
						if e == nil && lumi == lumiVal {
							row := make(mongo.DASRecord)
							row["file"] = rec["file"]
							out = append(out, row)
						}
					case float64:
						if lumi == v {
							row := make(mongo.DASRecord)
							row["file"] = rec["file"]
							out = append(out, row)
						}
					}
				}
			}
		}
	}
	return out
}

// Blocks4TierDates finds blocks for given tier and dates
func (LocalAPIs) Blocks4TierDates(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	inst := dasquery.Instance
	var out []mongo.DASRecord
	tier := spec["tier"].(string)
	dates := spec["date"].([]string)
	mind := utils.UnixTime(dates[0])
	maxd := utils.UnixTime(dates[1]) + 24*60*60 // inclusive date
	api := "blocks"
	furl := fmt.Sprintf("%s/%s?data_tier_name=%s&min_cdate=%d&max_cdate=%d", dbsUrl(inst), api, tier, mind, maxd)
	resp := utils.FetchResponse(furl, "") // "" specify optional args
	records := DBSUnmarshal(api, resp.Data)
	var blocks []string
	for _, rec := range records {
		blk := rec["block_name"].(string)
		dataset := strings.Split(blk, "#")[0]
		tierName := strings.Split(dataset, "/")[3]
		if tierName == tier && !utils.InList(blk, blocks) {
			blocks = append(blocks, blk)
		}
	}
	for _, name := range blocks {
		rec := make(mongo.DASRecord)
		row := make(mongo.DASRecord)
		row["name"] = name
		rec["block"] = []mongo.DASRecord{row}
		out = append(out, rec)
	}
	return out
}

// Lumi4BlockRun finds lumi for given block and run
func (LocalAPIs) Lumi4BlockRun(dasquery dasql.DASQuery) []mongo.DASRecord {
	keys := []string{"lumi_section_num"}
	return fileRunLumi(dasquery, keys)
}

// DatasetList finds dataset list
func (LocalAPIs) DatasetList(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	inst := dasquery.Instance
	api := "datasetlist"
	furl := fmt.Sprintf("%s/%s", dbsUrl(inst), api)
	switch d := spec["dataset"].(type) {
	case string:
		if strings.Contains(d, "*") { // patterns are not supported by this API
			return []mongo.DASRecord{}
		}
		spec["dataset"] = []string{d} // API accepts list of datasets
	case []string:
		//         fmt.Println("### valid data type", d)
		spec["dataset"] = d
	default:
		return []mongo.DASRecord{} // no other data types are allowed
	}
	spec["detail"] = 1 // get detailed results from DBS
	spec["dataset_access_type"] = "VALID"
	s := spec["status"]
	if s != nil {
		status := s.(string)
		if status != "" {
			spec["dataset_access_type"] = strings.Replace(status, "*", "", -1)
			delete(spec, "status")
		}
	}
	args, err := json.Marshal(spec)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Spec":  spec,
			"Error": err,
		}).Error("DBS datasetlist unable to unmarshal spec")
		return []mongo.DASRecord{}
	}
	resp := utils.FetchResponse(furl, string(args)) // POST request
	records := DBSUnmarshal(api, resp.Data)
	return records
}

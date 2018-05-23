package services

// DAS service module
// Phedex module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
)

// helper function to load data stream and return DAS records
func loadPhedexData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
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
		msg := fmt.Sprintf("Phedex unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		if utils.VERBOSE > 0 {
			logs.WithFields(logs.Fields{
				"Error": err,
				"Api":   api,
				"data":  string(data),
			}).Error("Phedex unable to unmarshal the data")
		}
		out = append(out, mongo.DASErrorRecord(msg, utils.PhedexErrorName, utils.PhedexError))
	}
	out = append(out, rec)
	return out
}

// PhedexUnmarshal unmarshals Phedex data stream and return DAS records based on api
func PhedexUnmarshal(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	records := loadPhedexData(api, data)
	for _, rec := range records {
		if api == "fileReplicas4dataset" || api == "fileReplicas" || api == "fileReplicas4file" {
			data := rec["phedex"]
			if data == nil {
				continue
			}

			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				blocks := val["block"].([]interface{})
				for _, item := range blocks {
					brec := item.(map[string]interface{})
					files := brec["file"].([]interface{})
					for _, elem := range files {
						frec := elem.(map[string]interface{})
						out = append(out, frec)
					}
				}
			}
		} else if api == "groups" {
			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				group := val["group"].([]interface{})
				for _, item := range group {
					row := item.(map[string]interface{})
					r := mongo.DASRecord{"name": row["name"].(string)}
					out = append(out, r)
				}
			}
		} else if api == "blockReplicas" || api == "blockReplicas4dataset" {
			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				blocks := val["block"].([]interface{})
				for _, item := range blocks {
					brec := item.(map[string]interface{})
					out = append(out, brec)
				}
			}
		} else if api == "site4dataset" || api == "site4block" {
			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				blocks := val["block"].([]interface{})
				for _, item := range blocks {
					brec := item.(map[string]interface{})
					replicas := brec["replica"].([]interface{})
					for _, val := range replicas {
						row := val.(map[string]interface{})
						node := ""
						if row["node"] != nil {
							node = row["node"].(string)
						}
						se := ""
						if row["se"] != nil {
							se = row["se"].(string)
						}
						rec := mongo.DASRecord{"name": node, "se": se}
						out = append(out, rec)
					}
				}
			}
		} else if api == "site4file" {
			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				blocks := val["block"].([]interface{})
				for _, item := range blocks {
					brec := item.(map[string]interface{})
					files := brec["file"].([]interface{})
					for _, vvv := range files {
						rep := vvv.(map[string]interface{})
						replicas := rep["replica"].([]interface{})
						for _, val := range replicas {
							row := val.(map[string]interface{})
							node := ""
							if row["node"] != nil {
								node = row["node"].(string)
							}
							se := ""
							if row["se"] != nil {
								se = row["se"].(string)
							}
							rec := mongo.DASRecord{"name": node, "se": se}
							out = append(out, rec)
						}
					}
				}
			}
		} else if api == "dataset4site" || api == "dataset4site_group" || api == "dataset4se" || api == "dataset4se_group" {
			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				blocks := val["block"].([]interface{})
				for _, item := range blocks {
					brec := item.(map[string]interface{})
					dataset := strings.Split(brec["name"].(string), "#")[0]
					rec = mongo.DASRecord{"name": dataset}
					out = append(out, rec)
				}
			}
		} else if api == "block4site" || api == "block4se" {
			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				blocks := val["block"].([]interface{})
				for _, item := range blocks {
					brec := item.(map[string]interface{})
					rec = mongo.DASRecord{"name": brec["name"].(string)}
					out = append(out, rec)
				}
			}
		} else if api == "nodeusage" || api == "groupusage" || api == "nodes" {
			if rec["phedex"] != nil {
				val := rec["phedex"].(map[string]interface{})
				groups := val["node"].([]interface{})
				for _, item := range groups {
					brec := item.(map[string]interface{})
					prec := make(mongo.DASRecord)
					for k, v := range brec {
						prec[k] = v
					}
					out = append(out, prec)
				}
			}
		} else {
			out = append(out, rec)
		}
	}
	return out
}

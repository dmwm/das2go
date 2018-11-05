package services

// DAS service module
// Rucio module
//
// Copyright (c) 2018 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
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
					logs.WithFields(logs.Fields{
						"Error": err,
						"Api":   api,
						"data":  string(row),
					}).Error("Rucio unable to unmarshal the data")
				}
				out = append(out, mongo.DASErrorRecord(msg, utils.RucioErrorName, utils.RucioError))
			}
			out = append(out, rec)
			row = []byte{}
		} else {
			row = append(row, r)
		}
	}
	return out
}

// RucioUnmarshal unmarshals Rucio data stream and return DAS records based on api
func RucioUnmarshal(dasquery dasql.DASQuery, api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	records := loadRucioData(api, data)
	specs := dasquery.Spec
	for _, rec := range records {
		if api == "rses" {
			if val, ok := specs["site"]; ok {
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
			} else {
				fmt.Println("### unable to look-up site from specs", specs, ok)
			}
		} else if api == "replicas" {
			out = append(out, rec)
		} else {
			out = append(out, rec)
		}
	}
	return out
}

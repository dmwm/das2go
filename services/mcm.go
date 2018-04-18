package services

// DAS service module
// McM module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/dmwm/das2go/mongo"
)

// helper function to load McM data stream
func loadMcMData(api string, data []byte) []mongo.DASRecord {
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
		msg := fmt.Sprintf("McM unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		out = append(out, mongo.DASErrorRecord(msg))
	}
	out = append(out, rec)
	return out
}

// McMUnmarshal unmarshals McM data stream and return DAS records based on api
func McMUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadMcMData(api, data)
	var out []mongo.DASRecord
	var r []interface{}
	if api == "dataset4mcm" {
		for _, rec := range records {
			for _, v := range rec {
				for _, r := range v.([]interface{}) {
					nrec := make(mongo.DASRecord)
					nrec["name"] = r
					out = append(out, nrec)
				}
			}
		}
	} else {
		for _, rec := range records {
			nrec := make(mongo.DASRecord)
			r = append(r, rec["results"])
			nrec["mcm"] = r
			out = append(out, nrec)
		}
	}
	return out
}

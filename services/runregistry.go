package services

// DAS service module
// RunRegistry module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

// example of DAS calls
// http://localhost:8081/runregistry/api/GLOBAL/runsummary/json/number%2CstartTime%2CstopTime%2Ctriggers%2CrunClassName%2CrunStopReason%2Cbfield%2CgtKey%2Cl1Menu%2ChltKeyDescription%2ClhcFill%2ClhcEnergy%2CrunCreated%2Cmodified%2ClsCount%2ClsRanges/none/data {"filter": {"number": ">= 165103 and <= 165110"}}

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// helper function to load RunRegistry data stream
func loadRunRegistryData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	if len(data) == 0 {
		return out
	}
	// to prevent json.Unmarshal behavior to convert all numbers to float
	// we'll use json decode method with instructions to use numbers as is
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := dec.Decode(&out)

	// original way to decode data
	// err := json.Unmarshal(data, &out)
	if err != nil {
		msg := fmt.Sprintf("RunRegistry unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		if utils.VERBOSE > 0 {
			log.Printf("ERROR: RunRegistry unable to unmarshal, data %+v, api %v, error %v\n", string(data), api, err)
		}
		out = append(out, mongo.DASErrorRecord(msg, utils.RunRegistryErrorName, utils.RunRegistryError))
	}
	return out
}

// RunRegistryUnmarshal unmarshals RunRegistry data stream and return DAS records based on api
func RunRegistryUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadRunRegistryData(api, data)
	var out []mongo.DASRecord
	if api == "rr_xmlrpc2" {
		for _, rec := range records {
			if rec["number"] != nil {
				run := rec["number"].(json.Number)
				v, e := run.Int64()
				if e == nil {
					rec["run_number"] = v
				} else {
					rec["run_number"] = run
				}
				delete(rec, "number")
				out = append(out, rec)
			} else {
				out = append(out, rec)
			}
		}
		return out
	}
	return records
}

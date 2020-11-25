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
	"log"
	"strings"

	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// helper function to load McM data stream
func loadMcMData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord

	// McM output for https://cms-pdmv.cern.ch/mcm/public/restapi/requests/produces/WToENu_Tune4C_13TeV-pythia8/Fall13dr-tsg_PU40bx25_POSTLS162_V2-v1/GEN-SIM-RAW
	// returns formatted JSON with \n because of text/html Content-type.
	// We convert bytes back to string and replace/trim unnecessary stuff
	if api == "mcm4dataset" && strings.Contains(string(data), "\\n") {
		r := strings.Replace(string(data), "\\n", "", -1)
		r = strings.Replace(r, " ", "", -1)
		r = strings.Replace(r, "\\", "", -1)
		r = strings.Trim(r, "\"")
		data = []byte(r)
	}

	// to prevent json.Unmarshal behavior to convert all numbers to float
	// we'll use json decode method with instructions to use numbers as is
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := dec.Decode(&rec)

	// original way to decode data
	//     err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("McM unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		if utils.VERBOSE > 0 {
			log.Printf("ERROR: McM unable to unmarshal, data %+v, api %v, error %v\n", string(data), api, err)
		}
		out = append(out, mongo.DASErrorRecord(msg, utils.McMErrorName, utils.McMError))
		return out
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
				switch t := v.(type) {
				case []interface{}:
					for _, r := range t {
						nrec := make(mongo.DASRecord)
						nrec["name"] = r
						out = append(out, nrec)
					}
				default:
					log.Println("WARNING: wrong data type of McM record", v)
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

package services

// DAS service module
// CondDB module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// helper function to load CondDB data stream
func loadCondDBData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	err := json.Unmarshal(data, &out)
	if err != nil {
		msg := fmt.Sprintf("CondDB unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		if utils.VERBOSE > 0 {
			log.Printf("ERROR: CondDB unable to unmarshal, data %+v, api %v, error %v\n", string(data), api, err)
		}
		out = append(out, mongo.DASErrorRecord(msg, utils.CondDBErrorName, utils.CondDBError))
	}
	return out
}

// CondDBUnmarshal unmarshals CondDB data stream and return DAS records based on api
func CondDBUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadCondDBData(api, data)
	var out []mongo.DASRecord
	if api == "get_run_info" || api == "get_run_info4date" {
		for _, rec := range records {
			r := make(mongo.DASRecord)
			rv := rec["Run"]
			if rv != nil {
				r["run_number"] = fmt.Sprintf("%d", int(rv.(float64)))
			}
			r["delivered_lumi"] = rec["DeliveredLumi"]
			out = append(out, r)
		}
		return out
	}
	return records
}

package services

// DAS service module
// McM module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/das2go/mongo"
)

// helper function to load McM data stream
func loadMcMData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("McM unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		out = append(out, mongo.DASErrorRecord(msg))
	}
	out = append(out, rec)
	return out
}

// Unmarshal McM data stream and return DAS records based on api
func McMUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadMcMData(api, data)
	var out []mongo.DASRecord
	for _, rec := range records {
		nrec := make(mongo.DASRecord)
		nrec["mcm"] = rec["results"]
		out = append(out, nrec)
	}
	return out
}

/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DBS module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"fmt"
	"mongo"
)

// helper function to load DBS data stream
func loadDBSData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	err := json.Unmarshal(data, &out)
	if err != nil {
		var out2 interface{}
		json.Unmarshal(data, &out2)
		fmt.Println("DBS unable to unmarshal the data into DAS record", api, out2)
	}
	return out
}

// Unmarshal Phedex data stream and return DAS records based on api
func DBSUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadDBSData(api, data)
	var out []mongo.DASRecord
	if api == "dataset_info" {
		for _, rec := range records {
			rec["name"] = rec["dataset"]
			delete(rec, "dataset")
			out = append(out, rec)
		}
		return out
	}
	return records
}

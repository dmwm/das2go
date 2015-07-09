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
	"mongo"
)

// helper function to load DBS data stream
func loadDBSData(data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	err := json.Unmarshal(data, &out)
	if err != nil {
		panic(err)
	}
	return out
}

// Unmarshal Phedex data stream and return DAS records based on api
func DBSUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadDBSData(data)
	return records
}

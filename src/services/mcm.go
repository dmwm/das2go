/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: McM module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"fmt"
	"mongo"
)

// helper function to load McM data stream
func loadMcMData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("McM unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		panic(msg)
	}
	out = append(out, rec)
	return out
}

// Unmarshal McM data stream and return DAS records based on api
func McMUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadMcMData(api, data)
	return records
}

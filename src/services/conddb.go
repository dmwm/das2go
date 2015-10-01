/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: CondDB module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"fmt"
	"mongo"
)

// helper function to load CondDB data stream
func loadCondDBData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	err := json.Unmarshal(data, &out)
	if err != nil {
		msg := fmt.Sprintf("CondDB unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		panic(msg)
	}
	return out
}

// Unmarshal CondDB data stream and return DAS records based on api
func CondDBUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadCondDBData(api, data)
	return records
}

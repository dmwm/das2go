/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: SiteDB module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"fmt"
	"mongo"
)

// helper function to load SiteDB data stream
func loadSiteDBData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("SiteDB unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		panic(msg)
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	values := rec["result"].([]interface{})
	for _, item := range values {
		row := make(mongo.DASRecord)
		val := item.([]interface{})
		for i, h := range headers {
			key := h.(string)
			row[key] = val[i]
			if key == "username" {
				row["name"] = row[key]
			}
		}
		out = append(out, row)
	}
	return out
}

// Unmarshal SiteDB data stream and return DAS records based on api
func SiteDBUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadSiteDBData(api, data)
	return records
}

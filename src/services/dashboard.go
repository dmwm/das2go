/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: Dashboard module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"fmt"
	"mongo"
)

// helper function to load Dashboard data stream
func loadDashboardData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("Dashboard unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		panic(msg)
	}
	val := rec["summaries"]
	summaries := val.([]mongo.DASRecord)
	for _, row := range summaries {
		out = append(out, row)
	}
	return out
}

// Unmarshal Dashboard data stream and return DAS records based on api
func DashboardUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadDashboardData(api, data)
	return records
}

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
	"github.com/vkuznet/das2go/mongo"
)

// helper function to load CondDB data stream
func loadCondDBData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	err := json.Unmarshal(data, &out)
	if err != nil {
		msg := fmt.Sprintf("CondDB unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		out = append(out, mongo.DASErrorRecord(msg))
	}
	return out
}

// Unmarshal CondDB data stream and return DAS records based on api
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

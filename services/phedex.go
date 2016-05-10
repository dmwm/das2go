/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: Phedex module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"fmt"
	"github.com/vkuznet/das2go/mongo"
	"strings"
)

// helper function to load data stream and return DAS records
func loadPhedexData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("Phedex unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		out = append(out, mongo.DASErrorRecord(msg))
	}
	out = append(out, rec)
	return out
}

// Unmarshal Phedex data stream and return DAS records based on api
func PhedexUnmarshal(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	records := loadPhedexData(api, data)
	for _, rec := range records {
		if api == "fileReplicas4dataset" || api == "fileReplicas" || api == "fileReplicas4file" {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				files := brec["file"].([]interface{})
				for _, elem := range files {
					frec := elem.(map[string]interface{})
					out = append(out, frec)
				}
			}
		} else if api == "groups" {
			val := rec["phedex"].(map[string]interface{})
			group := val["group"].([]interface{})
			for _, item := range group {
				row := item.(map[string]interface{})
				r := mongo.DASRecord{"name": row["name"].(string)}
				out = append(out, r)
			}
		} else if api == "blockReplicas" || api == "blockReplicas4dataset" {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				out = append(out, brec)
			}
		} else if api == "site4dataset" {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				replicas := brec["replica"].([]interface{})
				for _, val := range replicas {
					row := val.(map[string]interface{})
					node := row["node"].(string)
					se := row["se"].(string)
					rec := mongo.DASRecord{"name": node, "se": se}
					out = append(out, rec)
				}
			}
		} else if api == "dataset4site" || api == "dataset4site_group" || api == "dataset4se" || api == "dataset4se_group" {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				dataset := strings.Split(brec["name"].(string), "#")[0]
				rec = mongo.DASRecord{"name": dataset}
				out = append(out, rec)
			}
		} else if api == "block4site" || api == "block4se" {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				rec = mongo.DASRecord{"name": brec["name"].(string)}
				out = append(out, rec)
			}
		} else if api == "nodeusage" || api == "groupusage" || api == "nodes" {
			val := rec["phedex"].(map[string]interface{})
			groups := val["node"].([]interface{})
			for _, item := range groups {
				brec := item.(map[string]interface{})
				prec := make(mongo.DASRecord)
				for k, v := range brec {
					prec[k] = v
				}
				out = append(out, prec)
			}
		} else {
			out = append(out, rec)
		}
	}
	return out
}

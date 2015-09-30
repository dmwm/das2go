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
	"mongo"
	"strings"
)

// helper function to load data stream and return DAS records
func loadPhedexData(data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		panic(err)
	}
	out = append(out, rec)
	return out
}

// Unmarshal Phedex data stream and return DAS records based on api
func PhedexUnmarshal(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	records := loadPhedexData(data)
	for _, rec := range records {
		if api == "fileReplicas4dataset" || api == "fileReplicas" {
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
		} else if api == "blockReplicas" {
			val := rec["phedex"].(map[string]interface{})
			blocks := val["block"].([]interface{})
			for _, item := range blocks {
				brec := item.(map[string]interface{})
				out = append(out, brec)
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
		} else {
			out = append(out, rec)
		}
	}
	return out
}

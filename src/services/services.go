/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: Services module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"dasmaps"
	"dasql"
	"mongo"
	"utils"
)

// remap function uses DAS notations and convert series of DAS records
// into another set where appropriate remapping is done
func remap(system, api string, records []mongo.DASRecord, notations []mongo.DASRecord, expire int) []mongo.DASRecord {
	var out []mongo.DASRecord
	keys := utils.MapKeys(records[0])
	for _, rec := range records {
		for _, row := range notations {
			apiname, api_key, rec_key := dasmaps.GetNotation(row)
			if apiname != "" {
				if apiname == api && utils.InList(api_key, keys) {
					rec[rec_key] = rec[api_key]
					delete(rec, api_key)
				}
			} else {
				if utils.InList(api_key, keys) {
					rec[rec_key] = rec[api_key]
				}
			}
			dasheader := dasHeader()
			systems := dasheader["system"].([]string)
			apis := dasheader["api"].([]string)
			systems = append(systems, system)
			apis = append(apis, apiname)
			dasheader["system"] = systems
			dasheader["api"] = apis
			dasheader["expire"] = expire
		}
		out = append(out, rec)
	}
	return out
}

func Unmarshal(system, api string, data []byte, notations []mongo.DASRecord, expire int) []mongo.DASRecord {
	var out []mongo.DASRecord
	switch {
	case system == "phedex":
		out = PhedexUnmarshal(api, data)
	case system == "dbs3":
		out = DBSUnmarshal(api, data)
	}
	return remap(system, api, out, notations, expire)
}

func dasHeader() mongo.DASRecord {
	das := make(mongo.DASRecord)
	das["expire"] = 0
	das["primary_key"] = ""
	das["instance"] = ""
	das["api"] = []string{}
	das["system"] = []string{}
	return das

}

// adjust DAS record and add (if necessary) leading key from DAS query
func AdjustRecords(dasquery dasql.DASQuery, records []mongo.DASRecord) []mongo.DASRecord {
	var out []mongo.DASRecord
	fields := dasquery.Fields
	if len(fields) > 1 {
		return records
	}
	skey := fields[0]
	for _, rec := range records {
		keys := utils.MapKeys(rec)
		if utils.InList(skey, keys) {
			out = append(out, rec)
		} else {
			newrec := make(mongo.DASRecord)
			newrec[skey] = rec
			out = append(out, newrec)
		}
	}
	return out
}

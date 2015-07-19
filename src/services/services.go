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
	"labix.org/v2/mgo/bson"
	"mongo"
	"utils"
)

// remap function uses DAS notations and convert series of DAS records
// into another set where appropriate remapping is done
func remap(api string, records []mongo.DASRecord, notations []mongo.DASRecord) []mongo.DASRecord {
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
		}
		out = append(out, rec)
	}
	return out
}

func Unmarshal(system, api string, data []byte, notations []mongo.DASRecord) []mongo.DASRecord {
	var out []mongo.DASRecord
	switch {
	case system == "phedex":
		out = PhedexUnmarshal(api, data)
	case system == "dbs3":
		out = DBSUnmarshal(api, data)
	}
	return remap(api, out, notations)
}

func DASHeader() mongo.DASRecord {
	das := make(mongo.DASRecord)
	das["expire"] = 60 // default expire
	das["record"] = 1  // by default it is a data record (1 vs das record 0)
	das["primary_key"] = ""
	das["instance"] = ""
	das["api"] = []string{}
	das["system"] = []string{}
	return das

}

// adjust DAS record and add (if necessary) leading key from DAS query
func AdjustRecords(dasquery dasql.DASQuery, system, api string, records []mongo.DASRecord, expire int) []mongo.DASRecord {
	var out []mongo.DASRecord
	fields := dasquery.Fields
	qhash := dasquery.Qhash
	if len(fields) > 1 {
		return records
	}
	skey := fields[0]
	for _, rec := range records {
		// DAS header for records
		dasheader := DASHeader()
		systems := dasheader["system"].([]string)
		apis := dasheader["api"].([]string)
		systems = append(systems, system)
		dasheader["system"] = systems
		dasheader["expire"] = utils.Expire(expire)
		apis = append(apis, api)
		dasheader["api"] = apis

		keys := utils.MapKeys(rec)
		if utils.InList(skey, keys) {
			rec["qhash"] = qhash
			rec["das"] = dasheader
			out = append(out, rec)
		} else {
			newrec := make(mongo.DASRecord)
			newrec[skey] = rec
			newrec["qhash"] = qhash
			newrec["das"] = dasheader
			out = append(out, newrec)
		}
	}
	return out
}

// create DAS record for DAS cache
func CreateDASRecord(dasquery dasql.DASQuery, status string, srvs []string) mongo.DASRecord {
	dasrecord := make(mongo.DASRecord)
	dasrecord["query"] = dasquery.Query
	dasrecord["qhash"] = dasquery.Qhash
	dasrecord["instance"] = dasquery.Instance
	dasheader := DASHeader()
	dasheader["record"] = 0
	dasheader["status"] = status
	dasheader["services"] = srvs
	dasheader["system"] = []string{"das"}
	dasheader["expire"] = utils.Expire(60) // initial expire
	dasheader["api"] = []string{"das"}
	dasrecord["das"] = dasheader
	return dasrecord
}

// get DAS record from das cache
func GetDASRecord(uri, dbname, coll string, dasquery dasql.DASQuery) mongo.DASRecord {
	spec := bson.M{"qhash": dasquery.Qhash, "das.record": 0}
	rec := mongo.Get(uri, dbname, coll, spec)
	return rec[0]
}

// update DAS record in das cache
func UpdateDASRecord(uri, dbname, coll, qhash string, dasrecord mongo.DASRecord) {
	spec := bson.M{"qhash": qhash, "das.record": 0}
	newdata := bson.M{"query": dasrecord["query"], "qhash": dasrecord["qhash"], "instance": dasrecord["instance"], "das": dasrecord["das"]}
	mongo.Update(uri, dbname, coll, spec, newdata)
}

// helper function to get expire value from DAS/data record
func GetExpire(rec mongo.DASRecord) int64 {
	das := rec["das"].(mongo.DASRecord)
	expire := das["expire"].(int64)
	return expire
}

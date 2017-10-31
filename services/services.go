package services

// DAS service module
// It aggregates information from: DBS, PhEDEx, SiteDB, McM, ReqMgr, Dashboard, RunSummary, CondDB
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"strings"
	"time"

	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	"gopkg.in/mgo.v2/bson"
)

// remap function uses DAS notations and convert series of DAS records
// into another set where appropriate remapping is done
func remap(api string, records []mongo.DASRecord, notations []mongo.DASRecord) []mongo.DASRecord {
	var out []mongo.DASRecord
	if len(records) == 0 {
		return records
	}
	keys := utils.MapKeys(records[0])
	for _, rec := range records {
		for _, row := range notations {
			apiname, apiKey, recKey := dasmaps.GetNotation(row)
			if apiname != "" {
				if apiname == api && utils.InList(apiKey, keys) {
					rec[recKey] = rec[apiKey]
					delete(rec, apiKey)
				}
			} else {
				if utils.InList(apiKey, keys) {
					if _, ok := rec[recKey]; !ok {
						rec[recKey] = rec[apiKey]
					}
				}
			}
		}
		out = append(out, rec)
	}
	return out
}

// Unmarshal generic function to unmarshal DAS record for given system/api/data/notations
func Unmarshal(dasquery dasql.DASQuery, system, api string, r utils.ResponseType, notations []mongo.DASRecord, pkeys []string) []mongo.DASRecord {
	var out []mongo.DASRecord
	if r.Error != nil {
		rec := CreateDASErrorRecord(dasquery, pkeys)
		out = append(out, rec)
		return out
	}
	data := r.Data
	switch {
	case system == "phedex":
		out = PhedexUnmarshal(api, data)
	case system == "dbs3" || system == "dbs":
		out = DBSUnmarshal(api, data)
	case system == "reqmgr" || system == "reqmgr2":
		out = ReqMgrUnmarshal(api, data)
	case system == "mcm":
		out = McMUnmarshal(api, data)
	case system == "dashboard":
		out = DashboardUnmarshal(api, data)
	case system == "conddb":
		out = CondDBUnmarshal(api, data)
	case system == "runregistry":
		out = RunRegistryUnmarshal(api, data)
	case system == "sitedb2":
		out = SiteDBUnmarshal(api, data)
	}
	return remap(api, out, notations)
}

// DASHeader represents DAS Header
func DASHeader() mongo.DASRecord {
	das := make(mongo.DASRecord)
	das["expire"] = 60 // default expire
	das["record"] = 1  // by default it is a data record (1 vs das record 0)
	das["primary_key"] = ""
	das["instance"] = ""
	das["services"] = []string{}
	return das

}

// AdjustRecords adjusts DAS record and add (if necessary) leading key from DAS query
func AdjustRecords(dasquery dasql.DASQuery, system, api string, records []mongo.DASRecord, expire int, pkeys []string) []mongo.DASRecord {
	var out []mongo.DASRecord
	fields := dasquery.Fields
	qhash := dasquery.Qhash
	spec := dasquery.Spec
	skey := fields[0]
	for _, rec := range records {
		if rec == nil {
			rec = make(mongo.DASRecord)
		}
		// Check that spec key:values are presented in a record
		prim_key := strings.Split(pkeys[0], ".")
		if len(prim_key) == 2 && !utils.InList(prim_key[1], utils.MapKeys(rec)) {
			if val, ok := spec[prim_key[0]]; ok {
				rec[prim_key[1]] = val
			}
		}
		// DAS header for records
		dasheader := DASHeader()
		srvs := dasheader["services"].([]string)
		srv := strings.Join([]string{system, api}, ":")
		srvs = append(srvs, srv)
		dasheader["services"] = srvs
		dasheader["expire"] = utils.Expire(expire)
		dasheader["primary_key"] = pkeys[0]
		dasheader["instance"] = dasquery.Instance

		keys := utils.MapKeys(rec)
		if utils.InList(skey, keys) {
			rec["qhash"] = qhash
			rec["das"] = dasheader
			out = append(out, rec)
		} else {
			newrec := make(mongo.DASRecord)
			newrec[skey] = []mongo.DASRecord{rec} // record internal type must be list
			newrec["qhash"] = qhash
			newrec["das"] = dasheader
			out = append(out, newrec)
		}
	}
	return out
}

// CreateDASRecord creates DAS record for DAS cache
func CreateDASRecord(dasquery dasql.DASQuery, srvs, pkeys []string) mongo.DASRecord {
	dasrecord := make(mongo.DASRecord)
	dasrecord["query"] = dasquery.Query
	dasrecord["qhash"] = dasquery.Qhash
	dasheader := DASHeader()
	dasheader["record"] = 0           // DAS record type, zero for DAS record
	dasheader["status"] = "requested" // initial status
	dasheader["services"] = srvs
	if len(pkeys) > 0 {
		dasheader["primary_key"] = pkeys[0]
	} else {
		dasheader["primary_key"] = ""
	}
	dasheader["expire"] = utils.Expire(60) // initial expire, 60 seconds from now
	dasheader["ts"] = time.Now().Unix()
	dasheader["instance"] = dasquery.Instance
	dasrecord["das"] = dasheader
	return dasrecord
}

// CreateDASErrorRecord creates DAS record for DAS cache
func CreateDASErrorRecord(dasquery dasql.DASQuery, pkeys []string) mongo.DASRecord {
	dasrecord := make(mongo.DASRecord)
	dasrecord["query"] = dasquery.Query
	dasrecord["qhash"] = dasquery.Qhash
	dasheader := DASHeader()
	dasheader["record"] = 0    // DAS record type, zero for DAS record
	dasheader["status"] = "ok" // initial status
	dasheader["services"] = []string{"das:NA"}
	if len(pkeys) > 0 {
		dasheader["primary_key"] = pkeys[0]
	} else {
		dasheader["primary_key"] = ""
	}
	dasheader["expire"] = utils.Expire(600) // initial expire, 600 seconds from now
	dasheader["ts"] = time.Now().Unix()
	dasheader["instance"] = dasquery.Instance
	dasrecord["das"] = dasheader
	return dasrecord
}

// GetDASRecord gets DAS record from das cache
func GetDASRecord(dasquery dasql.DASQuery) mongo.DASRecord {
	spec := bson.M{"qhash": dasquery.Qhash, "das.record": 0}
	rec := mongo.Get("das", "cache", spec, 0, 1)
	if len(rec) > 0 {
		return rec[0]
	}
	return CreateDASErrorRecord(dasquery, []string{})
}

// GetMinExpire gets DAS min expire timestamp out of DAS record
func GetMinExpire(dasquery dasql.DASQuery) int64 {
	expire := utils.Expire(3600)
	spec := bson.M{"qhash": dasquery.Qhash}
	records := mongo.Get("das", "cache", spec, 0, -1) // get all records
	for _, rec := range records {
		dasExpire := GetExpire(rec)
		if dasExpire < expire {
			expire = dasExpire
		}
	}
	return expire
}

// UpdateDASRecord updates DAS record in das cache
func UpdateDASRecord(qhash string, dasrecord mongo.DASRecord) {
	spec := bson.M{"qhash": qhash, "das.record": 0}
	newdata := bson.M{"query": dasrecord["query"], "qhash": dasrecord["qhash"], "instance": dasrecord["instance"], "das": dasrecord["das"]}
	mongo.Update("das", "cache", spec, newdata)
}

// GetExpire helper function to get expire value from DAS/data record
func GetExpire(rec mongo.DASRecord) int64 {
	das := rec["das"].(mongo.DASRecord)
	expire := das["expire"].(int64)
	return expire
}

// MergeDASRecords merges DAS data records
func MergeDASRecords(dasquery dasql.DASQuery) ([]mongo.DASRecord, int64) {
	// get DAS record and extract primary key
	spec := bson.M{"qhash": dasquery.Qhash, "das.record": 0}
	records := mongo.Get("das", "cache", spec, 0, 1)
	dasrecord := records[0]
	das := dasrecord["das"].(mongo.DASRecord)
	lkeys := dasquery.Fields
	pkey := das["primary_key"].(string)
	mkey := strings.Split(pkey, ".")[0]
	// get DAS data record sorted by primary key
	spec = bson.M{"qhash": dasquery.Qhash, "das.record": 1}
	var skeys []string
	skeys = append(skeys, pkey)
	records = mongo.GetSorted("das", "cache", spec, skeys)
	if len(lkeys) > 1 {
		status := das["status"].(string)
		expire := das["expire"].(int64)
		for _, rec := range records {
			das := rec["das"].(mongo.DASRecord)
			das["status"] = status
			rec["das"] = das
		}
		return records, expire
	}

	// loop over data records and merge them, extract smallest expire timestamp
	var expire int64
	expire = time.Now().Unix() * 2
	var out []mongo.DASRecord
	var oldrec, rec mongo.DASRecord
	for idx, rec := range records {
		if idx == 0 { // we need to advance to new record because of init conditions above
			oldrec = rec
			continue
		}
		das := rec["das"].(mongo.DASRecord)
		dasexpire := das["expire"].(int64)
		if expire > dasexpire {
			expire = dasexpire
		}
		data1, err1 := mongo.GetStringValue(oldrec, pkey)
		data2, err2 := mongo.GetStringValue(rec, pkey)
		if err1 == nil && err2 == nil && data1 != data2 {
			out = append(out, oldrec)
		} else {
			rec = mergeRecords(rec, oldrec, mkey, dasquery.Qhash)
		}
		oldrec = rec
	}
	// we still left with last oldrec which should be merged with last record from the loop
	if rec[mkey] == nil {
		out = append(out, oldrec)
	}
	return out, expire
}

// helper function to get DAS records from different interfaces
func getRecords(rec mongo.DASRecord, pkey string) []mongo.DASRecord {
	var out []mongo.DASRecord
	switch records := rec[pkey].(type) {
	case []mongo.DASRecord:
		for _, r := range records {
			out = append(out, r)
		}
	case []interface{}:
		for _, r := range records {
			out = append(out, r.(mongo.DASRecord))
		}
	}
	return out
}

// function to merge DAS data records on given key
func mergeRecords(oldrec, newrec mongo.DASRecord, pkey, qhash string) mongo.DASRecord {
	var rec, records []mongo.DASRecord
	// when we look-up via primary key we always should get list of DAS records
	records = getRecords(oldrec, pkey)
	for _, r := range records {
		rec = append(rec, r)
	}
	records = getRecords(newrec, pkey)
	for _, r := range records {
		rec = append(rec, r)
	}
	das := mergeDASparts(oldrec["das"].(mongo.DASRecord), newrec["das"].(mongo.DASRecord))
	return mongo.DASRecord{pkey: rec, "qhash": qhash, "das": das}
}

// helper function to extract services from das record
func services(das mongo.DASRecord) []string {
	var srvs []string
	switch services := das["services"].(type) {
	case []string:
		for _, srv := range services {
			srvs = append(srvs, srv)
		}
	case []interface{}:
		for _, srv := range services {
			srvs = append(srvs, srv.(string))
		}

	}
	return srvs
}

// helper function to merge das parts of DAS records
func mergeDASparts(das1, das2 mongo.DASRecord) mongo.DASRecord {
	das := make(mongo.DASRecord)
	var srvs []string
	srvs1 := services(das1)
	srvs2 := services(das2)
	for _, srv := range srvs1 {
		srvs = append(srvs, srv)
	}
	for _, srv := range srvs2 {
		srvs = append(srvs, srv)
	}
	das["services"] = srvs
	var expire int64
	expire = time.Now().Unix() * 2
	ex1, err1 := mongo.GetInt64Value(das1, "expire")
	ex2, err2 := mongo.GetInt64Value(das2, "expire")
	if err1 == nil && ex1 < expire {
		expire = ex1
	}
	if err2 == nil && ex2 < expire {
		expire = ex2
	}
	das["expire"] = expire
	das["status"] = "ok" // merged step should return ok status
	das["primary_key"] = das1["primary_key"]
	das["instance"] = das1["instance"]
	das["record"] = 1
	return das
}

// UpdateExpire helper function to fix all DAS cache record expire timestamps
func UpdateExpire(qhash string, records []mongo.DASRecord, dasexpire int64) []mongo.DASRecord {
	var out []mongo.DASRecord
	for _, rec := range records {
		das := rec["das"].(mongo.DASRecord)
		das["expire"] = dasexpire
		rec["das"] = das
		out = append(out, rec)
	}
	return out
}

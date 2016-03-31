// das2go/das - Core system of DAS server
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//
package das

import (
	"fmt"
	"github.com/vkuznet/das2go/dasmaps"
	"github.com/vkuznet/das2go/dasql"
	"github.com/vkuznet/das2go/mongo"
	"github.com/vkuznet/das2go/services"
	"github.com/vkuznet/das2go/utils"
	"gopkg.in/mgo.v2/bson"
	"log"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// DASRecord is a main entity DAS server operates
type Record map[string]interface{}
type DASRecord struct {
	query  dasql.DASQuery
	record Record
	das    Record
}

func (r *DASRecord) Qhash() string {
	return string(r.query.Qhash)
}

func (r *DASRecord) Services() []string {
	return []string{}
}

// Extract API call parameters from das map entry
func getApiParams(dasmap mongo.DASRecord) (string, string, string, string) {
	das_key, ok := dasmap["das_key"].(string)
	if !ok {
		das_key = ""
	}
	rec_key, ok := dasmap["rec_key"].(string)
	if !ok {
		rec_key = ""
	}
	api_arg, ok := dasmap["api_arg"].(string)
	if !ok {
		api_arg = ""
	}
	pattern, ok := dasmap["pattern"].(string)
	if !ok {
		pattern = ""
	}
	return das_key, rec_key, api_arg, pattern
}

// Form appropriate URL from given dasquery and dasmap, the final URL
// contains all parameters
func formUrlCall(dasquery dasql.DASQuery, dasmap mongo.DASRecord) string {
	spec := dasquery.Spec
	skeys := utils.MapKeys(spec)
	base, ok := dasmap["url"].(string)
	system, _ := dasmap["system"].(string)
	// Adjust DBS URL wrt dbs instance name from query
	if system == "dbs" || system == "dbs3" {
		dbsInst := dasquery.Instance
		if len(dbsInst) > 0 && dbsInst != "prod/global" {
			base = strings.Replace(base, "prod/global", dbsInst, -1)
		}
	}
	if system == "sitedb2" {
		// all sitedb apis is better to treat as local APIs, since
		// they don't really accept parameters. Instead, we'll use local
		// APIs to fetch all data and match records with given parameters
		return "local_api"
	}
	if !strings.HasPrefix(base, "http") {
		return "local_api"
	}
	// Exception block, current DAS maps contains APIs which should be treated
	// as local apis, e.g. file_run_lumi4dataset in DBS3 maps. In a future
	// I'll need to fix DBS3 maps to make it local_api
	// For time being I'll list those exceptional APIs in DASLocalAPIs list
	urn, _ := dasmap["urn"].(string)
	if utils.InList(urn, services.DASLocalAPIs()) {
		return "local_api"
	}
	// TMP, until we change phedex maps to use JSON
	if strings.Contains(base, "phedex") {
		base = strings.Replace(base, "xml", "json", -1)
	}
	if !ok {
		log.Fatal("Unable to extract url from DAS map", dasmap)
	}
	dasmaps := dasmaps.GetDASMaps(dasmap["das_map"])
	vals := url.Values{}
	var use_args []string
	for _, dmap := range dasmaps {
		dkey, rkey, arg, pat := getApiParams(dmap)
		if utils.InList(dkey, skeys) {
			val, ok := spec[dkey].(string)
			if ok {
				matched, _ := regexp.MatchString(pat, val)
				if matched || pat == "" {
					// exception for lumi_list input parameter, files DBS3 API accept only lists of lumis
					if system == "dbs3" && arg == "lumi_list" {
						vals.Add(arg, fmt.Sprintf("[%s]", val))
					} else if system == "dbs3" && arg == "status" {
						// This may need revision, probably better to properly
						// adjust DAS maps
						if strings.ToLower(val) == "valid" {
							vals.Add("validFileOnly", "1")
						} else {
							vals.Add("validFileOnly", "0")
						}
					} else if system == "dbs3" && arg == "min_cdate" {
						vals.Add(arg, fmt.Sprintf("%d", utils.UnixTime(val)))
						maxd := utils.UnixTime(val) + 24*60*60
						vals.Add("max_cdate", fmt.Sprintf("%d", maxd))
					} else if dkey == "date" && system == "conddb" {
						vals.Add("startTime", utils.ConddbTime(val))
						eval := utils.Unix2DASTime(utils.UnixTime(val) + 37*3660)
						vals.Add("endTime", utils.ConddbTime(eval))
						use_args = append(use_args, arg)
					} else {
						vals.Add(arg, val)
					}
					use_args = append(use_args, arg)
				}
			} else { // let's try array of strings
				arr, ok := spec[dkey].([]string)
				if !ok {
					log.Println("WARNING, unable to get value(s) for daskey=", dkey,
						", reckey=", rkey, " from spec=", spec, " das map=", dmap)
				}
				if dkey == "date" && system == "dbs3" {
					vals.Add("min_cdate", fmt.Sprintf("%d", utils.UnixTime(arr[0])))
					vals.Add("max_cdate", fmt.Sprintf("%d", utils.UnixTime(arr[1])))
					use_args = append(use_args, arg)
				} else if dkey == "date" && system == "dashboard" {
					vals.Add("date1", utils.DashboardTime(arr[0]))
					vals.Add("date2", utils.DashboardTime(arr[1]))
					use_args = append(use_args, arg)
				} else if dkey == "date" && system == "conddb" {
					vals.Add("startTime", utils.ConddbTime(arr[0]))
					vals.Add("endTime", utils.ConddbTime(arr[1]))
					use_args = append(use_args, arg)
				} else if system == "conddb" && arg == "Runs" {
					if len(arr) > 0 {
						vals.Add(arg, strings.Join(arr, ","))
						use_args = append(use_args, arg)
					}
				} else {
					for _, val := range arr {
						matched, _ := regexp.MatchString(pat, val)
						if matched || pat == "" {
							vals.Add(arg, val)
							use_args = append(use_args, arg)
						}
					}
				}
			}
		}
	}
	// loop over params in DAS maps and add additional arguments which have
	// non empty, non optional and non required values
	skipList := []string{"optional", "required"}
	params := dasmap["params"].(mongo.DASRecord)
	for key, val := range params {
		vvv := val.(string)
		if !utils.InList(key, use_args) && !utils.InList(vvv, skipList) {
			vals.Add(key, vvv)
		}
	}

	// Encode all arguments for url
	args := vals.Encode()
	if len(vals) < len(skeys) {
		return "" // number of arguments should be equal or more number of spec key values
	}
	if len(args) > 0 {
		return base + "?" + args
	}
	return base
}

// Form appropriate URL from given dasquery and dasmap, the final URL
// contains all parameters
func formRESTUrl(dasquery dasql.DASQuery, dasmap mongo.DASRecord) string {
	spec := dasquery.Spec
	skeys := utils.MapKeys(spec)
	base, ok := dasmap["url"].(string)
	if !ok {
		log.Fatal("Unable to extract url from DAS map", dasmap)
	}
	if !strings.HasPrefix(base, "http") {
		return "local_api"
	}
	// Exception block, current DAS maps contains APIs which should be treated
	// as local apis, e.g. reqmgr_config_cache
	urn, _ := dasmap["urn"].(string)
	if utils.InList(urn, services.DASLocalAPIs()) {
		return "local_api"
	}
	dasmaps := dasmaps.GetDASMaps(dasmap["das_map"])
	for _, dmap := range dasmaps {
		dkey, _, _, pat := getApiParams(dmap)
		if utils.InList(dkey, skeys) {
			msg := fmt.Sprintf("Invalid '%T' type for '%s' DAS key in %v", spec[dkey], dkey, dmap)
			switch spec[dkey].(type) {
			case string:
				val, _ := spec[dkey].(string)
				matched, _ := regexp.MatchString(pat, val)
				if matched || pat == "" {
					if !(strings.HasSuffix(base, "/") && strings.HasPrefix(val, "/")) {
						return base + "/" + val
					} else {
						return base + val
					}
				}
			case []string:
				val, _ := spec[dkey].([]string)
				matched, _ := regexp.MatchString(pat, val[0])
				if matched || pat == "" {
					return base
				}
			default:
				panic(msg)
			}
		}
	}
	return ""
}

type DASRecords []mongo.DASRecord

// helper function to process given set of URLs associted with dasquery
func processLocalApis(dasquery dasql.DASQuery, dmaps []mongo.DASRecord, pkeys []string) {
	// defer function will propagate panic message to higher level
	//     defer utils.ErrPropagate("processLocalApis")

	for _, dmap := range dmaps {
		urn := dasmaps.GetString(dmap, "urn")
		system := dasmaps.GetString(dmap, "system")
		expire := dasmaps.GetInt(dmap, "expire")
		api := fmt.Sprintf("L_%s_%s", system, urn)
		if utils.VERBOSE > 0 {
			log.Println("DAS local API", api)
		}
		// we use reflection to look-up api from our services/localapis.go functions
		// for details on reflection see
		// http://stackoverflow.com/questions/12127585/go-lookup-function-by-name
		t := reflect.ValueOf(services.LocalAPIs{})         // type of LocalAPIs struct
		m := t.MethodByName(api)                           // associative function name for given api
		args := []reflect.Value{reflect.ValueOf(dasquery)} // list of function arguments
		vals := m.Call(args)[0]                            // return value
		records := vals.Interface().([]mongo.DASRecord)    // cast reflect value to its type
		//         log.Println("### LOCAL APIS", urn, system, expire, dmap, api, m, len(records))

		records = services.AdjustRecords(dasquery, system, urn, records, expire, pkeys)

		// get DAS record and adjust its settings
		dasrecord := services.GetDASRecord(dasquery)
		dasstatus := fmt.Sprintf("process %s:%s", system, urn)
		dasexpire := services.GetExpire(dasrecord)
		if len(records) != 0 {
			rec := records[0]
			recexpire := services.GetExpire(rec)
			if dasexpire > recexpire {
				dasexpire = recexpire
			}
		}
		das := dasrecord["das"].(mongo.DASRecord)
		das["expire"] = dasexpire
		das["status"] = dasstatus
		dasrecord["das"] = das
		services.UpdateDASRecord(dasquery.Qhash, dasrecord)

		// fix all records expire values based on lowest one
		records = services.UpdateExpire(dasquery.Qhash, records, dasexpire)

		// insert records into DAS cache collection
		mongo.Insert("das", "cache", records)
	}
	// initial expire timestamp is 1h
	//     expire := utils.Expire(3600)
	expire := services.GetMinExpire(dasquery)
	// get DAS record and adjust its settings
	dasrecord := services.GetDASRecord(dasquery)
	dasexpire := services.GetExpire(dasrecord)
	if dasexpire < expire {
		dasexpire = expire
	}
	das := dasrecord["das"].(mongo.DASRecord)
	das["expire"] = dasexpire
	das["status"] = "ok"
	dasrecord["das"] = das
	services.UpdateDASRecord(dasquery.Qhash, dasrecord)
}

// helper function to process given set of URLs associted with dasquery
func processURLs(dasquery dasql.DASQuery, urls map[string]string, maps []mongo.DASRecord, dmaps dasmaps.DASMaps, pkeys []string) {
	// defer function will propagate panic message to higher level
	//     defer utils.ErrPropagate("processUrls")

	out := make(chan utils.ResponseType)
	umap := map[string]int{}
	for furl, args := range urls {
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(furl, args, out)
	}

	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			system := ""
			expire := 0
			urn := ""
			for _, dmap := range maps {
				surl := dasmaps.GetString(dmap, "url")
				// TMP fix, until we fix Phedex data to use JSON
				if strings.Contains(surl, "phedex") {
					surl = strings.Replace(surl, "xml", "json", -1)
				}
				// here we check that request Url match DAS map one either by splitting
				// base from parameters or making a match for REST based urls
				stm := dasmaps.GetString(dmap, "system")
				inst := dasquery.Instance
				if inst != "prod/global" && stm == "dbs3" {
					surl = strings.Replace(surl, "prod/global", inst, -1)
				}
				if strings.Split(r.Url, "?")[0] == surl || strings.HasPrefix(r.Url, surl) || r.Url == surl {
					urn = dasmaps.GetString(dmap, "urn")
					system = dasmaps.GetString(dmap, "system")
					expire = dasmaps.GetInt(dmap, "expire")
				}
			}
			// process data records
			notations := dmaps.FindNotations(system)
			records := services.Unmarshal(system, urn, r.Data, notations)
			records = services.AdjustRecords(dasquery, system, urn, records, expire, pkeys)

			// get DAS record and adjust its settings
			dasrecord := services.GetDASRecord(dasquery)
			dasstatus := fmt.Sprintf("process %s:%s", system, urn)
			dasexpire := services.GetExpire(dasrecord)
			if len(records) != 0 {
				rec := records[0]
				recexpire := services.GetExpire(rec)
				if dasexpire > recexpire {
					dasexpire = recexpire
				}
			}
			das := dasrecord["das"].(mongo.DASRecord)
			das["expire"] = dasexpire
			das["status"] = dasstatus
			dasrecord["das"] = das
			services.UpdateDASRecord(dasquery.Qhash, dasrecord)

			// fix all records expire values based on lowest one
			records = services.UpdateExpire(dasquery.Qhash, records, dasexpire)

			// insert records into DAS cache collection
			mongo.Insert("das", "cache", records)
			// remove from umap, indicate that we processed it
			delete(umap, r.Url) // remove Url from map
		default:
			if len(umap) == 0 { // no more requests, merge data records
				expire := services.GetMinExpire(dasquery)
				// get DAS record and adjust its settings
				dasrecord := services.GetDASRecord(dasquery)
				dasexpire := services.GetExpire(dasrecord)
				if dasexpire < expire {
					dasexpire = expire
				}
				das := dasrecord["das"].(mongo.DASRecord)
				das["expire"] = dasexpire
				das["status"] = "ok"
				dasrecord["das"] = das
				services.UpdateDASRecord(dasquery.Qhash, dasrecord)
				exit = true
			}
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if exit {
			break
		}
	}
	close(out) // we're done with channel
}

// Process DAS query
func Process(dasquery dasql.DASQuery, dmaps dasmaps.DASMaps) string {
	// defer function will propagate panic message to higher level
	//     defer utils.ErrPropagate("Process")

	// find out list of APIs/CMS services which can process this query request
	maps := dmaps.FindServices(dasquery.Fields, dasquery.Spec)
	var srvs, pkeys []string
	urls := make(map[string]string)
	var local_apis []mongo.DASRecord
	var furl string
	// loop over services and fetch data
	for _, dmap := range maps {
		args := ""
		system, _ := dmap["system"].(string)
		if system == "runregistry" {
			switch v := dasquery.Spec["run"].(type) {
			case string:
				args = fmt.Sprintf("{\"filter\": {\"number\": \">= %s and <= %s\"}}", v, v)
			case []string:
				args = fmt.Sprintf("{\"filter\": {\"number\": \">= %s and <= %s\"}}", v[0], v[len(v)-1])
			}
			furl, _ = dmap["url"].(string)
		} else if system == "reqmgr" || system == "mcm" {
			furl = formRESTUrl(dasquery, dmap)
		} else {
			furl = formUrlCall(dasquery, dmap)
		}
		if furl == "local_api" && !dasmaps.MapInList(dmap, local_apis) {
			local_apis = append(local_apis, dmap)
		} else if furl != "" {
			if _, ok := urls[furl]; !ok {
				urls[furl] = args
			}
		}
		srv := fmt.Sprintf("%s:%s", dmap["system"], dmap["urn"])
		srvs = append(srvs, srv)
		lkeys := strings.Split(dmap["lookup"].(string), ",")
		for _, pkey := range lkeys {
			for _, item := range dmap["das_map"].([]interface{}) {
				rec := item.(mongo.DASRecord)
				daskey := rec["das_key"].(string)
				reckey := rec["rec_key"].(string)
				if daskey == pkey {
					pkeys = append(pkeys, reckey)
					break
				}
			}
		}
	}

	if len(srvs) == 0 {
		panic("Unable to find any CMS service to serve your request")
	}
	dasrecord := services.CreateDASRecord(dasquery, srvs, pkeys)
	var records []mongo.DASRecord
	records = append(records, dasrecord)
	mongo.Insert("das", "cache", records)

	// process local_api calls, we use GoDeferFunc to run processLocalApis as goroutine in defer/silent mode
	// panic errors will be captured in GoDeferFunc and passed again into this local function
	if len(local_apis) > 0 {
		utils.GoDeferFunc("go processLocalApis", func() { processLocalApis(dasquery, local_apis, pkeys) })
	}
	// process URLs which will insert records into das cache and merge them into das merge collection
	if urls != nil {
		utils.GoDeferFunc("go processURLs", func() { processURLs(dasquery, urls, maps, dmaps, pkeys) })
	}

	// merge DAS cache records
	records, _ = services.MergeDASRecords(dasquery)
	mongo.Insert("das", "merge", records)

	return dasquery.Qhash
}

// helper function to modify spec with given filter
func modSpec(spec bson.M, filter string) {
	var key, val, op string
	var vals []string
	if strings.Index(filter, "<") > 0 {
		if strings.Index(filter, "<=") > 0 {
			vals = strings.Split(filter, "<=")
			op = "$le"
		} else {
			vals = strings.Split(filter, "<")
			op = "$lt"
		}
	} else if strings.Index(filter, "<") > 0 {
		if strings.Index(filter, ">=") > 0 {
			vals = strings.Split(filter, ">=")
			op = "$ge"
		} else {
			vals = strings.Split(filter, ">")
			op = "$gt"
		}
	} else if strings.Index(filter, "!=") > 0 {
		vals = strings.Split(filter, "!=")
		op = "$ne"
	} else if strings.Index(filter, "=") > 0 {
		vals = strings.Split(filter, "=")
		op = "$eq"
	} else {
		return
	}
	key = vals[0]
	val = vals[1]
	var cond bson.M
	if utils.IsInt(val) {
		ival, _ := strconv.Atoi(val)
		cond = bson.M{op: ival}
	} else {
		cond = bson.M{op: val}
	}
	spec[key] = cond
}

// Get data for given pid (DAS Query qhash)
func GetData(dasquery dasql.DASQuery, coll string, idx, limit int) (string, []mongo.DASRecord) {
	var empty_data, data []mongo.DASRecord
	pid := dasquery.Qhash
	filters := dasquery.Filters
	aggrs := dasquery.Aggregators
	if len(aggrs) > 0 { // if we need to aggregate we should ignore pagination
		idx = 0
		limit = -1
	}
	spec := bson.M{"qhash": pid}
	skeys := filters["sort"]
	if len(filters) > 0 {
		var afilters []string
		for key, vals := range filters {
			if key == "grep" {
				for _, val := range vals {
					if strings.Index(val, "<") > 0 || strings.Index(val, "<") > 0 || strings.Index(val, "!") > 0 || strings.Index(val, "=") > 0 {
						modSpec(spec, val)
					} else {
						afilters = append(afilters, val)
					}
				}
			}
		}
		if len(afilters) > 0 {
			data = mongo.GetFilteredSorted("das", coll, spec, afilters, skeys, idx, limit)
		} else {
			data = mongo.Get("das", coll, spec, idx, limit)
		}
	} else {
		data = mongo.Get("das", coll, spec, idx, limit)
	}
	if len(aggrs) > 0 {
		data = aggregateAll(data, aggrs)
	}
	// Get DAS status from cache collection
	spec = bson.M{"qhash": pid, "das.record": 0}
	das_data := mongo.Get("das", "cache", spec, 0, 1)
	status, err := mongo.GetStringValue(das_data[0], "das.status")
	if err != nil {
		return fmt.Sprintf("failed to get data from DAS cache: %s\n", err), empty_data
	}
	if len(data) == 0 {
		return status, empty_data
	}
	return status, data
}

// helper function to aggregate results over provided aggregators
// we'll use go routine to do this in parallel
func aggregateAll(data []mongo.DASRecord, aggrs [][]string) []mongo.DASRecord {
	var out []mongo.DASRecord
	ch := make(chan mongo.DASRecord)
	for _, agg := range aggrs {
		fagg := agg[0]
		fval := agg[1]
		go aggregate(data, fagg, fval, ch)
	}
	// collect results
	for {
		select {
		case r := <-ch:
			out = append(out, r)
		default:
			time.Sleep(time.Duration(10) * time.Millisecond) // wait for response
		}
		if len(out) == len(aggrs) {
			break
		}
	}
	close(ch)
	return out
}

// helper function to aggregate results for given function and key
func aggregate(data []mongo.DASRecord, agg, key string, ch chan mongo.DASRecord) {
	var values []interface{}
	for _, r := range data {
		val := mongo.GetValue(r, key)
		values = append(values, val)
	}
	var rec mongo.DASRecord
	switch agg {
	case "sum":
		rec = mongo.DASRecord{"result": mongo.DASRecord{"value": utils.Sum(values)}, "function": "sum", "key": key}
	case "min":
		rec = mongo.DASRecord{"result": mongo.DASRecord{"value": utils.Min(values)}, "function": "min", "key": key}
	case "max":
		rec = mongo.DASRecord{"result": mongo.DASRecord{"value": utils.Max(values)}, "function": "max", "key": key}
	case "mean":
		rec = mongo.DASRecord{"result": mongo.DASRecord{"value": utils.Mean(values)}, "function": "mean", "key": key}
	case "count":
		rec = mongo.DASRecord{"result": mongo.DASRecord{"value": len(values)}, "function": "count", "key": key}
	case "median":
		rec = mongo.DASRecord{"result": mongo.DASRecord{"value": utils.Median(values)}, "function": "median", "key": key}
	case "avg":
		rec = mongo.DASRecord{"result": mongo.DASRecord{"value": utils.Avg(values)}, "function": "avg", "key": key}
	default:
		rec = make(mongo.DASRecord)
	}
	rec["das"] = data[0]["das"]
	ch <- rec
}

// Get number of records for given DAS query qhash
func Count(pid string) int {
	spec := bson.M{"qhash": pid}
	return mongo.Count("das", "merge", spec)
}

// Get initial timestamp of DAS query request
func GetTimestamp(pid string) int64 {
	spec := bson.M{"qhash": pid, "das.record": 0}
	data := mongo.Get("das", "cache", spec, 0, 1)
	ts, err := mongo.GetInt64Value(data[0], "das.ts")
	if err != nil {
		return time.Now().Unix()
	}
	return ts
}

// Check if data exists in DAS cache for given query/pid
// we look-up DAS record (record=0) with status ok (merging step is done)
func CheckDataReadiness(pid string) bool {
	espec := bson.M{"$gt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec, "das.record": 0, "das.status": "ok"}
	nrec := mongo.Count("das", "cache", spec)
	if nrec == 1 {
		return true
	}
	return false
}

// Check if data exists in DAS cache for given query/pid
func CheckData(pid string) bool {
	espec := bson.M{"$gt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	nrec := mongo.Count("das", "cache", spec)
	if nrec > 0 {
		return true
	}
	return false
}

// Remove expired records
func RemoveExpired(pid string) {
	espec := bson.M{"$lt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	mongo.Remove("das", "cache", spec) // remove from cache collection
	mongo.Remove("das", "merge", spec) // remove from merge collection
}

package das

// Core system of DAS server
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"fmt"
	"log"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/services"
	"github.com/dmwm/das2go/utils"
	"gopkg.in/mgo.v2/bson"
)

// Record is a main entity DAS server operates
type Record map[string]interface{}

// DASRecord represent basic DAS record structure
type DASRecord struct {
	query  dasql.DASQuery
	record Record
	das    Record
}

// Qhash returns hash of DAS record
func (r *DASRecord) Qhash() string {
	return string(r.query.Qhash)
}

// Services returns list of services for DAS record
func (r *DASRecord) Services() []string {
	return []string{}
}

// Extract API call parameters from das map entry
func getApiParams(dasmap mongo.DASRecord) (string, string, string, string) {
	dasKey, ok := dasmap["das_key"].(string)
	if !ok {
		dasKey = ""
	}
	recKey, ok := dasmap["rec_key"].(string)
	if !ok {
		recKey = ""
	}
	apiArg, ok := dasmap["api_arg"].(string)
	if !ok {
		apiArg = ""
	}
	pattern, ok := dasmap["pattern"].(string)
	if !ok {
		pattern = ""
	}
	return dasKey, recKey, apiArg, pattern
}

// helper function to fix DBS instance in provided base string
func fixDBSinstance(dbsInst, base string) string {
	if strings.Contains(base, "http") && dbsInst != "" && len(dbsInst) > 0 && dbsInst != "prod/global" {
		// we only have prod, int, dev DBSes
		// all DAS DBS maps contain only URLs with global DBS instance
		// therefore we'll replace xxx/global to provided dbsInst
		defInstances := []string{"prod/global", "int/global", "dev/global"}
		for _, i := range defInstances {
			if strings.Contains(base, i) {
				base = strings.Replace(base, i, dbsInst, -1)
			}
		}
	}
	return base
}

// FormUrlCall forms appropriate URL from given dasquery and dasmap, the final URL
// contains all parameters
func FormUrlCall(dasquery dasql.DASQuery, dasmap mongo.DASRecord) string {

	// defer function profiler
	defer utils.MeasureTime("das/FormUrlCall")()

	vals := url.Values{}
	spec := dasquery.Spec
	skeys := utils.MapKeys(spec)
	base, ok := dasmap["url"].(string)
	system, _ := dasmap["system"].(string)
	// Adjust DBS URL wrt dbs instance name from query
	if system == "dbs" || system == "dbs3" {
		// adjust dbs instance in our base url
		base = fixDBSinstance(dasquery.Instance, base)
		// adjust APIs with 'run between' clause
		if utils.InList("run", skeys) {
			val := spec["run"]
			if strings.Contains(dasquery.Query, "between") {
				var minr, maxr, run string
				switch runs := val.(type) {
				case []string:
					minr = runs[0]
					maxr = runs[len(runs)-1]
					run = fmt.Sprintf("\"%s-%s\"", minr, maxr)
					vals.Add("run_num", run)
				case string:
					vals.Add("run_num", runs)
				}
			} else if strings.Contains(dasquery.Query, "in") && utils.InList("run", skeys) {
				switch runs := val.(type) {
				case []string:
					for _, r := range runs {
						vals.Add("run_num", r)
					}
				case string:
					vals.Add("run_num", runs)
				}
			}
		}
		// return only valid files by default
		if strings.Contains(base, "file") && !utils.InList("status", skeys) {
			// do not use valid files for filechildren/fileparents
			if !strings.Contains(base, "filechildren") && !strings.Contains(base, "fileparents") {
				if _, ok := vals["validFileOnly"]; !ok {
					vals.Add("validFileOnly", "1")
				}
			}
			// for files API when file is used as parameter we look-up file regardless of its validity
			fields := dasquery.Fields
			if len(skeys) == 1 && skeys[0] == "file" && len(fields) == 1 && fields[0] == "file" {
				vals.Del("validFileOnly")
			}
		}
	}
	if system == "phedex" {
		if v, ok := spec["site"]; ok {
			val := v.(string)
			if !strings.Contains(val, "*") {
				spec["site"] = fmt.Sprintf("%s*", val)
			}
		}
	}
	if system == "sitedb2" {
		// all sitedb apis is better to treat as local APIs, since
		// they don't really accept parameters. Instead, we'll use local
		// APIs to fetch all data and match records with given parameters
		return "local_api"
	}
	if system == "cric" {
		// all cric apis is better to treat as local APIs, since
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
		log.Println("Unable to extract url from DAS map", dasmap)
	}
	dasmaps := dasmaps.GetDASMaps(dasmap["das_map"])
	var useArgs []string
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
					} else if system == "dbs3" && arg == "validFileOnly" {
						v := strings.Replace(val, "*", "", -1)
						if strings.ToLower(v) == "valid" {
							delete(vals, "validFileOnly")
							vals.Add("validFileOnly", "1")
						} else {
							delete(vals, "validFileOnly")
							vals.Add("validFileOnly", "0")
						}
					} else if system == "dbs3" && arg == "status" {
						// This may need revision, probably better to properly
						// adjust DAS maps
						if strings.ToLower(val) == "valid" {
							delete(vals, "validFileOnly")
							vals.Add("validFileOnly", "1")
						} else {
							delete(vals, "validFileOnly")
							vals.Add("validFileOnly", "0")
						}
					} else if system == "dbs3" && arg == "min_cdate" {
						vals.Add(arg, fmt.Sprintf("%d", utils.UnixTime(val)))
						maxd := utils.UnixTime(val) + 24*60*60
						vals.Add("max_cdate", fmt.Sprintf("%d", maxd))
					} else if system == "dbs3" && arg == "cdate" {
						vals.Add("min_cdate", fmt.Sprintf("%d", utils.UnixTime(val)))
						maxd := utils.UnixTime(val) + 24*60*60
						vals.Add("max_cdate", fmt.Sprintf("%d", maxd))
					} else if dkey == "date" && system == "conddb" {
						vals.Add("startTime", utils.ConddbTime(val))
						eval := utils.Unix2DASTime(utils.UnixTime(val) + 37*3660)
						vals.Add("endTime", utils.ConddbTime(eval))
						useArgs = append(useArgs, arg)
					} else {
						if vvv, ok := vals[arg]; ok {
							if !utils.InList(val, vvv) {
								vals.Add(arg, val)
							}
						} else {
							vals.Add(arg, val)
						}
					}
					useArgs = append(useArgs, arg)
				}
			} else { // let's try array of strings
				arr, ok := spec[dkey].([]string)
				if system == "dbs3" && arg == "run_num" { // we already changed runs parameters above for DBS call
					continue
				}
				if !ok {
					fmt.Println("WARNING, unable to get value(s) for daskey=", dkey,
						", reckey=", rkey, " from spec=", spec, " das map=", dmap)
				}
				if dkey == "date" && system == "dbs3" {
					vals.Add("min_cdate", fmt.Sprintf("%d", utils.UnixTime(arr[0])))
					vals.Add("max_cdate", fmt.Sprintf("%d", utils.UnixTime(arr[1])))
					useArgs = append(useArgs, arg)
				} else if dkey == "date" && system == "dashboard" {
					vals.Add("date1", utils.DashboardTime(arr[0]))
					vals.Add("date2", utils.DashboardTime(arr[1]))
					useArgs = append(useArgs, arg)
				} else if dkey == "date" && system == "conddb" {
					vals.Add("startTime", utils.ConddbTime(arr[0]))
					vals.Add("endTime", utils.ConddbTime(arr[1]))
					useArgs = append(useArgs, arg)
				} else if system == "conddb" && arg == "Runs" {
					if len(arr) > 0 {
						vals.Add(arg, strings.Join(arr, ","))
						useArgs = append(useArgs, arg)
					}
				} else {
					for _, val := range arr {
						matched, _ := regexp.MatchString(pat, val)
						if matched || pat == "" {
							vals.Add(arg, val)
							useArgs = append(useArgs, arg)
						}
					}
				}
			}
		}
	}
	// loop over params in DAS maps and add additional arguments which have
	// non empty, non optional and non required values
	skipList := []string{"optional", "required"}
	params := mongo.Convert2DASRecord(dasmap["params"])
	for key, val := range params {
		switch v := val.(type) {
		case string:
			// speed-up query by NOT fetching details
			if system == "dbs3" && key == "detail" {
				if utils.WEBSERVER == 0 {
					if urn == "file4DatasetRunLumi" || urn == "files_via_block" {
						v = "False"
					}
				}
			}
			vvv := v
			if !utils.InList(key, useArgs) && !utils.InList(vvv, skipList) && vvv != "*" {
				if _, ok := vals[key]; !ok {
					vals.Add(key, vvv)
				}
			}
		case []interface{}:
			for _, value := range v {
				vvv := fmt.Sprintf("%s", value)
				if !utils.InList(key, useArgs) && !utils.InList(vvv, skipList) && vvv != "*" {
					if _, ok := vals[key]; !ok {
						vals.Add(key, vvv)
					}
				}
			}
		}
	}

	// TMP: exception, DAS maps use jobsummary-plot-or-table dashboard api, while we need
	// jobsummary-plot-or-table2 which returns JSON
	if strings.HasSuffix(base, "jobsummary-plot-or-table") {
		base += "2" // add 2 at the end
	}

	// adjust datasets API to look-up all datasets regardless of their status
	// if dataset name is provided
	if system == "dbs3" && urn == "datasets" {
		val, ok := spec["dataset"].(string)
		if ok && !strings.Contains(val, "*") {
			if _, ok := spec["status"]; !ok { // only if user didn't specified a status
				delete(vals, "dataset_access_type")
				vals.Add("dataset_access_type", "*")
			}
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

// FormRESTUrl forms appropriate URL from given dasquery and dasmap, the final URL
// contains all parameters
func FormRESTUrl(dasquery dasql.DASQuery, dasmap mongo.DASRecord) string {

	// defer function profiler
	defer utils.MeasureTime("das/FormRESTUrl")()

	spec := dasquery.Spec
	skeys := utils.MapKeys(spec)
	base, ok := dasmap["url"].(string)
	if !ok {
		log.Println("Unable to extract url from DAS map", dasmap)
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
			switch spec[dkey].(type) {
			case string:
				val, _ := spec[dkey].(string)
				matched, _ := regexp.MatchString(pat, val)
				if matched || pat == "" {
					if strings.HasPrefix(val, "/") {
						if strings.HasSuffix(base, "/") {
							return base[0:len(base)-1] + val
						}
						return base + val
					}
					if strings.HasSuffix(base, "/") {
						return base + val
					}
					return base + "/" + val
				}
			case []string:
				val, _ := spec[dkey].([]string)
				matched, _ := regexp.MatchString(pat, val[0])
				if matched || pat == "" {
					return base
				}
			default:
				log.Printf("ERROR: invalid type for DAS key, type %T, key %v, map %v\n", spec[dkey], dkey, dmap)
				return ""
			}
		}
	}
	return ""
}

// DASRecords holds list of DAS records
type DASRecords []mongo.DASRecord

// helper function to process given set of URLs associted with dasquery
func processLocalApis(dasquery dasql.DASQuery, dmaps []mongo.DASRecord, pkeys []string) {
	if utils.WEBSERVER > 0 && utils.VERBOSE > 0 {
		log.Println("processLocalApis", dmaps)
	}
	// defer function will propagate error message to higher level
	//     defer utils.ErrPropagate("processLocalApis")

	// defer function profiler
	defer utils.MeasureTime("das/processLocalApis")()

	localApiMap := services.LocalAPIMap()
	for _, dmap := range dmaps {
		urn := dasmaps.GetString(dmap, "urn")
		system := dasmaps.GetString(dmap, "system")
		expire := dasmaps.GetInt(dmap, "expire")
		api := fmt.Sprintf("%s_%s", system, urn)
		apiFunc := localApiMap[api]
		if utils.VERBOSE > 0 {
			log.Printf("DAS look-up: api %s, func %s\n", api, apiFunc)
		}
		// we use reflection to look-up api from our services/localapis.go functions
		// for details on reflection see
		// http://stackoverflow.com/questions/12127585/go-lookup-function-by-name
		t := reflect.ValueOf(services.LocalAPIs{})         // type of LocalAPIs struct
		m := t.MethodByName(apiFunc)                       // associative function name for given api
		args := []reflect.Value{reflect.ValueOf(dasquery)} // list of function arguments
		vals := m.Call(args)[0]                            // return value
		records := vals.Interface().([]mongo.DASRecord)    // cast reflect value to its type
		if utils.VERBOSE > 1 {
			log.Printf("local apis, urn %v, system %v, expire %v, dmap %v, api %v, func %v, method %v, records %v\n", urn, system, expire, dmap, api, apiFunc, m, len(records))
		}

		records = services.AdjustRecords(dasquery, system, urn, records, expire, pkeys)

		// get DAS record and adjust its settings
		dasrecord := services.GetDASRecord(dasquery)
		dasstatus := fmt.Sprintf("process %s:%s", system, urn)
		dasexpire := services.GetExpire(dasrecord)
		if len(records) != 0 {
			rec := records[0]
			recexpire := services.GetExpire(rec)
			if dasexpire < recexpire {
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
	if utils.WEBSERVER > 0 && utils.VERBOSE > 0 {
		log.Println("processURLs", urls)
	}
	// defer function will propagate error message to higher level
	//     defer utils.ErrPropagate("processUrls")

	// defer function profiler
	defer utils.MeasureTime("das/processURLs")()

	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	client := utils.HttpClient()
	for furl, args := range urls {
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(client, furl, args, out)
	}

	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			log.Printf("pid=%s %s\n", dasquery.Qhash, r.Details())
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
				if stm == "dbs3" {
					surl = fixDBSinstance(dasquery.Instance, surl)
				}
				if strings.Split(r.Url, "?")[0] == surl || strings.HasPrefix(r.Url, surl) || r.Url == surl {
					urn = dasmaps.GetString(dmap, "urn")
					system = dasmaps.GetString(dmap, "system")
					expire = dasmaps.GetInt(dmap, "expire")
				}
			}
			// process data records
			notations := dmaps.FindNotations(system)
			records := services.Unmarshal(dasquery, system, urn, r, notations, pkeys)
			records = services.AdjustRecords(dasquery, system, urn, records, expire, pkeys)

			// get DAS record and adjust its settings
			dasrecord := services.GetDASRecord(dasquery)
			dasstatus := fmt.Sprintf("process %s:%s", system, urn)
			dasexpire := services.GetExpire(dasrecord)
			if len(records) != 0 {
				rec := records[0]
				recexpire := services.GetExpire(rec)
				if dasexpire < recexpire {
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
}

// ProcessLogic represents common logic for Process API shared both
// in das2go and dasgoclient codebase. It figures out which services
// pkeys, urls and localApis to use for given dasquery, das maps and selected Services
// The selectedServices is only used in dasgoclient to speed up the process.
func ProcessLogic(dasquery dasql.DASQuery, maps []mongo.DASRecord, selectedServices []string) ([]string, []string, map[string]string, []mongo.DASRecord) {

	// defer function profiler
	defer utils.MeasureTime("das/ProcessLogic")()

	var srvs, pkeys []string
	urls := make(map[string]string)
	var localApis []mongo.DASRecord
	var furl string
	// loop over services and fetch data
	for _, dmap := range maps {
		args := ""
		system, _ := dmap["system"].(string)
		// for das2go we'll use empty selectedServices while for dasgoclient we'll pay attention here
		if len(selectedServices) > 0 && !utils.InList(system, selectedServices) {
			continue
		}
		if system == "runregistry" {
			switch v := dasquery.Spec["run"].(type) {
			case string:
				args = fmt.Sprintf("{\"filter\": {\"number\": \">= %s and <= %s\"}}", v, v)
			case []string:
				cond := fmt.Sprintf("= %s", v[0])
				for i, vvv := range v {
					if i > 0 {
						cond = fmt.Sprintf("%s or = %s", cond, vvv)
					}
				}
				args = fmt.Sprintf("{\"filter\": {\"number\": \"%s\"}}", cond)
			}
			switch v := dasquery.Spec["date"].(type) {
			case string:
				t := utils.RunRegistryTime(v)
				n := utils.RunRegistryTime(utils.Unix2DASTime(utils.UnixTime(v) + 25*60*60))
				args = fmt.Sprintf("{\"filter\": {\"startTime\": \">= %s and < %s\"}}", t, n)
			case []string:
				cond := fmt.Sprintf(">= %s and <= %s", utils.RunRegistryTime(v[0]), utils.RunRegistryTime(v[len(v)-1]))
				args = fmt.Sprintf("{\"filter\": {\"startTime\": \"%s\"}}", cond)
			}
			furl, _ = dmap["url"].(string)
			// Adjust url to use custom columns
			columns := "number%2CstartTime%2CstopTime%2Ctriggers%2CrunClassName%2CrunStopReason%2Cbfield%2CgtKey%2Cl1Menu%2ChltKeyDescription%2ClhcFill%2ClhcEnergy%2CrunCreated%2Cmodified%2ClsCount%2ClsRanges"
			if furl[len(furl)-1:] == "/" { // look-up last slash
				furl = fmt.Sprintf("%sapi/GLOBAL/runsummary/json/%s/none/data", furl, columns)
			} else {
				furl = fmt.Sprintf("%s/api/GLOBAL/runsummary/json/%s/none/data", furl, columns)
			}
		} else if system == "reqmgr" || system == "mcm" || system == "rucio" {
			if system == "rucio" {
				urn, _ := dmap["urn"].(string)
				site, ok := dasquery.Spec["site"]
				if ok && urn == "file4dataset_site" {
					// remove site from site since it should not go to REST URL
					delete(dasquery.Spec, "site")
				}
				furl = FormRESTUrl(dasquery, dmap)
				if ok && urn == "file4dataset_site" { // put back site condition into dasquery spec
					dasquery.Spec["site"] = site
				}
				if urn == "block4dataset_size" {
					// add datasets after url which will return CMS blocks (Rucio datasets)
					furl = fmt.Sprintf("%s/datasets/", furl)
				}
				if urn == "rses" {
					// cut off site parameter from REST URL since no site condition is supported yet
					arr := strings.Split(furl, "/rses/")
					furl = fmt.Sprintf("%s/rses/", arr[0])
				}
				if urn == "block4dataset" {
					furl = fmt.Sprintf("%s/dids", furl)
					if strings.Contains(furl, "#") {
						furl = strings.Replace(furl, "#", "%23", -1)
					}
				}
				if urn == "rules4dataset" || urn == "rules4block" || urn == "rules4file" {
					// adjust rest URL
					furl = fmt.Sprintf("%s/rules", furl)
					if strings.Contains(furl, "#") {
						furl = strings.Replace(furl, "#", "%23", -1)
					}
				}
			} else {
				furl = FormRESTUrl(dasquery, dmap)
			}
		} else {
			furl = FormUrlCall(dasquery, dmap)
		}
		if furl == "local_api" && !dasmaps.MapInList(dmap, localApis) {
			localApis = append(localApis, dmap)
		} else if furl != "" {
			// adjust conddb URL, remove Runs= empty parater since it leads to an error
			if strings.Contains(furl, "Runs=&") {
				furl = strings.Replace(furl, "Runs=&", "", -1)
			}
			if _, ok := urls[furl]; !ok {
				urls[furl] = args
			}
		}

		srv := fmt.Sprintf("%s:%s", dmap["system"], dmap["urn"])
		srvs = append(srvs, srv)
		lkeys := strings.Split(dmap["lookup"].(string), ",")
		for _, pkey := range lkeys {
			for _, item := range dmap["das_map"].([]interface{}) {
				rec := mongo.Convert2DASRecord(item)
				daskey := rec["das_key"].(string)
				reckey := rec["rec_key"].(string)
				if daskey == pkey {
					pkeys = append(pkeys, reckey)
					break
				}
			}
		}
	}
	return srvs, pkeys, urls, localApis
}

// Process takes care of processing given DAS query
func Process(dasquery dasql.DASQuery, dmaps dasmaps.DASMaps) {
	// defer function will propagate error message to higher level
	//     defer utils.ErrPropagate("Process")

	// defer function profiler
	defer utils.MeasureTime("das/Process")()

	// find out list of APIs/CMS services which can process this query request
	maps := dmaps.FindServices(dasquery)

	// get list of services, pkeys, urls and localApis we need to process
	// but for das2go we don't need to use selectedServices, here we'll pass empty list
	var selectedServices []string
	srvs, pkeys, urls, localApis := ProcessLogic(dasquery, maps, selectedServices)

	if utils.WEBSERVER > 0 && utils.VERBOSE > 0 {
		log.Println("ProcessLogic, services", srvs, "pkeys", pkeys, "urls", urls, "localApis", localApis)
	}

	if len(srvs) == 0 {
		if utils.WEBSERVER > 0 {
			log.Printf("unable to find any CMS service to fullfil this request, query: %s\n", dasquery.String())
		} else {
			fmt.Println("DAS WARNING", dasquery, "unable to find any CMS service to fullfil this request")
		}
		dasrecord := services.CreateDASErrorRecord(dasquery, pkeys)
		var records []mongo.DASRecord
		records = append(records, dasrecord)
		mongo.Insert("das", "cache", records)
		mongo.Insert("das", "merge", records)
		return
	}
	dasrecord := services.CreateDASRecord(dasquery, srvs, pkeys)
	if utils.VERBOSE > 0 {
		log.Printf("services.CreateDASRecord, record %v, services %v, pkeys %v\n", dasrecord, srvs, pkeys)
	}
	var records []mongo.DASRecord
	records = append(records, dasrecord)
	mongo.Insert("das", "cache", records)

	// process local_api calls, we use GoDeferFunc to run processLocalApis as goroutine in defer/silent mode
	// errors will be captured in GoDeferFunc and passed again into this local function
	if len(localApis) > 0 {
		utils.GoDeferFunc("go processLocalApis", func() { processLocalApis(dasquery, localApis, pkeys) })
	}
	// process URLs which will insert records into das cache and merge them into das merge collection
	if urls != nil {
		utils.GoDeferFunc("go processURLs", func() { processURLs(dasquery, urls, maps, dmaps, pkeys) })
	}

	// merge DAS cache records
	records, _ = services.MergeDASRecords(dasquery)
	mongo.Insert("das", "merge", records)

	// insert das.record=0 into DAS Merge collection to indicate that we done with request
	spec := bson.M{"das.record": 0, "qhash": dasquery.Qhash}
	recs := mongo.Get("das", "cache", spec, 0, 1)
	mongo.Insert("das", "merge", recs)
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

// GetData for given pid (DAS Query qhash)
func GetData(dasquery dasql.DASQuery, coll string, idx, limit int) (string, []mongo.DASRecord) {

	// defer function profiler
	defer utils.MeasureTime("das/GetData")()

	var emptyData, data []mongo.DASRecord
	pid := dasquery.Qhash
	filters := dasquery.Filters
	aggrs := dasquery.Aggregators
	if len(aggrs) > 0 { // if we need to aggregate we should ignore pagination
		idx = 0
		limit = -1
	}
	spec := bson.M{"qhash": pid, "das.record": 1}
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

	// perform post-processing of DAS records
	//     data = PostProcessing(dasquery, data)

	// Get DAS status from merge collection
	spec = bson.M{"qhash": pid, "das.record": 0}
	dasData := mongo.Get("das", "merge", spec, 0, 1)
	if len(dasData) == 0 {
		return fmt.Sprintf("ERROR no DAS record found in das.merge collection\n"), emptyData
	}
	status, err := mongo.GetStringValue(dasData[0], "das.status")
	if err != nil {
		return fmt.Sprintf("ERROR failed to get data from DAS cache: %s\n", err), emptyData
	}
	if len(data) == 0 {
		return status, emptyData
	}
	return status, data
}

// helper function to perform post-processing of DAS data, e.g.
// when we call site query we need to distinguish the case when
// to show original site
func PostProcessing(dasquery dasql.DASQuery, data []mongo.DASRecord) []mongo.DASRecord {

	// defer function profiler
	defer utils.MeasureTime("das/PostProcessing")()

	// site4dataset use case
	fields := dasquery.Fields
	if utils.InList("site", fields) {
		var out []mongo.DASRecord
		for _, r := range data {
			var das mongo.DASRecord
			switch v := r["das"].(type) {
			case interface{}:
				das = v.(mongo.DASRecord)
			case mongo.DASRecord:
				das = v
			}
			var srvs []string
			switch v := das["services"].(type) {
			case []interface{}:
				for _, v := range v {
					srvs = append(srvs, v.(string))
				}
			case []string:
				srvs = v
			}
			orig := false // original placement
			if len(srvs) == 1 {
				var recs []mongo.DASRecord
				switch v := r["site"].(type) {
				case []interface{}:
					for _, v := range v {
						recs = append(recs, v.(mongo.DASRecord))
					}
				case []mongo.DASRecord:
					recs = v
				}
				for _, s := range recs {
					k, ok := s["kind"]
					if ok && k.(string) == "original placement" {
						orig = true
					}
				}
			}
			if orig == false {
				out = append(out, r)
			}
		}
		if len(out) > 0 && len(out) != len(data) {
			return out
		}
	}
	return data
}

// helper function to aggregate results over provided aggregators
// we'll use go routine to do this in parallel
func aggregateAll(data []mongo.DASRecord, aggrs [][]string) []mongo.DASRecord {

	// defer function profiler
	defer utils.MeasureTime("das/aggregateAll")()

	var out []mongo.DASRecord
	ch := make(chan mongo.DASRecord)
	defer close(ch)
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
	return out
}

// helper function to aggregate results for given function and key and yield them to channel
func aggregate(data []mongo.DASRecord, agg, key string, ch chan mongo.DASRecord) {
	ch <- Aggregate(data, agg, key)
}

// Aggregate function aggregates results for given function and key
func Aggregate(data []mongo.DASRecord, agg, key string) mongo.DASRecord {
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
	if len(data) > 0 {
		rec["das"] = data[0]["das"]
	} else {
		rec["das"] = "Unable to aggregate"
	}
	return rec
}

// Count gets number of records for given DAS query qhash
func Count(pid string) int {
	spec := bson.M{"qhash": pid, "das.record": 1}
	return mongo.Count("das", "merge", spec)
}

// Bytes gets size of records for given DAS query
func Bytes(pid string) int {
	spec := bson.M{"qhash": pid, "das.record": 1}
	return mongo.Bytes("das", "merge", spec)
}

// GetTimestamp gets initial timestamp of DAS query request
func GetTimestamp(pid string) int64 {
	spec := bson.M{"qhash": pid, "das.record": 0}
	data := mongo.Get("das", "cache", spec, 0, 1)
	ts, err := mongo.GetInt64Value(data[0], "das.ts")
	if err != nil {
		return time.Now().Unix()
	}
	return ts
}

// CheckDataReadiness checks if data exists in DAS cache for given query/pid
// we look-up DAS record (record=0) with status ok (merging step is done)
func CheckDataReadiness(pid string) bool {
	espec := bson.M{"$gt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec, "das.record": 0, "das.status": "ok"}
	nrec := mongo.Count("das", "merge", spec)
	if nrec == 1 {
		return true
	}
	return false
}

// CheckData checks if data exists in DAS cache for given query/pid
func CheckData(pid string) bool {
	espec := bson.M{"$gt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	nrec := mongo.Count("das", "cache", spec)
	if nrec > 0 {
		return true
	}
	return false
}

// RemoveExpired remove expired records
func RemoveExpired(pid string) {
	espec := bson.M{"$lt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	mongo.Remove("das", "cache", spec) // remove from cache collection
	mongo.Remove("das", "merge", spec) // remove from merge collection
}

// TimeStamp returns list of DAS queries which are currently processing by the server
func TimeStamp(dasquery dasql.DASQuery) int64 {
	spec := bson.M{"das.record": 0, "qhash": dasquery.Qhash}
	recs := mongo.Get("das", "cache", spec, 0, 1)
	if len(recs) == 0 {
		log.Printf("ERROR: unable to find das record, query: %v, spec %v\n", dasquery.String, spec)
		return 0
	}
	ts, err := mongo.GetInt64Value(recs[0], "das.ts")
	if err != nil {
		log.Printf("ERROR: unable to find das record, query: %v, spec %v\n", dasquery.String, spec)
		return 0
	}
	return ts
}

// ProcessingQueries returns list of DAS queries which are currently processing by the server
func ProcessingQueries() []string {
	var out []string
	spec := bson.M{"das.record": 0, "das.status": "processing"}
	for _, r := range mongo.Get("das", "cache", spec, 0, 0) {
		q := r["query"].(string)
		out = append(out, q)
	}
	spec = bson.M{"das.record": 0, "das.status": "requested"}
	for _, r := range mongo.Get("das", "cache", spec, 0, 0) {
		q := r["query"].(string)
		out = append(out, q)
	}
	return utils.List2Set(out)
}

/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS core module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 */
package das

import (
	"dasmaps"
	"dasql"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
	"mongo"
	"net/url"
	"regexp"
	"services"
	"strings"
	"time"
	"utils"
)

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
	// TMP, until we change phedex maps to use JSON
	if strings.Contains(base, "phedex") {
		base = strings.Replace(base, "xml", "json", -1)
	}
	if !ok {
		log.Fatal("Unable to extract url from DAS map", dasmap)
	}
	dasmaps := dasmaps.GetDASMaps(dasmap["das_map"])
	vals := url.Values{}
	for _, dmap := range dasmaps {
		dkey, rkey, arg, pat := getApiParams(dmap)
		if utils.InList(dkey, skeys) {
			val, ok := spec[dkey].(string)
			if !ok {
				log.Fatal("Unable to get value for daskey=", dkey, ", reckey=", rkey, " from record=", dmap)
			}
			matched, _ := regexp.MatchString(pat, val)
			if matched {
				vals.Add(arg, val)
			}
		}
	}
	args := vals.Encode()
	if len(args) > 1 {
		return base + "?" + args
	}
	return base
}

// helper function to process given set of URLs associted with dasquery
func processURLs(dasquery dasql.DASQuery, urls []string, maps []mongo.DASRecord, dmaps dasmaps.DASMaps, pkeys []string) {
	out := make(chan utils.ResponseType)
	umap := map[string]int{}
	rmax := 3 // maximum number of retries
	for _, furl := range urls {
		umap[furl] = 0 // number of retries per url
		go utils.Fetch(furl, out)
	}

	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			if r.Error != nil {
				retry := umap[r.Url]
				if retry < rmax {
					retry += 1
					// incremenet sleep duration with every retry
					sleep := time.Duration(retry) * time.Second
					time.Sleep(sleep)
					umap[r.Url] = retry
				} else {
					delete(umap, r.Url) // remove Url from map
				}
			} else {
				system := ""
				//                 format := ""
				expire := 0
				urn := ""
				for _, dmap := range maps {
					surl := dasmaps.GetString(dmap, "url")
					// TMP fix, until we fix Phedex data to use JSON
					if strings.Contains(surl, "phedex") {
						surl = strings.Replace(surl, "xml", "json", -1)
					}
					if strings.Split(r.Url, "?")[0] == surl {
						urn = dasmaps.GetString(dmap, "urn")
						system = dasmaps.GetString(dmap, "system")
						expire = dasmaps.GetInt(dmap, "expire")
						//                         format = dasmaps.GetString(dmap, "format")
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

				// insert records into DAS cache collection
				mongo.Insert("das", "cache", records)
				// remove from umap, indicate that we processed it
				delete(umap, r.Url) // remove Url from map
			}
		default:
			if len(umap) == 0 { // no more requests, merge data records
				records, expire := services.MergeDASRecords(dasquery)
				mongo.Insert("das", "merge", records)
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

// Process DAS query
func Process(dasquery dasql.DASQuery, dmaps dasmaps.DASMaps) string {
	// find out list of APIs/CMS services which can process this query request
	maps := dmaps.FindServices(dasquery.Fields, dasquery.Spec)
	var urls, srvs, pkeys []string
	// loop over services and fetch data
	for _, dmap := range maps {
		furl := formUrlCall(dasquery, dmap)
		urls = append(urls, furl)
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

	dasrecord := services.CreateDASRecord(dasquery, srvs, pkeys)
	var records []mongo.DASRecord
	records = append(records, dasrecord)
	mongo.Insert("das", "cache", records)

	// process URLs which will insert records into das cache and merge them into das merge collection
	go processURLs(dasquery, urls, maps, dmaps, pkeys)
	return dasquery.Qhash
}

// Get data for given pid (DAS Query qhash)
func GetData(pid, coll string, idx, limit int) (string, []mongo.DASRecord) {
	var empty_data []mongo.DASRecord
	spec := bson.M{"qhash": pid}
	data := mongo.Get("das", coll, spec, idx, limit)
	if len(data) == 0 {
		return fmt.Sprintf("No data in DAS cache"), empty_data
	}
	status, err := mongo.GetStringValue(data[0], "das.status")
	if err != nil {
		return fmt.Sprintf("failed to get data from DAS cache: %s\n", err), empty_data
	}
	return status, data
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

// Represent DAS records for web UI
func PresentData(dasquery dasql.DASQuery, data []mongo.DASRecord, pmap mongo.DASRecord) []string {
	var out []string
	line := "<hr/>\n"
	br := "<br/>\n"
	fields := dasquery.Fields
	for _, item := range data {
		//         das := item["das"].(mongo.DASRecord)
		for _, key := range fields {
			records := item[key].([]interface{})
			uiRows := pmap[key].([]interface{})
			for _, elem := range records {
				rec := elem.(mongo.DASRecord)
				var values []string
				for _, uir := range uiRows {
					uirow := uir.(mongo.DASRecord)
					daskey := uirow["das"].(string)
					webkey := uirow["ui"].(string)
					attrs := strings.Split(daskey, ".")
					attr := strings.Join(attrs[1:len(attrs)], ".")
					value := ExtractValue(rec, attr)
					if len(value) > 0 {
						row := fmt.Sprintf("%s: %v\n", webkey, value)
						values = append(values, row)
					}
				}
				out = append(out, strings.Join(values, ","))
			}
			out = append(out, br)
		}
		out = append(out, line)
	}
	return out
}

// helper function to extract value from das record
// relies on type switching, see
// https://golang.org/doc/effective_go.html#type_switch
func ExtractValue(data mongo.DASRecord, daskey string) string {
	var out []string
	keys := strings.Split(daskey, ".")
	count := 1
	for _, key := range keys {
		value := data[key]
		if value == nil {
			return ""
		}
		switch value := value.(type) {
		case string:
			out = append(out, value)
		case int:
			out = append(out, fmt.Sprintf("%d", value))
		case int64:
			out = append(out, fmt.Sprintf("%d", value))
		case float64:
			if key == "size" {
				out = append(out, utils.SizeFormat(value))
			} else {
				out = append(out, fmt.Sprintf("%v", value))
			}
		case []interface{}:
			for _, rec := range value {
				value := ExtractValue(rec.(mongo.DASRecord), strings.Join(keys[count:len(keys)], "."))
				out = append(out, fmt.Sprintf("%v", value))
			}
			break
		default:
			if count != len(keys) {
				return ExtractValue(value.(mongo.DASRecord), strings.Join(keys[count:len(keys)], "."))
			}
			out = append(out, fmt.Sprintf("%v", value))
		}
		count = count + 1
	}
	return strings.Join(out, ",")
}

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
	"labix.org/v2/mgo/bson"
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
	uri, dbname := utils.ParseConfig()
	out := make(chan utils.ResponseType)
	umap := map[string]int{}
	rmax := 3 // maximum number of retries
	for _, furl := range urls {
		log.Println("Call", furl)
		umap[furl] = 0 // number of retries per url
		go utils.Fetch(furl, out)
		// transform data into JSON
		log.Println("Transform record")
		// insert record into MongoDB
		log.Println("Insert record into MongoDB")
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
				dasrecord := services.GetDASRecord(uri, dbname, "cache", dasquery)
				dasstatus := fmt.Sprintf("process %s:%s", system, urn)
				dasexpire := services.GetExpire(dasrecord)
				rec := records[0]
				recexpire := services.GetExpire(rec)
				if dasexpire < recexpire {
					dasexpire = recexpire
				}
				das := dasrecord["das"].(mongo.DASRecord)
				das["expire"] = dasexpire
				das["status"] = dasstatus
				dasrecord["das"] = das
				services.UpdateDASRecord(uri, dbname, "cache", dasquery.Qhash, dasrecord)

				// insert records into DAS cache collection
				mongo.Insert(uri, dbname, "cache", records)
				// remove from umap, indicate that we processed it
				delete(umap, r.Url) // remove Url from map
			}
		default:
			if len(umap) == 0 { // no more requests, merge data records
				log.Println("Merge DAS data records from DAS cache into DAS merge collection")
				records, expire := services.MergeDASRecords(dasquery)
				log.Println("Merge records", len(records), expire)
				mongo.Insert(uri, dbname, "merge", records)
				// get DAS record and adjust its settings
				dasrecord := services.GetDASRecord(uri, dbname, "cache", dasquery)
				dasexpire := services.GetExpire(dasrecord)
				if dasexpire < expire {
					dasexpire = expire
				}
				das := dasrecord["das"].(mongo.DASRecord)
				das["expire"] = dasexpire
				das["status"] = "ok"
				dasrecord["das"] = das
				services.UpdateDASRecord(uri, dbname, "cache", dasquery.Qhash, dasrecord)
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
	uri, dbname := utils.ParseConfig()
	mongo.Insert(uri, dbname, "cache", records)

	// process URLs which will insert records into das cache and merge them into das merge collection
	go processURLs(dasquery, urls, maps, dmaps, pkeys)
	return dasquery.Qhash
}

// Get data for given pid (DAS Query qhash)
func GetData(pid, coll string) (string, []mongo.DASRecord) {
	uri, dbname := utils.ParseConfig()
	spec := bson.M{"qhash": pid}
	data := mongo.Get(uri, dbname, coll, spec)
	status, err := mongo.GetStringValue(data[0], "das.status")
	if err != nil {
		var data []mongo.DASRecord
		return fmt.Sprintf("failed to get data from DAS cache: %s\n", err), data
	}
	return status, data
}

// Check if data exists in DAS cache for given query/pid
// we look-up DAS record (record=0) with status ok (merging step is done)
func CheckDataReadiness(pid string) bool {
	uri, dbname := utils.ParseConfig()
	espec := bson.M{"$gt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec, "das.record": 0, "das.status": "ok"}
	nrec := mongo.Count(uri, dbname, "cache", spec)
	if nrec == 1 {
		return true
	}
	return false
}

// Check if data exists in DAS cache for given query/pid
func CheckData(pid string) bool {
	uri, dbname := utils.ParseConfig()
	espec := bson.M{"$gt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	nrec := mongo.Count(uri, dbname, "cache", spec)
	if nrec > 0 {
		return true
	}
	return false
}

// Remove expired records
func RemoveExpired(pid string) {
	uri, dbname := utils.ParseConfig()
	espec := bson.M{"$lt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	mongo.Remove(uri, dbname, "cache", spec) // remove from cache collection
	mongo.Remove(uri, dbname, "merge", spec) // remove from merge collection
}

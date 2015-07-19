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

// Helper function to parse DAS configuration file and return
// MongoDB uri, dbname, collection name
func parseConfig() (string, string, string) {
	uri := "mongodb://localhost:8230"
	dbname := "das"
	coll := "cache"
	return uri, dbname, coll
}

// helper function to process given set of URLs associted with dasquery
func processURLs(dasquery dasql.DASQuery, urls []string, maps []mongo.DASRecord, dmaps dasmaps.DASMaps) {
	uri, dbname, coll := parseConfig()
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
				// TODO: replace with parsing and writing to mongo
				notations := dmaps.FindNotations(system)
				records := services.Unmarshal(system, urn, r.Data, notations)
				records = services.AdjustRecords(dasquery, system, urn, records, expire)
				log.Println("#### Unmarshalled data", system, urn, records)
				// insert records into MongoDB
				mongo.Insert(uri, dbname, coll, records)
				// remove from umap, indicate that we processed it
				delete(umap, r.Url) // remove Url from map
			}
		default:
			if len(umap) == 0 { // no more requests
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
func Process(dasquery dasql.DASQuery, dmaps dasmaps.DASMaps) (bool, string) {
	status := true
	// parse input query and convert it into DASQuery format
	//     dasquery := dasql.Parse(query)
	//     log.Printf("Process %s\n", dasquery)
	// find out list of APIs/CMS services which can process this query request
	maps := dmaps.FindServices(dasquery.Fields, dasquery.Spec)
	var urls []string
	// loop over services and fetch data
	for _, dmap := range maps {
		furl := formUrlCall(dasquery, dmap)
		urls = append(urls, furl)
	}
	// TODO: this should be sent as goroutine
	go processURLs(dasquery, urls, maps, dmaps)
	// perform merge step
	log.Println("Merge DAS data records from DAS cache into DAS merge collection")
	return status, dasquery.Qhash
}

// Get data for given pid (DAS Query qhash)
func GetData(pid string) (bool, []mongo.DASRecord) {
	uri, dbname, coll := parseConfig()
	spec := bson.M{"qhash": pid}
	data := mongo.Get(uri, dbname, coll, spec)
	status := true
	return status, data
}

// Check if data exists in DAS cache for given query/pid
func CheckData(pid string) bool {
	uri, dbname, coll := parseConfig()
	espec := bson.M{"$gt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	nrec := mongo.Count(uri, dbname, coll, spec)
	if nrec > 0 {
		return true
	}
	return false
}

// Remove expired records
func RemoveExpired(pid string) {
	uri, dbname, coll := parseConfig()
	espec := bson.M{"$lt": time.Now().Unix()}
	spec := bson.M{"qhash": pid, "das.expire": espec}
	log.Println("### RemoveExpired", spec)
	mongo.Remove(uri, dbname, coll, spec)
}

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
	"log"
	"mongo"
	"net/url"
	"regexp"
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
			log.Println("Matching String", pat, val, matched)
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

// Process DAS query
func Process(query string, dasmaps dasmaps.DASMaps) (bool, string) {
	status := true
	// parse input query and convert it into DASQuery format
	dasquery := dasql.Parse(query)
	// find out list of APIs/CMS services which can process this query request
	maps := dasmaps.FindServices(dasquery.Fields, dasquery.Spec)
	// loop over services and fetch data
	for _, dmap := range maps {
		url := formUrlCall(dasquery, dmap)
		log.Println("Call", url)
		// transform data into JSON
		log.Println("Transform record")
		// insert record into MongoDB
		log.Println("Insert record into MongoDB")
	}
	// perform merge step
	log.Println("Merge DAS data records from DAS cache into DAS merge collection")
	return status, dasquery.Qhash
}

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

func processURLs(urls []string, maps []mongo.DASRecord) {
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
				data := string(r.Data[:])
				system := ""
				format := ""
				expire := 0
				for _, dmap := range maps {
					surl := dasmaps.GetString(dmap, "url")
					if strings.Split(r.Url, "?")[0] == surl {
						system = dasmaps.GetString(dmap, "system")
						expire = dasmaps.GetInt(dmap, "expire")
						format = dasmaps.GetString(dmap, "format")
						break
					}
				}
				// TODO: replace with parsing and writing to mongo
				log.Println("Response", system, format, expire, r.Url, data)
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
func Process(query string, dmaps dasmaps.DASMaps) (bool, string) {
	status := true
	// parse input query and convert it into DASQuery format
	dasquery := dasql.Parse(query)
	log.Printf("Process %s\n", dasquery)
	// find out list of APIs/CMS services which can process this query request
	maps := dmaps.FindServices(dasquery.Fields, dasquery.Spec)
	var urls []string
	// loop over services and fetch data
	for _, dmap := range maps {
		furl := formUrlCall(dasquery, dmap)
		urls = append(urls, furl)
	}
	// TODO: this should be sent as goroutine
	go processURLs(urls, maps)
	// perform merge step
	log.Println("Merge DAS data records from DAS cache into DAS merge collection")
	return status, dasquery.Qhash
}

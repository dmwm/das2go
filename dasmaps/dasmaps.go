// DASMaps implementation for DAS server
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//
package dasmaps

import (
	"fmt"
	"github.com/vkuznet/das2go/mongo"
	"github.com/vkuznet/das2go/utils"
	"gopkg.in/mgo.v2/bson"
	"log"
	"regexp"
	"strings"
)

type DASMaps struct {
	records       []mongo.DASRecord
	services      []string
	notations     []mongo.DASRecord
	presentations mongo.DASRecord
	daskeys       []string
}

func (m *DASMaps) Maps() []mongo.DASRecord {
	return m.records
}

// DASMaps interface method to get list of DAS keys
func (m *DASMaps) DASKeys() []string {
	if len(m.daskeys) != 0 {
		return m.daskeys
	}
	var value string
	for _, rec := range m.records {
		rtype := rec["type"]
		if val, ok := rtype.(string); ok {
			value = val
		} else {
			continue
		}
		if value == "service" {
			dmaps := GetDASMaps(rec["das_map"])
			for _, dmap := range dmaps {
				entry := dmap["das_key"]
				if dkey, ok := entry.(string); ok {
					if !utils.FindInList(dkey, m.daskeys) {
						m.daskeys = append(m.daskeys, dkey)
					}
				}
			}
		}
	}
	return m.daskeys
}

// DASMaps interface method to get list of services
func (m *DASMaps) Services() []string {
	if len(m.services) != 0 {
		return m.services
	}
	var value string
	for _, rec := range m.records {
		rtype := rec["type"]
		if val, ok := rtype.(string); ok {
			value = val
		} else {
			continue
		}
		if value == "service" {
			system := rec["system"]
			if val, ok := system.(string); ok {
				if !utils.FindInList(val, m.services) {
					m.services = append(m.services, val)
				}
			}
		}
	}
	return m.services
}

// DASMaps interface method to get notation maps
func (m *DASMaps) NotationMaps() []mongo.DASRecord {
	if len(m.notations) != 0 {
		return m.notations
	}
	var value string
	for _, rec := range m.records {
		rtype := rec["type"]
		if val, ok := rtype.(string); ok {
			value = val
		} else {
			continue
		}
		if value == "notation" {
			m.notations = append(m.notations, rec)
		}
	}
	return m.notations
}

// DASMaps interface method to get notation maps
func (m *DASMaps) PresentationMap() mongo.DASRecord {
	if len(m.presentations) != 0 {
		return m.presentations
	}
	var value string
	for _, rec := range m.records {
		rtype := rec["type"]
		if val, ok := rtype.(string); ok {
			value = val
		} else {
			continue
		}
		if value == "presentation" {
			m.presentations = rec["presentation"].(mongo.DASRecord)
			break
		}
	}
	return m.presentations
}

// Find notation maps for given system
func (m *DASMaps) FindNotations(system string) []mongo.DASRecord {
	var out []mongo.DASRecord
	for _, rec := range m.NotationMaps() {
		val, _ := rec["system"].(string)
		if val == system {
			nmaps := GetDASMaps(rec["notations"])
			for _, nmap := range nmaps {
				out = append(out, nmap)
			}
		}
	}
	return out
}

// Find presentation maps for given primary DAS key
func (m *DASMaps) FindPresentation(daskey string) []mongo.DASRecord {
	return m.presentations[daskey].([]mongo.DASRecord)
}

// get DAS map from given record
func GetDASMaps(entry interface{}) []mongo.DASRecord {
	var maps []mongo.DASRecord
	if val, ok := entry.([]interface{}); ok {
		for _, item := range val {
			rec := item.(mongo.DASRecord)
			maps = append(maps, rec)
		}
	}
	return maps
}

// helper function to extract all required arguments for given dasmap record
func getRequiredArgs(rec mongo.DASRecord) []string {
	var out, args []string
	params := rec["params"].(mongo.DASRecord)
	for k, v := range params {
		if v == "required" {
			args = append(args, k)
		}
	}
	dasmaps := GetDASMaps(rec["das_map"])
	for _, dmap := range dasmaps {
		das_key := dmap["das_key"].(string)
		api_val := dmap["api_arg"]
		if api_val == nil {
			continue
		}
		api_arg := api_val.(string)
		for _, v := range args {
			if v == api_arg && !utils.InList(das_key, out) {
				out = append(out, das_key)
			}
		}
	}
	return out
}

// helper function to extract all required arguments for given dasmap record
func getAllArgs(rec mongo.DASRecord) []string {
	var out, args []string
	params := rec["params"].(mongo.DASRecord)
	for k, _ := range params {
		args = append(args, k)
	}
	dasmaps := GetDASMaps(rec["das_map"])
	for _, dmap := range dasmaps {
		das_key := dmap["das_key"].(string)
		api_val := dmap["api_arg"]
		if api_val == nil {
			continue
		}
		api_arg := api_val.(string)
		for _, v := range args {
			if v == api_arg && !utils.InList(das_key, out) {
				out = append(out, das_key)
			}
		}
	}
	return out
}

// helper function for DASRecord type similar to utils.InList
func MapInList(a mongo.DASRecord, list []mongo.DASRecord) bool {
	check := 0
	for _, b := range list {
		if b["urn"] == a["urn"] && b["url"] == a["url"] && b["system"] == a["system"] {
			check += 1
		}
	}
	if check != 0 {
		return true
	}
	return false
}

// Find services for given set fields and spec pair, return DAS maps associated with found services
func (m *DASMaps) FindServices(inst string, fields []string, spec bson.M) []mongo.DASRecord {
	keys := utils.MapKeys(spec)
	var cond_records, out []mongo.DASRecord
	for _, rec := range m.records {
		dasmaps := GetDASMaps(rec["das_map"])
		for _, dmap := range dasmaps {
			das_key := dmap["das_key"].(string)
			das_pat := dmap["pattern"]
			if utils.InList(das_key, keys) {
				if das_pat == nil {
					cond_records = append(cond_records, rec)
				} else {
					das_val := fmt.Sprintf("%v", spec[das_key])
					pat := fmt.Sprintf("^%s", das_pat.(string))
					matched, _ := regexp.MatchString(pat, das_val)
					if matched {
						cond_records = append(cond_records, rec)
					}
				}
			}

		}
	}
	var values []string
	for _, key := range keys {
		val, _ := spec[key].(string)
		values = append(values, val)
	}
	for _, rec := range cond_records {
		lkeys := strings.Split(rec["lookup"].(string), ",")
		rkeys := getRequiredArgs(rec)
		akeys := getAllArgs(rec)
		if utils.VERBOSE > 1 {
			log.Printf("DAS map lookup, urn %v, lookup %v, required keys %v, all keys %v", rec["urn"].(string), lkeys, rkeys, akeys)
		}
		if utils.EqualLists(lkeys, fields) && utils.CheckEntries(rkeys, keys) && utils.CheckEntries(keys, akeys) && !MapInList(rec, out) {
			// adjust DBS instance
			rec["url"] = strings.Replace(rec["url"].(string), "prod/global", inst, 1)
			log.Println("DAS match", rec["system"], rec["urn"], rec["url"], "spec keys", keys, "required keys", rkeys, "all api keys", akeys)
			out = append(out, rec)
		}
	}
	return out
}

// Load maps DASMaps API
func (m *DASMaps) LoadMaps(dbname, dbcoll string) {
	m.records = mongo.Get(dbname, dbcoll, bson.M{}, 0, -1) // index=0, limit=-1
}

// Get string value from DAS map for a given key
func GetString(dmap mongo.DASRecord, key string) string {
	val, ok := dmap[key].(string)
	if !ok {
		log.Fatal("Unable to extract key ", key, " from DAS map", dmap)
	}
	return val
}

// Get int value from DAS map for a given key
func GetInt(dmap mongo.DASRecord, key string) int {
	val, ok := dmap[key].(int)
	if !ok {
		log.Fatal("Unable to extract key ", key, " from DAS map", dmap)
	}
	return val
}

// Get float value from DAS map for a given key
func GetFloat(dmap mongo.DASRecord, key string) float64 {
	val, ok := dmap[key].(float64)
	if !ok {
		log.Fatal("Unable to extract key ", key, " from DAS map", dmap)
	}
	return val
}

// Get notation values from notation map
func GetNotation(nmap mongo.DASRecord) (string, string, string) {
	api_output := GetString(nmap, "api_output")
	rec_key := GetString(nmap, "rec_key")
	api := GetString(nmap, "api")
	return api, api_output, rec_key
}

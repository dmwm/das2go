/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS maps module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 */
package dasmaps

import (
	"gopkg.in/mgo.v2/bson"
	"log"
	"mongo"
	//     "sort"
	"strings"
	"utils"
)

type DASMaps struct {
	records       []mongo.DASRecord
	services      []string
	notations     []mongo.DASRecord
	presentations mongo.DASRecord
}

func (m *DASMaps) Maps() []mongo.DASRecord {
	return m.records
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

// Find services for given set fields and spec pair, return DAS maps associated with found services
func (m *DASMaps) FindServices(fields []string, spec bson.M) []mongo.DASRecord {
	keys := utils.MapKeys(spec)
	var cond_records, out []mongo.DASRecord
	for _, rec := range m.records {
		dasmaps := GetDASMaps(rec["das_map"])
		for _, dmap := range dasmaps {
			das_key := dmap["das_key"].(string)
			if utils.InList(das_key, keys) {
				cond_records = append(cond_records, rec)
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
		if utils.EqualLists(lkeys, fields) && utils.CheckEntries(keys, rkeys) {
			log.Println("Match", rec["system"], rec["urn"], rec["url"], keys, rkeys)
			out = append(out, rec)
		}
	}
	return out
}

// TODO: extract all required arguments for given dasmap record
func getRequiredArgs(rec mongo.DASRecord) []string {
	var out []string
	params := rec["params"].(mongo.DASRecord)
	for k, v := range params {
		if v == "required" {
			out = append(out, k)
		}
	}
	return out
}

func (m *DASMaps) LoadMaps(dbname, dbcoll string) {
	m.records = mongo.Get(dbname, dbcoll, bson.M{}, 0, -1) // index=0, limit=-1
}

func GetString(dmap mongo.DASRecord, key string) string {
	val, ok := dmap[key].(string)
	if !ok {
		log.Fatal("Unable to extract key ", key, " from DAS map", dmap)
	}
	return val
}
func GetInt(dmap mongo.DASRecord, key string) int {
	val, ok := dmap[key].(int)
	if !ok {
		log.Fatal("Unable to extract key ", key, " from DAS map", dmap)
	}
	return val
}
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

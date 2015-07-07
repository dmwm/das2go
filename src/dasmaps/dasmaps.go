/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS maps module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 */
package dasmaps

import (
	"labix.org/v2/mgo/bson"
	"log"
	"mongo"
	//     "sort"
	"strings"
	"utils"
)

type DASMaps struct {
	records  []mongo.DASRecord
	services []string
}

func (m *DASMaps) Maps() []mongo.DASRecord {
	return m.records
}

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
	for _, rec := range cond_records {
		lkeys := strings.Split(rec["lookup"].(string), ",")
		if utils.EqualLists(lkeys, fields) {
			log.Println("Match", rec["system"], rec["urn"], rec["url"])
			out = append(out, rec)
		}
	}
	return out
}

func (m *DASMaps) LoadMaps(uri, dbname, dbcoll string) {
	m.records = mongo.Get(uri, dbname, dbcoll, bson.M{})
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

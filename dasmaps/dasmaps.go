package dasmaps

// DASMaps implementation for DAS server
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
)

// DASMaps structure holds all information about DAS records
type DASMaps struct {
	records       []mongo.DASRecord
	services      []string
	notations     []mongo.DASRecord
	presentations mongo.DASRecord
	daskeys       []string
	systemApis    map[string][]string
}

// FindApiRecord finds DAS API record
func (m *DASMaps) FindApiRecord(system, urn string) mongo.DASRecord {
	var value string
	for _, rec := range m.records {
		rtype := rec["type"]
		if val, ok := rtype.(string); ok {
			value = val
		} else {
			continue
		}
		if value == "service" {
			srv := rec["system"].(string)
			api := rec["urn"].(string)
			if srv == system && api == urn {
				return rec
			}
		}
	}
	return nil
}

// Maps provides access to DAS records
func (m *DASMaps) Maps() []mongo.DASRecord {
	return m.records
}

// DASKeys provides list of DAS keys
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

// SystemApis provides map of DAS system and their apis
func (m *DASMaps) SystemApis() map[string][]string {
	if len(m.systemApis) != 0 {
		return m.systemApis
	}
	m.systemApis = make(map[string][]string)
	var value string
	for _, rec := range m.records {
		rtype := rec["type"]
		if val, ok := rtype.(string); ok {
			value = val
		} else {
			continue
		}
		if value == "service" {
			api := rec["urn"].(string)
			srv := rec["system"].(string)
			if apis, ok := m.systemApis[srv]; ok {
				apis = append(apis, api)
				m.systemApis[srv] = apis
			} else {
				m.systemApis[srv] = []string{api}
			}
		}
	}
	return m.systemApis
}

// AssignServices assigns given services to dasmaps
func (m *DASMaps) AssignServices(services []string) {
	m.services = services
}

// Services provides list of services
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

// NotationMaps provides notation maps
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

// PresentationMap provides presentation map of DAS maps
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

// FindNotations provides notation maps for given system
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

// FindPresentation returns presentations for given das key
func (m *DASMaps) FindPresentation(daskey string) []mongo.DASRecord {
	return m.presentations[daskey].([]mongo.DASRecord)
}

// GetDASMaps returns das maps for given entry
func GetDASMaps(entry interface{}) []mongo.DASRecord {
	var maps []mongo.DASRecord
	if val, ok := entry.([]interface{}); ok {
		for _, item := range val {
			rec := mongo.Convert2DASRecord(item)
			maps = append(maps, rec)
		}
	}
	return maps
}

// helper function to extract all required arguments for given dasmap record
func getRequiredArgs(rec mongo.DASRecord) []string {
	var out, args []string
	params := mongo.Convert2DASRecord(rec["params"])
	for k, v := range params {
		if v == "required" {
			args = append(args, k)
		}
	}
	dasmaps := GetDASMaps(rec["das_map"])
	for _, dmap := range dasmaps {
		dasKey := dmap["das_key"].(string)
		apiVal := dmap["api_arg"]
		if apiVal == nil {
			continue
		}
		apiArg := apiVal.(string)
		for _, v := range args {
			if v == apiArg && !utils.InList(dasKey, out) {
				out = append(out, dasKey)
			}
		}
	}
	return out
}

// helper function to extract all required arguments for given dasmap record
func getAllArgs(rec mongo.DASRecord) []string {
	var out, args []string
	params := mongo.Convert2DASRecord(rec["params"])
	for k := range params {
		args = append(args, k)
	}
	dasmaps := GetDASMaps(rec["das_map"])
	for _, dmap := range dasmaps {
		dasKey := dmap["das_key"].(string)
		apiVal := dmap["api_arg"]
		if apiVal == nil {
			continue
		}
		apiArg := apiVal.(string)
		for _, v := range args {
			if v == apiArg && !utils.InList(dasKey, out) {
				out = append(out, dasKey)
			}
		}
	}
	return out
}

// MapInList helper functions check if given map exists in given list
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

// FindServices look-up DAS services for given set fields and spec pair, return DAS maps associated with found services
func (m *DASMaps) FindServices(dasquery dasql.DASQuery) []mongo.DASRecord {
	inst := dasquery.Instance
	fields := dasquery.Fields
	spec := dasquery.Spec
	system := dasquery.System
	keys := utils.MapKeys(spec)
	var condRecords, out []mongo.DASRecord
	specKeysMatches := make(map[string][]bool)
	for _, rec := range m.records {
		dasmaps := GetDASMaps(rec["das_map"])
		var urn string
		r := rec["urn"]
		if r != nil {
			urn = r.(string)
		}
		for _, dmap := range dasmaps {
			dasKey := dmap["das_key"].(string)
			dasPattern := dmap["pattern"]
			if utils.InList(dasKey, keys) {
				if dasPattern == nil {
					condRecords = append(condRecords, rec)
					if v, ok := specKeysMatches[urn]; ok {
						v = append(v, true)
						specKeysMatches[urn] = v
					} else {
						specKeysMatches[urn] = []bool{true}
					}
				} else {
					dasValue := fmt.Sprintf("%v", spec[dasKey])
					pat := fmt.Sprintf("^%s", dasPattern.(string))
					matched, _ := regexp.MatchString(pat, dasValue)
					if matched {
						condRecords = append(condRecords, rec)
						if v, ok := specKeysMatches[urn]; ok {
							v = append(v, true)
							specKeysMatches[urn] = v
						} else {
							specKeysMatches[urn] = []bool{true}
						}
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
	for _, rec := range condRecords {
		lkeys := strings.Split(rec["lookup"].(string), ",")
		rkeys := getRequiredArgs(rec)
		akeys := getAllArgs(rec)
		if utils.VERBOSE > 1 {
			if utils.WEBSERVER > 0 {
				logs.WithFields(logs.Fields{
					"System":        rec["system"],
					"urn":           rec["urn"],
					"lookup keys":   lkeys,
					"required keys": rkeys,
					"all api keys":  akeys,
				}).Info("DAS map lookup")
			} else {
				fmt.Printf("DAS map lookup, system %s, urn %s, lookup %v, required keys %v, all keys %v\n", rec["system"].(string), rec["urn"].(string), lkeys, rkeys, akeys)
			}
		}
		if system != "" && rec["system"] != system { // requested system does not match the record one
			if system != rec["system"] {
				continue
			}
		}
		// this dict keep track of matched keys for given urn
		// we need that our selection keys not exceed number of possible matched keys
		allMatches := specKeysMatches[rec["urn"].(string)]
		if utils.EqualLists(lkeys, fields) && utils.CheckEntries(rkeys, keys) && utils.CheckEntries(keys, akeys) && !MapInList(rec, out) && len(allMatches) >= len(keys) {
			// adjust DBS instance
			//             rec["url"] = strings.Replace(rec["url"].(string), "prod/global", inst, 1)
			if utils.VERBOSE > 0 && utils.WEBSERVER > 0 {
				logs.WithFields(logs.Fields{
					"System":        rec["system"],
					"urn":           rec["urn"],
					"url":           rec["url"],
					"spec keys":     keys,
					"required keys": rkeys,
					"all api keys":  akeys,
					"instance":      inst,
				}).Info(utils.Color(utils.GREEN, "DAS match"))
			}
			if utils.VERBOSE > 1 && utils.WEBSERVER == 0 {
				// used by dasgoclient, keep fmt.Println
				msg := utils.Color(utils.GREEN, fmt.Sprintf("DAS match: system=%s urn=%s url=%s spec keys=%s requested keys=%s all api keys %s", rec["system"], rec["urn"], rec["url"], keys, rkeys, akeys))
				fmt.Println(msg)
			}
			out = append(out, rec)
		}
	}
	return out
}

// LoadMaps loads DAS maps from given database collection
func (m *DASMaps) LoadMaps(dbname, dbcoll string) {
	m.records = mongo.Get(dbname, dbcoll, bson.M{}, 0, -1) // index=0, limit=-1
}

// LoadMapsFromFile loads DAS maps from github or local file
func (m *DASMaps) LoadMapsFromFile() {
	githubUrl := "https://raw.githubusercontent.com/dmwm/DASMaps/master/js/das_maps_dbs_prod.js"
	var home string
	for _, item := range os.Environ() {
		value := strings.Split(item, "=")
		if value[0] == "HOME" {
			home = value[1]
			break
		}
	}
	dname := fmt.Sprintf("%s/.dasmaps", home)
	if _, err := os.Stat(dname); err != nil {
		os.Mkdir(dname, 0777)
	}
	fname := fmt.Sprintf("%s/.dasmaps/das_maps_dbs_prod.js", home)
	stats, err := os.Stat(fname)
	if err != nil || time.Now().Unix()-stats.ModTime().Unix() > 24*60*60 {
		if utils.VERBOSE > 0 {
			fmt.Println("### download dasmaps")
		}
		// download maps from github
		resp := utils.FetchResponse(githubUrl, "")
		if resp.Error == nil {
			// write data to local area
			err := ioutil.WriteFile(fname, []byte(resp.Data), 0777)
			if err != nil {
				msg := fmt.Sprintf("Unable to write DAS maps, error %s", err)
				panic(msg)
			}
		} else {
			msg := fmt.Sprintf("Unable to get DAS maps from github, error %s", resp.Error)
			panic(msg)
		}
	}
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		msg := fmt.Sprintf("Unable to read DAS maps from %s, error %s", fname, err)
		panic(msg)
	}
	records := string(data)
	for _, rec := range strings.Split(records, "\n") {
		if strings.Contains(rec, "hash") {
			var dmap mongo.DASRecord
			err := json.Unmarshal([]byte(rec), &dmap)
			if err == nil {
				m.records = append(m.records, dmap)
			}
		}
	}
}

// ChangeUrl changes url of dasmaps from old to new pattern
func (m *DASMaps) ChangeUrl(old, pat string) {
	var records []mongo.DASRecord
	for _, dmap := range m.records {
		if v, ok := dmap["url"]; ok {
			url := v.(string)
			if strings.Contains(url, "http://") || strings.Contains(url, "https://") {
				url = strings.Replace(url, old, pat, -1)
				dmap["url"] = url
			} else if strings.Contains(url, "combined") {
				services := dmap["services"].(map[string]interface{})
				new_services := make(map[string]string)
				for key, val := range services {
					url := val.(string)
					url = strings.Replace(url, old, pat, -1)
					new_services[key] = url
				}
				dmap["services"] = services
			}
			records = append(records, dmap)
		}
	}
	m.records = records
}

// GetString provides value from DAS map for a given key
func GetString(dmap mongo.DASRecord, key string) string {
	val, ok := dmap[key].(string)
	if !ok {
		logs.Fatal("GetString, unable to extract key ", key, " from DAS map: ", dmap)
	}
	return val
}

// GetInt provides value from DAS map for a given key
func GetInt(dmap mongo.DASRecord, key string) int {
	switch v := dmap[key].(type) {
	case int:
		return v
	case string:
		val, err := strconv.Atoi(v)
		if err == nil {
			return val
		}
		logs.Fatal("GetInt, unable to convert key ", key, " from DAS map: ", dmap, " too integer")
	}
	return 0
}

// GetFloat provides value from DAS map for a given key
func GetFloat(dmap mongo.DASRecord, key string) float64 {
	switch v := dmap[key].(type) {
	case int:
		return float64(v)
	case float64:
		return v
	case string:
		val, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return val
		}
		logs.Fatal("GetInt, unable to convert key ", key, " from DAS map: ", dmap, " too integer")
	}
	return 0
}

// GetNotation provides values from notation map
func GetNotation(nmap mongo.DASRecord) (string, string, string) {
	apiOutput := GetString(nmap, "api_output")
	recKey := GetString(nmap, "rec_key")
	api := GetString(nmap, "api")
	return api, apiOutput, recKey
}

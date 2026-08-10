package services

// DAS service module
// Rucio module
//
// Copyright (c) 2018 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// helper function to load data stream and return DAS records
func loadRucioData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord

	// Rucio uses application/x-json-stream content type which yields dict records from the server
	var row []byte
	for _, r := range data {
		if string(r) == "\n" {
			var rec mongo.DASRecord
			err := json.Unmarshal(row, &rec)
			if err != nil {
				msg := fmt.Sprintf("Rucio unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(row), err)
				if utils.VERBOSE > 0 {
					log.Printf("ERROR: Rucio unable to unmarshal, data %+v, api %v, error %v\n", string(row), api, err)
				}
				out = append(out, mongo.DASErrorRecord(msg, utils.RucioErrorName, utils.RucioError))
			}
			out = append(out, rec)
			row = []byte{}
		} else {
			row = append(row, r)
		}
	}
	// last record from Rucio does not have '\n' so we need to collect it
	if len(row) > 0 {
		var rec mongo.DASRecord
		err := json.Unmarshal(row, &rec)
		if err != nil {
			msg := fmt.Sprintf("Rucio unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(row), err)
			if utils.VERBOSE > 0 {
				log.Printf("ERROR: Rucio unable to unmarshal, data %+v, row %+v, api %v, error %v\n", string(data), string(row), api, err)
			}
			out = append(out, mongo.DASErrorRecord(msg, utils.RucioErrorName, utils.RucioError))
		}
		out = append(out, rec)
	}
	return out
}

// RucioUnmarshal unmarshals Rucio data stream and return DAS records based on api
func RucioUnmarshal(dasquery dasql.DASQuery, api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	records := loadRucioData(api, data)
	specs := dasquery.Spec
	rmap := make(mongo.DASRecord)
	if api == "block4block" {
		if val, ok := specs["block"]; ok {
			block := fmt.Sprintf("%s", val)
			if info, ok := rucioBlockReplicaInfoFromRecords(block, "", records); ok {
				rec := mongo.DASRecord{"name": block}
				for k, v := range info {
					rec[k] = v
				}
				out = append(out, rec)
			}
		}
		return out
	}
	if api == "dataset4dataset" || api == "dataset4dataset_site" {
		site := ""
		if api == "dataset4dataset_site" {
			if val, ok := specs["site"]; ok {
				site = fmt.Sprintf("%s", val)
			}
		}
		if dataset, ok := datasetSpecName(specs["dataset"]); ok {
			if rec, ok := rucioDatasetReplicaInfo(dataset, site, records); ok {
				out = append(out, rec)
			}
		}
		return out
	}
	if api == "file4dataset" || api == "file4dataset_site" {
		site := ""
		if api == "file4dataset_site" {
			if val, ok := specs["site"]; ok {
				site = fmt.Sprintf("%s", val)
			}
		}
		return rucioFilesForDataset(site, records)
	}
	for _, rec := range records {
		if api == "rses" {
			if val, ok := specs["site"]; ok {
				if rec["rse"] != nil {
					rse := rec["rse"].(string)
					site := fmt.Sprintf("%s", val)
					if strings.Contains(site, "*") {
						site = strings.Replace(site, "*", ".*", -1)
					} else {
						site = fmt.Sprintf("%s.*", site)
					}
					matched, _ := regexp.MatchString(site, rse)
					if matched {
						rec["name"] = rse
						out = append(out, rec)
					}
				}
			}
		} else if api == "site4dataset" || api == "site4block" || api == "site4file" {
			if rec["states"] != nil {
				states := rec["states"].(map[string]interface{})
				for rse, _ := range states {
					// we need to create a new map record since we'll reassign
					// rse name as a main key
					newrec := make(map[string]interface{})
					for k, v := range rec {
						newrec[k] = v
					}
					newrec["name"] = rse
					out = append(out, newrec)
				}
			}
		} else if api == "dataset4site" {
			if rec["name"] != nil {
				blk := rec["name"].(string)
				arr := strings.Split(blk, "#")
				rmap[arr[0]] = 1
			}
		} else if api == "block4site" {
			if rec["name"] != nil {
				out = append(out, rec)
			}
		} else if api == "rules4dataset" || api == "rules4block" || api == "rules4file" {
			out = append(out, rec)
		} else if api == "block4dataset" {
			if rec["name"] != nil {
				block := rec["name"].(string)
				if info, ok := rucioBlockReplicaInfo(block, ""); ok {
					for k, v := range info {
						rec[k] = v
					}
				}
			}
			out = append(out, rec)
		} else if api == "block4dataset_site" {
			if val, ok := specs["site"]; ok && rec["name"] != nil {
				site := fmt.Sprintf("%s", val)
				block := rec["name"].(string)
				if info, ok := rucioBlockReplicaInfo(block, site); ok {
					for k, v := range info {
						rec[k] = v
					}
					out = append(out, rec)
				}
			}
		} else if api == "full_record" {
			out = append(out, rec)
		} else if api == "file4block_site" {
			if val, ok := specs["site"]; ok {
				site := fmt.Sprintf("%s", val)
				if rec["states"] != nil {
					states := rec["states"].(map[string]interface{})
					var sites []string
					for rse := range states {
						sites = append(sites, rse)
					}
					if utils.InList(site, sites) {
						out = append(out, rec)
					}
				}
			}
		} else {
			if rec["states"] != nil {
				states := rec["states"].(map[string]interface{})
				var replicas []mongo.DASRecord
				for k, v := range states {
					rep := mongo.DASRecord{"name": k, "state": v}
					replicas = append(replicas, rep)
				}
				rec["replicas"] = replicas
				out = append(out, rec)
			}
		}
	}
	if api == "dataset4site" {
		for d := range rmap {
			rec := mongo.DASRecord{"name": d}
			out = append(out, rec)
		}
	}
	return out
}

func datasetSpecName(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, v != ""
	case []string:
		if len(v) == 1 {
			return v[0], v[0] != ""
		}
	case []interface{}:
		if len(v) == 1 {
			name, ok := v[0].(string)
			return name, ok && name != ""
		}
	}
	return "", false
}

func rucioDatasetReplicaInfo(dataset, site string, records []mongo.DASRecord) (mongo.DASRecord, bool) {
	rec := mongo.DASRecord{
		"name":   dataset,
		"states": mongo.DASRecord{},
		"rses":   mongo.DASRecord{},
	}
	nblocks := 0
	for _, row := range records {
		block, ok := row["name"].(string)
		if !ok || block == "" {
			continue
		}
		info, ok := rucioBlockReplicaInfo(block, site)
		if !ok {
			continue
		}
		nblocks += 1
		mergeRucioDatasetReplicaInfo(rec, info)
	}
	if nblocks == 0 {
		return nil, false
	}
	rec["nblocks"] = nblocks
	if bytes := rec["bytes"]; bytes != nil {
		rec["size"] = bytes
	}
	return rec, true
}

func mergeRucioDatasetReplicaInfo(dst mongo.DASRecord, src mongo.DASRecord) {
	for _, key := range []string{"scope", "type"} {
		if dst[key] == nil && src[key] != nil {
			dst[key] = src[key]
		}
	}
	for _, key := range []string{"bytes", "available_bytes", "length", "available_length"} {
		if val, ok := rucioNumericValue(src[key]); ok {
			cur, _ := rucioNumericValue(dst[key])
			dst[key] = cur + val
		}
	}
	mergeRucioRecordMap(dst, src, "states")
	mergeRucioRSEs(dst, src)
}

func mergeRucioRecordMap(dst mongo.DASRecord, src mongo.DASRecord, key string) {
	dmap, ok := dst[key].(mongo.DASRecord)
	if !ok {
		dmap = mongo.DASRecord{}
		dst[key] = dmap
	}
	switch smap := src[key].(type) {
	case mongo.DASRecord:
		for k, v := range smap {
			dmap[k] = v
		}
	case map[string]interface{}:
		for k, v := range smap {
			dmap[k] = v
		}
	}
}

func mergeRucioRSEs(dst mongo.DASRecord, src mongo.DASRecord) {
	dmap, ok := dst["rses"].(mongo.DASRecord)
	if !ok {
		dmap = mongo.DASRecord{}
		dst["rses"] = dmap
	}
	switch smap := src["rses"].(type) {
	case mongo.DASRecord:
		for rse := range smap {
			dmap[rse] = []interface{}{}
		}
	case map[string]interface{}:
		for rse := range smap {
			dmap[rse] = []interface{}{}
		}
	}
}

func rucioNumericValue(value interface{}) (float64, bool) {
	switch val := value.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case json.Number:
		num, err := val.Float64()
		return num, err == nil
	}
	return 0, false
}

// rucioFilesForDataset resolves the block DIDs returned for a CMS dataset
// container. A complete block can use its compact replica summary plus its DID
// contents; a partial block needs the file-level replica response so that only
// files actually available at the requested site are returned.
func rucioFilesForDataset(site string, blocks []mongo.DASRecord) []mongo.DASRecord {
	var out []mongo.DASRecord
	seenBlocks := make(map[string]bool)
	seenFiles := make(map[string]bool)
	for _, row := range blocks {
		block, ok := row["name"].(string)
		if !ok || block == "" || seenBlocks[block] {
			continue
		}
		seenBlocks[block] = true

		summaryURL := fmt.Sprintf("%s/replicas/cms/%s/datasets?deep=True", RucioUrl(), url.QueryEscape(block))
		summary, err := fetchRucioRecords("file4dataset", summaryURL)
		if err != nil {
			out = append(out, rucioErrorRecord(err))
			continue
		}
		relevant, complete := rucioBlockCompleteness(site, summary)
		if !relevant {
			continue
		}

		api := "file4dataset"
		filesURL := rucioDIDContentsURL(block)
		filterBySite := false
		if !complete {
			filesURL = fmt.Sprintf("%s/replicas/cms/%s", RucioUrl(), url.QueryEscape(block))
			filterBySite = site != ""
			api = "file4dataset_partial"
		}
		files, err := fetchRucioRecords(api, filesURL)
		if err != nil {
			out = append(out, rucioErrorRecord(err))
			continue
		}
		for _, rec := range files {
			if filterBySite && !rucioFileAvailableAtSite(rec, site) {
				continue
			}
			name, ok := rec["name"].(string)
			if !ok || name == "" {
				out = append(out, rec)
				continue
			}
			if !seenFiles[name] {
				seenFiles[name] = true
				out = append(out, rec)
			}
		}
	}
	return out
}

func fetchRucioRecords(api, furl string) ([]mongo.DASRecord, error) {
	resp := utils.FetchResponse(utils.HttpClient(), furl, "")
	if resp.Error != nil {
		return nil, fmt.Errorf("Rucio request failed, api=%s, url=%s, error=%v", api, furl, resp.Error)
	}
	return loadRucioData(api, resp.Data), nil
}

func rucioDIDContentsURL(name string) string {
	name = strings.Replace(name, "#", "%23", -1)
	if strings.HasPrefix(name, "/") {
		return fmt.Sprintf("%s/dids/cms%s/dids", RucioUrl(), name)
	}
	return fmt.Sprintf("%s/dids/cms/%s/dids", RucioUrl(), name)
}

func rucioErrorRecord(err error) mongo.DASRecord {
	return mongo.DASErrorRecord(err.Error(), utils.RucioErrorName, utils.RucioError)
}

func rucioBlockCompleteness(site string, records []mongo.DASRecord) (bool, bool) {
	relevant := false
	for _, rec := range records {
		rse, ok := rec["rse"].(string)
		if !ok || !rucioSiteMatch(site, rse) {
			continue
		}
		relevant = true
		length, lengthOK := rucioNumericValue(rec["length"])
		available, availableOK := rucioNumericValue(rec["available_length"])
		if lengthOK && availableOK && available == length {
			return true, true
		}
	}
	return relevant, false
}

func rucioFileAvailableAtSite(rec mongo.DASRecord, site string) bool {
	match := func(states map[string]interface{}) bool {
		for rse, state := range states {
			if rucioSiteMatch(site, rse) && strings.EqualFold(fmt.Sprintf("%v", state), "AVAILABLE") {
				return true
			}
		}
		return false
	}
	switch states := rec["states"].(type) {
	case mongo.DASRecord:
		return match(map[string]interface{}(states))
	case map[string]interface{}:
		return match(states)
	}
	return false
}

func rucioBlockReplicaInfo(block, site string) (mongo.DASRecord, bool) {
	furl := fmt.Sprintf("%s/replicas/cms/%s/datasets?deep=True", RucioUrl(), url.QueryEscape(block))
	client := utils.HttpClient()
	resp := utils.FetchResponse(client, furl, "")
	if resp.Error != nil {
		return nil, false
	}
	return rucioBlockReplicaInfoFromRecords(block, site, loadRucioData("block4dataset_site", resp.Data))
}

func rucioBlockReplicaInfoFromRecords(block, site string, records []mongo.DASRecord) (mongo.DASRecord, bool) {
	info := make(mongo.DASRecord)
	states := make(mongo.DASRecord)
	rses := make(mongo.DASRecord)
	var replicas []mongo.DASRecord
	for _, rec := range records {
		if rec["rse"] == nil {
			continue
		}
		rse := rec["rse"].(string)
		if !rucioSiteMatch(site, rse) {
			continue
		}
		replica := make(mongo.DASRecord)
		for k, v := range rec {
			replica[k] = v
			if k != "name" && k != "scope" {
				info[k] = v
			}
		}
		if rec["state"] != nil {
			states[rse] = rec["state"]
		}
		rses[rse] = []interface{}{}
		replicas = append(replicas, replica)
	}
	if len(replicas) == 0 {
		return nil, false
	}
	info["states"] = states
	info["rses"] = rses
	info["replicas"] = replicas
	return info, true
}

func rucioSiteMatch(site, rse string) bool {
	if site == "" {
		return true
	}
	if strings.Contains(site, "*") {
		pat := "^" + strings.Replace(regexp.QuoteMeta(site), "\\*", ".*", -1) + "$"
		matched, _ := regexp.MatchString(pat, rse)
		return matched
	}
	return site == rse
}

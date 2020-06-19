package services

// DAS service module
// CRIC module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// helper function to load CRIC data stream
func loadCRICData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord

	// to prevent json.Unmarshal behavior to convert all numbers to float
	// we'll use json decode method with instructions to use numbers as is
	buf := bytes.NewBuffer(data)
	dec := json.NewDecoder(buf)
	dec.UseNumber()
	err := dec.Decode(&rec)

	// original way to decode data
	// err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("CRIC unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		if utils.VERBOSE > 0 {
			log.Printf("ERROR: CRIC unable to unmarshal, data %+v, api %v, error %v\n", string(data), api, err)
		}
		out = append(out, mongo.DASErrorRecord(msg, utils.CRICErrorName, utils.CRICError))
		return out
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	values := rec["result"].([]interface{})
	for _, item := range values {
		row := make(mongo.DASRecord)
		val := item.([]interface{})
		for i, h := range headers {
			key := h.(string)
			row[key] = val[i]
			if key == "username" {
				row["name"] = row[key]
			}
		}
		out = append(out, row)
	}
	return out
}

// CRICUnmarshal unmarshals CRIC data stream and return DAS records based on api
func CRICUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadCRICData(api, data)
	return records
}

func getCRICData(api string) []mongo.DASRecord {
	furl := CricUrl(api)
	if strings.Contains(api, "site") {
		furl = fmt.Sprintf("%s?json&preset=site-names&rcsite_state=ANY", furl)
	} else if strings.Contains(api, "group") {
		furl = fmt.Sprintf("%s?json&preset=group-responsibilities", furl)
	} else if strings.Contains(api, "role") {
		furl = fmt.Sprintf("%s?json&preset=roles", furl)
	} else {
		furl = fmt.Sprintf("%s?json&preset=people", furl)
	}
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadCRICData(api, response.Data)
		return records
	}
	log.Printf("ERROR: cric, api %v, error %v\n", api, response.Error)
	var out []mongo.DASRecord
	return out
}

// CricSiteNames local API returns site-names
func (LocalAPIs) CricSiteNames(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	api := "site-names"
	site := spec["site"].(string)
	sitePattern := ""
	if strings.Contains(site, "*") {
		sitePattern = strings.Replace(site, "*", "", -1)
	}
	records := getCRICData(api)
	for _, r := range records {
		siteName := r["alias"].(string)
		r["name"] = r["alias"]
		if siteName == site {
			out = append(out, r)
		} else if len(sitePattern) > 0 {
			matched, _ := regexp.MatchString(fmt.Sprintf("^%s", sitePattern), siteName)
			if matched {
				out = append(out, r)
			}
		}
	}
	return out
}

// CricGroups local API returns group names
func (LocalAPIs) CricGroups(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	api := "groups"
	group := spec["group"].(string)
	groupPattern := ""
	if strings.Contains(group, "*") {
		groupPattern = strings.Replace(group, "*", "", -1)
	}
	records := getCRICData(api)
	for _, r := range records {
		groupName := r["name"].(string)
		if groupName == group {
			out = append(out, r)
		} else if len(groupPattern) > 0 && strings.Contains(groupName, groupPattern) {
			out = append(out, r)
		}
	}
	return out
}

// CricGroupResponsibilities return group responsibilities
func (LocalAPIs) CricGroupResponsibilities(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	api := "group-responsibilities"
	group := spec["group"].(string)
	groupPattern := ""
	if strings.Contains(group, "*") {
		groupPattern = strings.Replace(group, "*", "", -1)
	}
	records := getCRICData(api)
	for _, r := range records {
		val := r["user_name"]
		if val != nil {
			groupName := val.(string)
			r["name"] = r["user_group"]
			if groupName == group {
				out = append(out, r)
			} else if len(groupPattern) > 0 && strings.Contains(groupName, groupPattern) {
				out = append(out, r)
			}
		}
	}
	return out
}

// CricPeopleEmail returns CRIC people via email
func (LocalAPIs) CricPeopleEmail(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	api := "people"
	user := spec["user"].(string)
	records := getCRICData(api)
	for _, r := range records {
		if r["email"].(string) == user {
			out = append(out, r)
		}
	}
	return out
}

// CricPeopleName returns CRIC people via names
func (LocalAPIs) CricPeopleName(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	api := "people"
	user := strings.ToLower(spec["user"].(string))
	records := getCRICData(api)
	for _, r := range records {
		username := strings.ToLower(r["username"].(string))
		forename := strings.ToLower(r["forename"].(string))
		surname := strings.ToLower(r["surname"].(string))
		email := strings.ToLower(r["email"].(string))
		if username == user || forename == user || surname == user || email == user {
			out = append(out, r)
		}
	}
	return out
}

// CricRoles returns CRIC roles
func (LocalAPIs) CricRoles(dasquery dasql.DASQuery) []mongo.DASRecord {
	spec := dasquery.Spec
	var out []mongo.DASRecord
	api := "roles"
	role := spec["role"].(string)
	rolePattern := ""
	if strings.Contains(role, "*") {
		rolePattern = strings.Replace(role, "*", "", -1)
	}
	records := getCRICData(api)
	for _, r := range records {
		roleTitle := r["title"].(string)
		if roleTitle == role {
			out = append(out, r)
		} else if len(rolePattern) > 0 && strings.Contains(roleTitle, rolePattern) {
			out = append(out, r)
		}
	}
	return out
}

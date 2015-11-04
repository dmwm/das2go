/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: SiteDB module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
	"mongo"
	"strings"
	"utils"
)

// helper function to load SiteDB data stream
func loadSiteDBData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		msg := fmt.Sprintf("SiteDB unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		//         panic(msg)
		fmt.Println(msg)
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

// Unmarshal SiteDB data stream and return DAS records based on api
func SiteDBUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadSiteDBData(api, data)
	return records
}

/*
 * Local SiteDB APIs
 */
func getSiteDBData(api string) []mongo.DASRecord {
	furl := fmt.Sprintf("%s/%s", sitedbUrl(), api)
	response := utils.FetchResponse(furl, "")
	if response.Error == nil {
		records := loadSiteDBData(api, response.Data)
		return records
	} else {
		log.Println(fmt.Sprintf("DAS ERROR, SiteDB API=%s, error=%s", api, response.Error))
	}
	var out []mongo.DASRecord
	return out
}

// site-names
func (LocalAPIs) L_sitedb2_site_names(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	api := "site-names"
	site := spec["site"].(string)
	records := getSiteDBData(api)
	for _, r := range records {
		if r["site_name"].(string) == site {
			r["name"] = r["site_name"]
			out = append(out, r)
		}
	}
	return out
}

// groups
func (LocalAPIs) L_sitedb2_groups(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	api := "groups"
	group := spec["group"].(string)
	records := getSiteDBData(api)
	for _, r := range records {
		if r["name"].(string) == group {
			out = append(out, r)
		}
	}
	return out
}

// group_responsibilities
func (LocalAPIs) L_sitedb2_group_responsibilities(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	api := "group-responsibilities"
	group := spec["group"].(string)
	records := getSiteDBData(api)
	for _, r := range records {
		if r["user_group"].(string) == group {
			r["name"] = r["user_group"]
			out = append(out, r)
		}
	}
	return out
}

// people_via_email
func (LocalAPIs) L_sitedb2_people_via_email(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	api := "people"
	user := spec["user"].(string)
	records := getSiteDBData(api)
	for _, r := range records {
		if r["email"].(string) == user {
			out = append(out, r)
		}
	}
	return out
}

// people_via_name
func (LocalAPIs) L_sitedb2_people_via_name(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	api := "people"
	user := strings.ToLower(spec["user"].(string))
	records := getSiteDBData(api)
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

// roles
func (LocalAPIs) L_sitedb2_roles(spec bson.M) []mongo.DASRecord {
	var out []mongo.DASRecord
	api := "roles"
	role := spec["role"].(string)
	records := getSiteDBData(api)
	for _, r := range records {
		if r["title"].(string) == role {
			out = append(out, r)
		}
	}
	return out
}

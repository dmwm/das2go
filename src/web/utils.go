package web

import (
	"crypto/md5"
	"dasql"
	"encoding/hex"
	"fmt"
	"mongo"
	"strings"
	"utils"
)

// helper function to make a link
func href(path, daskey, value string) string {
	key := strings.Split(daskey, ".")[0]
	ref := fmt.Sprintf("%s=%s", key, value)
	out := fmt.Sprintf("<span class=\"highlight\"><a href=\"%s?input=%s\">%s</a></span>", path, ref, value)
	return out
}

func genColor(system string) (string, string) {
	var bkg string
	col := "black"
	if system == "das" {
		bkg = "#DFDBC3"
	} else if system == "dbs" {
		bkg = "#008B8B"
		col = "white"
	} else if system == "dbs3" {
		bkg = "#006400"
		col = "white"
	} else if system == "phedex" {
		bkg = "#00BFBF"
	} else if system == "sitedb2" {
		bkg = "#6495ED"
		col = "white"
	} else if system == "runregistry" {
		bkg = "#FF8C00"
	} else if system == "dashboard" {
		bkg = "#DAA520"
	} else if system == "conddb" {
		bkg = "#FFD700"
	} else if system == "reqmgr" {
		bkg = "#696969"
		col = "white"
	} else if system == "combined" {
		bkg = "#7B68EE"
		col = "white"
	} else if system == "tier0" {
		bkg = "#AFEEEE"
	} else if system == "monitor" {
		bkg = "#FF4500"
	} else if system == "prep2" {
		bkg = "#CCFF66"
	} else {
		data := []byte(system)
		arr := md5.Sum(data)
		bkg = "#" + hex.EncodeToString(arr[:])[0:6]
		col = "white"
	}
	return bkg, col
}

// helper function to show services
func colServices(services []string) string {
	out := make(map[string]interface{})
	for _, val := range services {
		bkg, col := genColor(val)
		srv := fmt.Sprintf("<span style=\"background-color:%s;color:%s;padding:2px\">%s</span>", bkg, col, val)
		out[srv] = 1
	}
	return "Sources: " + strings.Join(utils.MapKeys(out), "")
}

// helper function to create links
func dasLinks(path, inst, val string, links []interface{}) string {
	var out []string
	for _, row := range links {
		rec := row.(mongo.DASRecord)
		name := rec["name"].(string)
		query := fmt.Sprintf(rec["query"].(string), val)
		link := fmt.Sprintf("<a href=\"%s?instance=%s&input=%s\">%s</a>", path, inst, query, name)
		out = append(out, link)
	}
	return "<br/>" + strings.Join(out, ", ")
}

// Represent DAS records for web UI
func PresentData(path string, dasquery dasql.DASQuery, data []mongo.DASRecord, pmap mongo.DASRecord) string {
	var out []string
	line := "<hr class=\"line\" />"
	//     br := "<br/>"
	fields := dasquery.Fields
	var services []string
	for jdx, item := range data {
		das := item["das"].(mongo.DASRecord)
		if len(services) == 0 {
			for _, v := range das["services"].([]interface{}) {
				srv := strings.Split(v.(string), ":")[0]
				services = append(services, srv)
			}
		}
		pkey := das["primary_key"].(string)
		inst := das["instance"].(string)
		for _, key := range fields {
			records := item[key].([]interface{})
			uiRows := pmap[key].([]interface{})
			var links []interface{}
			var pval string
			for idx, elem := range records {
				rec := elem.(mongo.DASRecord)
				var values []string
				for _, uir := range uiRows {
					uirow := uir.(mongo.DASRecord)
					daskey := uirow["das"].(string)
					if links == nil {
						links = uirow["link"].([]interface{})
					}
					if idx != 0 && daskey == pkey {
						continue // look-up only once primary key
					}
					webkey := uirow["ui"].(string)
					attrs := strings.Split(daskey, ".")
					attr := strings.Join(attrs[1:len(attrs)], ".")
					value := ExtractValue(rec, attr)
					if pval == "" {
						pval = value
					}
					if len(value) > 0 {
						var row string
						if daskey == pkey {
							row = fmt.Sprintf("%s: %v\n<br/>\n", webkey, href(path, pkey, value))
						} else {
							row = fmt.Sprintf("%s: %v\n", webkey, value)
						}
						values = append(values, row)
					}
				}
				out = append(out, strings.Join(values, ", "))
			}
			out = append(out, dasLinks(path, inst, pval, links))
		}
		out = append(out, colServices(services))
		if jdx != len(data) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

// helper function to extract value from das record
// relies on type switching, see
// https://golang.org/doc/effective_go.html#type_switch
func ExtractValue(data mongo.DASRecord, daskey string) string {
	var out []string
	keys := strings.Split(daskey, ".")
	count := 1
	for _, key := range keys {
		value := data[key]
		if value == nil {
			return ""
		}
		switch value := value.(type) {
		case string:
			out = append(out, value)
		case int:
			out = append(out, fmt.Sprintf("%d", value))
		case int64:
			out = append(out, fmt.Sprintf("%d", value))
		case float64:
			if key == "size" {
				out = append(out, utils.SizeFormat(value))
			} else {
				out = append(out, fmt.Sprintf("%v", value))
			}
		case []interface{}:
			for _, rec := range value {
				value := ExtractValue(rec.(mongo.DASRecord), strings.Join(keys[count:len(keys)], "."))
				out = append(out, fmt.Sprintf("%v", value))
			}
			break
		default:
			if count != len(keys) {
				return ExtractValue(value.(mongo.DASRecord), strings.Join(keys[count:len(keys)], "."))
			}
			out = append(out, fmt.Sprintf("%v", value))
		}
		count = count + 1
	}
	return strings.Join(out, ",")
}

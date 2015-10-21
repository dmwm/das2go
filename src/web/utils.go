package web

import (
	"crypto/md5"
	"dasql"
	"encoding/hex"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"mongo"
	"sort"
	"strings"
	"time"
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

// implement sort for []string type
type StringList []string

func (s StringList) Len() int           { return len(s) }
func (s StringList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StringList) Less(i, j int) bool { return s[i] < s[j] }

// helper function to show services
func colServices(services []string) string {
	out := make(map[string]interface{})
	for _, val := range services {
		bkg, col := genColor(val)
		srv := fmt.Sprintf("<span style=\"background-color:%s;color:%s;padding:2px\">%s</span>", bkg, col, val)
		out[val] = srv
	}
	var srvs []string
	keys := utils.MapKeys(out)
	sort.Sort(StringList(keys))
	for _, k := range keys {
		srvs = append(srvs, out[k].(string))
	}
	return "Sources: " + strings.Join(srvs, "")
	//     return "Sources: " + strings.Join(utils.MapKeys(out), "")
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

// helper function to show|hide DAS record on web UI
func showRecord(data mongo.DASRecord) string {
	var out []string
	var rid string
	did := data["_id"]
	if did != nil {
		oid := data["_id"].(bson.ObjectId)
		rid = oid.Hex()
	} else {
		fun := data["function"].(string)
		rid = fmt.Sprintf("%d-%s", int64(time.Now().Unix()), fun)
	}
	das := data["das"].(mongo.DASRecord)
	pkey := strings.Split(das["primary_key"].(string), ".")[0]
	for i, v := range das["services"].([]interface{}) {
		srv := v.(string)
		arr := strings.Split(srv, ":")
		system := arr[0]
		dasapi := arr[1]
		bkg, col := genColor(system)
		srvval := fmt.Sprintf("<span style=\"background-color:%s;color:%s;padding:2px\">%s</span>", bkg, col, system)
		out = append(out, fmt.Sprintf("DAS service: %v DAS api: %s", srvval, dasapi))
		var rec mongo.DASRecord
		if data[pkey] != nil {
			vvv := data[pkey].([]interface{})
			rec = vvv[i].(mongo.DASRecord)
		} else {
			rec = data
		}
		out = append(out, fmt.Sprintf("<pre style=\"background-color:%s;color:white;\"><div class=\"code\"><pre>%s</pre></div></pre><br/>", bkg, rec.ToString()))
	}
	val := fmt.Sprintf("<div class=\"hide\" id=\"id_%s\"><div class=\"code\">%s</div></div>", rid, strings.Join(out, "\n"))
	wrap := fmt.Sprintf("<a href=\"javascript:ToggleTag('id_%s', 'link_%s')\" id=\"link_%s\">show</a>", rid, rid, rid)
	return wrap + val
}

// helper function to provide proper url
func makeUrl(url, urlType string, startIdx, limit, nres int) string {
	var out string
	var idx int
	if urlType == "first" {
		idx = 0
	} else if urlType == "prev" {
		if startIdx != 0 {
			idx = startIdx - limit
		} else {
			idx = 0
		}
	} else if urlType == "next" {
		idx = startIdx + limit
	} else if urlType == "last" {
		j := 0
		for i := 0; i < nres; i = i + limit {
			if i > nres {
				break
			}
			j = i
		}
		idx = j
	}
	out = fmt.Sprintf("%s&amp;idx=%d&&amp;limit=%d", url, idx, limit)
	return out
}

// helper function to provide pagination
func pagination(base, query string, nres, startIdx, limit int) string {
	var templates DASTemplates
	url := fmt.Sprintf("%s?input=%s", base, query)
	tmplData := make(map[string]interface{})
	tmplData["StartIndex"] = fmt.Sprintf("%d", startIdx)
	tmplData["EndIndex"] = fmt.Sprintf("%d", startIdx+limit)
	tmplData["Total"] = fmt.Sprintf("%d", nres)
	tmplData["FirstUrl"] = makeUrl(url, "first", startIdx, limit, nres)
	tmplData["PrevUrl"] = makeUrl(url, "prev", startIdx, limit, nres)
	tmplData["NextUrl"] = makeUrl(url, "next", startIdx, limit, nres)
	tmplData["LastUrl"] = makeUrl(url, "last", startIdx, limit, nres)
	page := templates.Pagination(_tdir, tmplData) // _tdir defined in web/server.go
	line := "<hr class=\"line\" />"
	return fmt.Sprintf("%s%s<br/>", page, line)
}

// Represent DAS records for web UI
func PresentData(path string, dasquery dasql.DASQuery, data []mongo.DASRecord, pmap mongo.DASRecord, nres, startIdx, limit int) string {
	var out []string
	line := "<hr class=\"line\" />"
	total := nres
	if len(dasquery.Aggregators) > 0 {
		total = len(dasquery.Aggregators)
	}
	out = append(out, pagination(path, dasquery.Query, total, startIdx, limit))
	//     br := "<br/>"
	fields := dasquery.Fields
	var pkey, inst string
	var das mongo.DASRecord
	var services []string
	for jdx, item := range data {
		das = item["das"].(mongo.DASRecord)
		if len(services) == 0 {
			for _, v := range das["services"].([]interface{}) {
				srv := strings.Split(v.(string), ":")[0]
				services = append(services, srv)
			}
		}
		pkey = das["primary_key"].(string)
		inst = das["instance"].(string)
		// aggregator part
		if len(dasquery.Aggregators) > 0 {
			fname := item["function"].(string)
			fkey := item["key"].(string)
			res := item["result"].(mongo.DASRecord)
			var val string
			if strings.HasSuffix(fkey, "size") {
				val = fmt.Sprintf("%s(%s)=%v<br/>\n", fname, fkey, utils.SizeFormat(res["value"].(float64)))
			} else {
				val = fmt.Sprintf("%s(%s)=%v<br/>\n", fname, fkey, res["value"])
			}
			out = append(out, val)
			out = append(out, colServices(services))
			out = append(out, showRecord(item))
			if jdx != len(data) {
				out = append(out, line)
			}
			continue
		}
		// record part
		var links []interface{}
		var pval string
		var values []string
		for _, key := range fields {
			records := item[key].([]interface{})
			uiRows := pmap[key].([]interface{})
			for idx, elem := range records {
				rec := elem.(mongo.DASRecord)
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
			}
		}
		// Join attribute fields, e.g. in file dataset=/a/b/c query it is
		// File size: N GB File Type: EDM
		if len(values) == 1 {
			values[0] = strings.Replace(values[0], "<br/>", "", 1)
		}
		out = append(out, strings.Join(utils.List2Set(values), " "))
		out = append(out, dasLinks(path, inst, pval, links))
		out = append(out, colServices(services))
		out = append(out, showRecord(item))
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
			} else if strings.HasSuffix(key, "time") {
				out = append(out, utils.TimeFormat(value))
			} else {
				out = append(out, fmt.Sprintf("%v", value))
			}
		case []interface{}:
			for _, rec := range value {
				var value string
				switch vvv := rec.(type) {
				case mongo.DASRecord:
					value = ExtractValue(vvv, strings.Join(keys[count:len(keys)], "."))
				default:
					value = fmt.Sprintf("%v", vvv)
				}
				out = append(out, value)
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

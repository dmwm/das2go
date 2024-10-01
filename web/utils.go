package web

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/das"
	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	"gopkg.in/mgo.v2/bson"
)

// helper function to make a link of first element of the record in web UI presentation
func href(path, daskey, value, inst, query string) string {
	key := strings.Split(daskey, ".")[0]
	var ref string
	if key == "parent" || key == "child" {
		if strings.Contains(query, "dataset=") {
			ref = fmt.Sprintf("dataset=%s", value)
		} else if strings.Contains(query, "block=") {
			ref = fmt.Sprintf("block=%s", value)
		} else if strings.Contains(query, "file=") {
			ref = fmt.Sprintf("file=%s", value)
		} else {
			ref = fmt.Sprintf("%s=%s", key, value)
		}
	} else {
		ref = fmt.Sprintf("%s=%s", key, value)
	}
	var furl url.URL
	furl.Path = path
	parameters := url.Values{}
	parameters.Add("input", ref)
	furl.RawQuery = parameters.Encode()
	out := fmt.Sprintf("<span class=\"highlight\"><a href=\"%s&instance=%s\">%s</a></span>", furl.String(), inst, value)
	// if value is config one we simply need to show its value
	if strings.Contains(furl.String(), "config") {
		out = fmt.Sprintf("<span class=\"highlight\">%s</span>", value)
	}
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
	} else if system == "rucio" {
		bkg = "#11d7b5"
	} else if system == "dynamo" {
		bkg = "#13d152"
	} else if system == "sitedb2" {
		bkg = "#6495ED"
		col = "white"
	} else if system == "cric" {
		bkg = "#00d9ff"
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

// helper function to convert URLs into human readable form
func urlsFormat(urls interface{}) string {
	var out []string
	rec := urls.(mongo.DASRecord)
	for _, val := range rec {
		output := val.([]interface{})
		for i, v := range output {
			url := fmt.Sprintf("<a href=\"%s\">url-%d</a>", v, i)
			out = append(out, url)
		}
	}
	return strings.Join(out, ", ")
}

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
	sort.Sort(utils.StringList(keys))
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
		if strings.Contains(name, "%s") {
			name = fmt.Sprintf(name, val)
		}
		if v, ok := rec["query"]; ok {
			q := v.(string)
			if q != "" {
				query := fmt.Sprintf(q, val)
				link := fmt.Sprintf("<a href=\"%s?instance=%s&input=%s\">%s</a>", path, inst, url.QueryEscape(query), name)
				out = append(out, link)
			}
		}
		if v, ok := rec["url"]; ok {
			q := v.(string)
			if q != "" {
				qurl := fmt.Sprintf(q, val)
				link := fmt.Sprintf("<a href=\"%s\">%s</a>", qurl, name)
				out = append(out, link)
			}
		}
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
			switch r := data[pkey].(type) {
			case []interface{}:
				vvv := data[pkey].([]interface{})
				if len(vvv) > 0 && len(vvv) >= i {
					if vvv[i] != nil {
						rec = vvv[i].(mongo.DASRecord)
					}
				} else {
					rec = nil
				}
			case mongo.DASRecord:
				rec = r
			}
			//             vvv := data[pkey].([]interface{})
			//             rec = vvv[i].(mongo.DASRecord)
		} else {
			rec = data
		}
		if rec != nil {
			out = append(out, fmt.Sprintf("<pre style=\"background-color:%s;color:white;\"><div class=\"code\"><pre>%s</pre></div></pre><br/>", bkg, rec.ToString()))
		}
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
	out = fmt.Sprintf("%s&idx=%d&limit=%d", url, idx, limit)
	return out
}

// helper function to provide pagination
func pagination(base, query, inst string, nres, startIdx, limit int) string {
	var templates DASTemplates
	url := fmt.Sprintf("%s?input=%s&instance=%s", base, url.QueryEscape(query), inst)
	tmplData := make(map[string]interface{})
	if nres > 0 {
		tmplData["StartIndex"] = fmt.Sprintf("%d", startIdx+1)
	} else {
		tmplData["StartIndex"] = fmt.Sprintf("%d", startIdx)
	}
	if nres > startIdx+limit {
		tmplData["EndIndex"] = fmt.Sprintf("%d", startIdx+limit)
	} else {
		tmplData["EndIndex"] = fmt.Sprintf("%d", nres)
	}
	tmplData["Total"] = fmt.Sprintf("%d", nres)
	tmplData["FirstUrl"] = makeUrl(url, "first", startIdx, limit, nres)
	tmplData["PrevUrl"] = makeUrl(url, "prev", startIdx, limit, nres)
	tmplData["NextUrl"] = makeUrl(url, "next", startIdx, limit, nres)
	tmplData["LastUrl"] = makeUrl(url, "last", startIdx, limit, nres)
	page := templates.Pagination(config.Config.Templates, tmplData)
	line := "<hr class=\"line\" />"
	return fmt.Sprintf("%s%s<br/>", page, line)
}

// helper function to
// Helper function to show lumi-events pairs suitable for web UI
func lumiEvents(rec mongo.DASRecord) string {
	var run int64
	if rec == nil || rec["run"] == nil {
		return ""
	}
	for _, v := range rec["run"].([]interface{}) {
		r := v.(mongo.DASRecord)
		if r != nil {
			run = r["run_number"].(int64)
			break
		}
	}
	var lfn string
	for _, v := range rec["file"].([]interface{}) {
		r := v.(mongo.DASRecord)
		if r != nil {
			lfn = r["name"].(string)
			break
		}
	}
	var lumis []int64
	for _, v := range rec["lumi"].([]interface{}) {
		r := v.(mongo.DASRecord)
		if r != nil {
			for _, lumi := range r["number"].([]interface{}) {
				lumis = append(lumis, lumi.(int64))
			}
		}
	}
	var events []int64
	if _, ok := rec["events"]; ok {
		for _, v := range rec["events"].([]interface{}) {
			r := v.(mongo.DASRecord)
			evts := r["number"]
			if evts != nil {
				for _, lumi := range evts.([]interface{}) {
					events = append(events, lumi.(int64))
				}
			}
		}
	}
	lfnArr := strings.Split(lfn, "/")
	lfnTag := strings.Replace(lfnArr[len(lfnArr)-1], ".root", "", 1)
	lumiTag := fmt.Sprintf("%v", lumis)
	tag := fmt.Sprintf("id_%s_%d_%s", lfnTag, run, lumiTag)
	link := fmt.Sprintf("link_%s_%d_%s", lfnTag, run, lumiTag)
	var rows []string
	var totEvents int64
	ev := make(map[int64]int64)
	for idx, lumi := range lumis {
		if len(lumis) == len(events) {
			ev[lumi] = events[idx]
		} else {
			ev[lumi] = -1
		}
	}
	sort.Sort(utils.Int64List(lumis))
	for idx, lumi := range lumis {
		var row string
		evt := ev[lumi]
		if evt > -1 {
			row = fmt.Sprintf("Lumi: %d, Events %d", lumi, evt)
			totEvents += events[idx]
		} else {
			row = fmt.Sprintf("Lumi: %d, Events None", lumi)
		}
		rows = append(rows, row)
	}
	out := fmt.Sprintf("&nbsp;<em>lumis/events pairs</em> ")
	out += fmt.Sprintf("<a href=\"javascript:ToggleTag('%s', '%s')\" id=\"%s\">show</a>", tag, link, link)
	if totEvents > 0 {
		out += fmt.Sprintf("&nbsp; Total events=%d", totEvents)
	}
	out += fmt.Sprintf("<div class=\"hide\" id=\"%s\" name=\"%s\">", tag, tag)
	out += strings.Join(rows, "<br/>\n")
	out += fmt.Sprintf("</div>")
	return out
}

// helper function to check dataset patterns and return user-based message
func datasetPattern(q string) string {
	if strings.Contains(q, "dataset=") && strings.Contains(q, "*") && !strings.Contains(q, "status") {
		msg := fmt.Sprintf("By default DAS shows dataset with <b>VALID</b> status. ")
		msg += fmt.Sprintf("To query datasets regardless of their status please use")
		msg += fmt.Sprintf("<div class=\"example\">dataset status=* %s</div>", q)
		return fmt.Sprintf("<div>%s</div>", msg)
	}
	return ""
}

// PresentDataPlain represents DAS records for web UI
func PresentDataPlain(path string, dasquery dasql.DASQuery, data []mongo.DASRecord) string {
	var pkey, out string
	var dasrec mongo.DASRecord
	for _, item := range data {
		dasrec = item["das"].(mongo.DASRecord)
		pkey = dasrec["primary_key"].(string)
		val := ExtractValue(item, pkey)
		vals := strings.Split(val, ",")
		if len(vals) > 1 {
			val = vals[0]
		}
		if out == "" {
			out = fmt.Sprintf("%v", val)
		} else {
			out = fmt.Sprintf("%s\n%v", out, val)
		}
	}
	return out
}

// helper function to parse DBSError struct
func parseDBSError(dbsErr mongo.DASRecord) string {
	var out string
	var desc, mesg bool
	if v, ok := dbsErr["reason"]; ok {
		reason := fmt.Sprintf("%v", v)
		for _, v := range strings.Split(reason, ":") {
			if desc || mesg {
				v := strings.Replace(v, " Function", "", -1)
				v = strings.Replace(v, " Message", "", -1)
				out = fmt.Sprintf("%s<br/>%s", out, v)
			}
			if strings.Contains(v, " Description") {
				desc = true
			}
			if strings.Contains(v, " Message") {
				mesg = true
			}
		}
	}
	return out
}

// PresentData represents DAS records for web UI
func PresentData(path string, dasquery dasql.DASQuery, data []mongo.DASRecord, pmap mongo.DASRecord, nres, startIdx, limit int, procTime time.Duration) string {
	var out []string
	line := "<hr class=\"line\" />"
	red := "style=\"color:red\""
	green := "style=\"color:green\""
	blue := "style=\"color:blue\""
	total := nres
	if len(dasquery.Aggregators) > 0 {
		total = len(dasquery.Aggregators)
	}
	out = append(out, pagination(path, dasquery.Query, dasquery.Instance, total, startIdx, limit))
	patMsg := datasetPattern(dasquery.Query)
	if patMsg != "" {
		out = append(out, patMsg)
	}
	//     br := "<br/>"
	fields := dasquery.Fields
	var pkey, inst string
	var dasrec mongo.DASRecord
	var services []string
	for jdx, item := range data {
		switch r := item["das"].(type) {
		case mongo.DASRecord:
			dasrec = r
		default:
			val := fmt.Sprintf("unable to cast to DASRecord, %+v", r)
			out = append(out, val)
			continue
		}
		if _, ok := dasrec["services"]; !ok {
			continue
		}
		//         dasrec = item["das"].(mongo.DASRecord)
		services = []string{}
		for _, v := range dasrec["services"].([]interface{}) {
			srv := strings.Split(v.(string), ":")[0]
			services = append(services, srv)
		}
		pkey = dasrec["primary_key"].(string)
		inst = dasrec["instance"].(string)
		// aggregator part
		if len(dasquery.Aggregators) > 0 {
			fname := item["function"].(string)
			fkey := item["key"].(string)
			res := item["result"].(mongo.DASRecord)
			var val string
			if strings.Contains(fkey, "_size") {
				val = fmt.Sprintf("%s(%s)=%v<br/>\n", fname, fkey, utils.SizeFormat(res["value"]))
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
		dtypes := make(map[string]string)
		var values []string
		for _, key := range fields {
			var records []interface{}
			switch r := item[key].(type) {
			case []interface{}:
				records = r
			case mongo.DASRecord:
				records = append(records, r)
			}
			//             records := item[key].([]interface{})
			var uiRows []interface{}
			if v, ok := pmap[key]; ok {
				switch p := v.(type) {
				case []interface{}:
					uiRows = sortUiRows(p, pkey)
				default:
					log.Printf("WARNING: unsupported type of record %+v, key=%s\n", pmap, key)
				}
			}
			for idx, elem := range records {
				if elem == nil {
					continue
				}
				rec := elem.(mongo.DASRecord)
				if v, ok := rec["error"]; ok && v != "" && key != "rules" {
					estm := fmt.Sprintf("### %v of type %T", v, v)
					//                     log.Println("### estm", estm)
					switch t := v.(type) {
					case mongo.DASRecord:
						estm = parseDBSError(t)
					}
					erec := fmt.Sprintf("<b>Error:</b> <span %s>%v</span>", red, estm)
					values = append(values, erec)
					continue
				}
				for _, uir := range uiRows {
					uirow := uir.(mongo.DASRecord)
					daskey := uirow["das"].(string)
					if links == nil {
						if uirow["link"] != nil {
							links = uirow["link"].([]interface{})
						}
					}
					if idx != 0 && daskey == pkey {
						continue // look-up only once primary key
					}
					webkey := uirow["ui"].(string)
					attr := daskey
					if strings.Contains(daskey, ".") {
						attrs := strings.Split(daskey, ".")
						attr = strings.Join(attrs[1:], ".")
					}
					if attr == "replica.site" {
						attr = "replica.node" // Phedex record contains replica.node instead of replica.site
					}
					value := ExtractValue(rec, attr)
					if daskey == "lumi.number" {
						value = joinLumis(strings.Split(value, ","))
					}
					if daskey == "config.ids" {
						var vals []string
						// get name from configIDMap
						rmap := make(map[string]string)
						if rec["ids_map"] != nil {
							r := rec["ids_map"].(mongo.DASRecord)
							for k, v := range r {
								key := fmt.Sprintf("%v", k)
								val := fmt.Sprintf("%v", v)
								rmap[key] = val
							}
						}
						for _, vvv := range strings.Split(value, ",") {
							cid := strings.Trim(vvv, " ")
							var name string
							if cname, ok := rmap[cid]; ok {
								name = cname
							}
							furl := config.Config.Frontend
							v := fmt.Sprintf("<a href=\"%s/couchdb/reqmgr_config_cache/%s/configFile\">%s</a>", furl, cid, name)
							vals = append(vals, v)
						}
						value = strings.Join(vals, ", ")
					}
					if pval == "" {
						pval = value
					}
					if pkey == "dataset.name" {
						dtypes[pval] = ExtractValue(rec, "datatype")
					}
					if len(value) > 0 {
						var row string
						/*
							if webkey == "Luminosity number" {
								value = joinLumis(strings.Split(value, ","))
							} else if webkey == "Site type" {
						*/
						if webkey == "Site type" {
							v := strings.ToLower(value)
							if v == "disk" {
								value = fmt.Sprintf("<b><span %s>DISK</span></b>", green)
							} else if strings.Contains(v, "orig") {
								value = fmt.Sprintf("<b><span %s>Original placement</span></b>", blue)
							} else {
								value = fmt.Sprintf("<b><span %s>TAPE</span> no user access</b>", red)
							}
						} else if webkey == "Status" {
							if value == "VALID" {
								value = fmt.Sprintf("<b><span %s>%s</span></b>", green, value)
							} else {
								value = fmt.Sprintf("<b><span %s>%s</span></b>", red, value)
							}
						} else if webkey == "Tag" && value == "UNKNOWN" {
							value = fmt.Sprintf("<span %s>%s</span>", red, value)
						} else if webkey == "Dataset presence" || webkey == "Block presence" || webkey == "Block completion" || webkey == "File-replica presence" {
							color := red
							if strings.HasPrefix(value, "100") {
								color = green
							}
							value = fmt.Sprintf("<b><span %s>%s</span></b>", color, value)
							webkey = tooltip(webkey)
						}
						if daskey == pkey {
							row = fmt.Sprintf("%s: %v\n<br/>\n", webkey, href(path, pkey, value, inst, dasquery.Query))
						} else {
							row = fmt.Sprintf("%s: %v\n", webkey, value)
						}
						values = append(values, row)
					}
				}
				// add blocks/files presence on site
				if v, ok := rec["nblocks"]; ok {
					if t, ok := rec["total_blocks"]; ok {
						row := fmt.Sprintf("\n<br/>\n <b>Number of blocks</b> %v/%v ", v, t)
						values = append(values, row)
					}
				}
				if v, ok := rec["nfiles"]; ok {
					if t, ok := rec["total_files"]; ok {
						row := fmt.Sprintf(" <b>number of files</b> %v/%v ", v, t)
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
		// add lumis/events pairs for queries which contains events
		if utils.InList("events", fields) {
			out = append(out, lumiEvents(item))
		}
		out = append(out, dasLinks(path, inst, pval, links))
		if pkey == "dataset.name" {
			arr := strings.Split(pval, "/")
			if len(arr) > 1 {
				primds := arr[1]
				link := fmt.Sprintf("<a href=\"https://xsecdb-xsdb-official.app.cern.ch/xsdb/?searchQuery=DAS=%s\">XSDB</a>", primds)
				out = append(out, link)
				if v, ok := dtypes[pval]; ok {
					if v == "mc" {
						link = fmt.Sprintf("<a href=\"https://cms-pdmv.cern.ch/mcm/requests?produce=%s&page=0&shown=127\">McM</a>", pval)
						out = append(out, link)
					}
				}
			}
		}
		out = append(out, colServices(services))
		out = append(out, showRecord(item))
		if jdx != len(data) {
			out = append(out, line)
		}
	}
	out = append(out, pagination(path, dasquery.Query, dasquery.Instance, total, startIdx, limit))
	if procTime.Seconds() == 0 { // look-up processing time if it is not provided
		ts := das.TimeStamp(dasquery)
		procTime = time.Now().Sub(time.Unix(ts, 0))
	}
	out = append(out, fmt.Sprintf("<div align=\"right\">processing time: %v</div>", procTime))
	return strings.Join(out, "\n")
}

// helper function to sort ui rows to have persistent view on DAS web page for ui names
func sortUiRows(uiRows []interface{}, pkey string) []interface{} {
	var out []interface{}
	urows := make(map[string]interface{})
	var keys []string
	var dasUiKey string
	for _, uir := range uiRows {
		r := uir.(mongo.DASRecord)
		k := r["ui"].(string)
		urows[k] = r
		daskey := r["das"].(string)
		if daskey != pkey {
			keys = append(keys, k)
		} else {
			dasUiKey = k
		}
	}
	sort.Sort(utils.StringList(keys))
	// place dasUiKey (primary key) first in a list such that on web page we'll show it first
	keys = append([]string{dasUiKey}, keys...)
	for _, k := range keys {
		if urows[k] != nil {
			out = append(out, urows[k])
		}
	}
	return out
}

// ExtractValue helper function to extract value from das record
func ExtractValue(data mongo.DASRecord, daskey string) string {
	var out []string
	keys := strings.Split(daskey, ".")
	count := 1
	for _, key := range keys {
		val := data[key]
		switch value := val.(type) {
		case nil:
			return ""
		case float64, int, int64, string:
			if key == "size" || key == "bytes" || key == "file_size" {
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
					value = ExtractValue(vvv, strings.Join(keys[count:], "."))
				default:
					value = fmt.Sprintf("%v", vvv)
				}
				out = append(out, value)
			}
			return strings.Join(out, ", ")
		default:
			if count != len(keys) {
				return ExtractValue(value.(mongo.DASRecord), strings.Join(keys[count:], "."))
			}
			if strings.HasSuffix(key, "urls") {
				out = append(out, urlsFormat(value))
			} else {
				out = append(out, fmt.Sprintf("%v", value))
			}
		}
		count = count + 1
	}
	return strings.Join(out, ", ")
}

// helper function to join lumi sections
func joinLumis(lumis []string) string {
	var intLumis []int
	for _, v := range lumis {
		l, _ := strconv.Atoi(strings.TrimSpace(v))
		intLumis = append(intLumis, l)
	}
	sort.Sort(utils.IntList(intLumis))
	var out []string
	flumi := 0
	clumi := 0
	for _, l := range intLumis {
		if flumi == 0 {
			flumi = l
		}
		if clumi == 0 {
			clumi = l
		}
		if l-clumi > 1 {
			out = append(out, fmt.Sprintf("[%d, %d]", flumi, clumi))
			flumi = l
		}
		clumi = l
	}
	out = append(out, fmt.Sprintf("[%d, %d]", flumi, clumi))
	return fmt.Sprintf("[%s]", strings.Join(out, ", "))
}

// helper function for tooltips
func tooltip(key string) string {
	page := ""
	tooltip := ""
	if key == "Dataset presence" {
		tooltip = key + " is a total number of files at the site divided by total number of files in a dataset"
	} else if key == "Block presence" {
		tooltip = key + " is a total number of blocks at the site divided by total number of blocks in a dataset"
	} else if key == "File-replica presence" {
		tooltip = key + " is a total number of files at the site divided by total number of files in all block at this site"
	} else if key == "Block completion" {
		tooltip = key + " is a total number of blocks fully transferred to the site divided by total number of blocks at this site"
	} else if key == "Config urls" {
		tooltip = key + " represents either config file(s) used to produced this dataset (input-config) or config file(s) used to produce other datasets using dataset in question (output-config)"
	}
	if len(tooltip) > 0 {
		page = fmt.Sprintf("<span class=\"tooltip\">%s<span class=\"classic\">%s</span></span>", key, tooltip)
	}
	return page
}

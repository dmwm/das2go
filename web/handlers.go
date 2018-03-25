package web

// das2go - DAS web server handlers
//
// Copyright (c) 2015-2017 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/das"
	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
)

// ServerSettings controls server parameters
type ServerSettings struct {
	Level        int    `json:"level"`        // verbosity level
	LogFormatter string `json:"logFormatter"` // logrus formatter
}

// DASKeys provides information about DAS keys used by ServiceHandler
type DASKeys struct {
	System string
	DKey   string
	RKey   string
}

// helper function to build system-apis mapping
func apisrows() [][]string {
	var out [][]string
	sdict := _dasmaps.SystemApis()
	for _, srv := range _dasmaps.Services() {
		if v, ok := sdict[srv]; ok {
			out = append(out, v)
		}
	}
	return out
}

// helper function to build daskey-system-record keys mapping
func keysrows() [][]string {
	var out [][]string
	var value string
	var records []DASKeys
	for _, rec := range _dasmaps.Maps() {
		rtype := rec["type"]
		if val, ok := rtype.(string); ok {
			value = val
		} else {
			continue
		}
		if value == "service" {
			dmaps := dasmaps.GetDASMaps(rec["das_map"])
			for _, dmap := range dmaps {
				dkey := dmap["das_key"].(string)
				rkey := dmap["rec_key"].(string)
				srv := rec["system"].(string)
				rec := DASKeys{System: srv, DKey: dkey, RKey: rkey}
				records = append(records, rec)
			}
		}
	}
	for _, key := range _dasmaps.DASKeys() {
		var row []string
		row = append(row, key)
		for _, srv := range _dasmaps.Services() {
			var entries []string
			for _, rec := range records {
				if key == rec.DKey {
					if srv == rec.System {
						entries = append(entries, rec.RKey)
					}
				}
			}
			if len(entries) == 0 {
				entries = append(entries, "-")
			}
			val := strings.Join(utils.List2Set(entries), ", ")
			row = append(row, val)
		}
		out = append(out, row)
	}
	return out
}

// Examples returns list of DAS query examples
func examples() []string {
	examples := []string{"block_queries.txt", "file_queries.txt", "lumi_queries.txt", "mcm_queries.txt", "run_queries.txt", "dataset_queries.txt", "jobsummary_queries.txt", "misc_queries.txt", "site_queries.txt"}
	var out []string
	for _, fname := range examples {
		arr := strings.Split(fname, "_")
		msg := fmt.Sprintf("%s queries:", arr[0])
		out = append(out, strings.ToTitle(msg))
		for _, v := range strings.Split(utils.LoadExamples(fname, config.Config.DasExamples), "\n") {
			e := fmt.Sprintf("%s", v)
			out = append(out, e)
		}
	}
	return out
}

// helper function to form DAS error used in web Handlers
func dasError(query, msg string) string {
	tmplData := make(map[string]interface{})
	tmplData["Error"] = msg
	tmplData["Query"] = query
	var templates DASTemplates
	page := templates.DASError(config.Config.Templates, tmplData)
	return _top + _search + _hiddenCards + page + _bottom
}

func processRequest(dasquery dasql.DASQuery, pid string, idx, limit int) map[string]interface{} {
	// defer function will propagate panic message to higher level
	defer utils.ErrPropagate("processRequest")

	response := make(map[string]interface{})
	if das.CheckDataReadiness(pid) { // data exists in cache and ready for retrieval
		status, data := das.GetData(dasquery, "merge", idx, limit)
		response["nresults"] = das.Count(pid)
		response["timestamp"] = das.GetTimestamp(pid)
		response["status"] = status
		response["pid"] = pid
		response["data"] = data
		logs.WithFields(logs.Fields{
			"DASQuery": dasquery,
			"PID":      pid,
			"Unix":     time.Now().Unix(),
		}).Info("ready")
	} else if das.CheckData(pid) { // data exists in cache but still processing
		response["status"] = "processing"
		response["pid"] = pid
	} else { // no data in cache (even client supplied the pid), process it
		logs.WithFields(logs.Fields{
			"DASQuery": dasquery,
			"PID":      pid,
			"Unix":     time.Now().Unix(),
		}).Info("requested")
		go das.Process(dasquery, _dasmaps)
		response["status"] = "requested"
		response["pid"] = pid
	}
	response["idx"] = idx
	response["limit"] = limit
	return response
}

// UserDN function parses user Distinguished Name (DN) from client's HTTP request
func UserDN(r *http.Request) string {
	var names []interface{}
	ndn := "No DN is provided"
	if r.TLS == nil {
		return ndn
	}
	for _, cert := range r.TLS.PeerCertificates {
		for _, name := range cert.Subject.Names {
			switch v := name.Value.(type) {
			case string:
				names = append(names, v)
			}
		}
	}
	if len(names) == 0 {
		return ndn
	}
	parts := names[:7]
	return fmt.Sprintf("/DC=%s/DC=%s/OU=%s/OU=%s/CN=%s/CN=%s/CN=%s", parts...)
}

// custom logic for CMS authentication, users may implement their own logic here
func auth(r *http.Request) bool {
	if !_auth {
		return true
	}
	userDN := UserDN(r)
	match := utils.InList(userDN, _userDNs.DNs)
	if !match {
		logs.WithFields(logs.Fields{
			"User DN": userDN,
		}).Error("Auth userDN not found in SiteDB")
	}
	return match
}

// AuthHandler authenticate incoming requests and route them to appropriate handler
func AuthHandler(w http.ResponseWriter, r *http.Request) {
	/*
		// check if server started with hkey file (auth is required)
		if config.Config.Hkey != "" {
			status := _cmsAuth.CheckAuthnAuthz(r.Header)
			if !status {
				msg := "You are not allowed to access this resource"
				http.Error(w, msg, http.StatusForbidden)
				return
			}
		}
	*/
	// check if server started with hkey file (auth is required)
	status := auth(r)
	if !status {
		msg := "You are not allowed to access this resource"
		http.Error(w, msg, http.StatusForbidden)
		return
	}
	arr := strings.Split(r.URL.Path, "/")
	path := arr[len(arr)-1]
	switch path {
	case "cli":
		CliHandler(w, r)
	case "faq":
		FAQHandler(w, r)
	case "keys":
		KeysHandler(w, r)
	case "apis":
		ApisHandler(w, r)
	case "status":
		StatusHandler(w, r)
	case "server":
		SettingsHandler(w, r)
	case "services":
		ServicesHandler(w, r)
	default:
		RequestHandler(w, r)
	}
}

// GET methods

// CliHandler hadnlers cli requests
func CliHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.WriteHeader(http.StatusOK)
	page := "Please use dasgoclient which is available in any CMSSW releases"
	w.Write([]byte(_top + page + _bottom))
}

// FAQHandler handlers FAQ requests
func FAQHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	tmplData["Operators"] = []string{"=", "between", "last", "in"}
	tmplData["Daskeys"] = []string{}
	tmplData["Aggregators"] = []string{}
	tmplData["Guide"] = templates.Guide(config.Config.Templates, tmplData)
	page := templates.FAQ(config.Config.Templates, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// KeysHandler handlers Keys requests
func KeysHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	tmplData["Keys"] = _dasmaps.DASKeys()
	tmplData["Examples"] = examples()
	page := templates.Keys(config.Config.Templates, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// ApisHandler handlers Apis requests
func ApisHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	system := r.FormValue("system")
	api := r.FormValue("api")
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	tmplData["Record"] = _dasmaps.FindApiRecord(system, api).ToHtml()
	page := templates.ApiRecord(config.Config.Templates, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// StatusHandler handlers Status requests
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// get unfinished queries
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	queries := das.ProcessingQueries()
	tmplData["Queries"] = strings.Join(queries, "\n")
	tmplData["NQueries"] = len(queries)
	tmplData["Base"] = config.Config.Base
	tmplData["NGo"] = runtime.NumGoroutine()
	page := templates.Status(config.Config.Templates, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// ServicesHandler handlers Services requests
func ServicesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	tmplData["DBSList"] = config.Config.DbsInstances
	tmplData["Systems"] = _dasmaps.Services()
	tmplData["Base"] = config.Config.Base
	tmplData["Rows"] = keysrows()
	tmplData["Apis"] = apisrows()
	page := templates.Services(config.Config.Templates, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// RequestHandler is used by web server to handle incoming requests
func RequestHandler(w http.ResponseWriter, r *http.Request) {

	if v, err := strconv.Atoi(r.FormValue("verbose")); err == nil {
		logs.Info("verbose level=%d", v)
		utils.VERBOSE = v
	}
	// Example to parse all args
	/*
		if err := r.ParseForm(); err == nil {
			for k, v := range r.Form {
				log.Println(k, v)
			}
		}
	*/
	query := r.FormValue("input")
	pid := r.FormValue("pid")
	ajax := r.FormValue("ajax")
	hash := r.FormValue("hash")
	inst := r.FormValue("instance")
	if hash != "" {
		dasquery, err := dasql.Parse(query, inst, _dasmaps.DASKeys())
		msg := fmt.Sprintf("%s, spec=%v, filters=%v, aggregators=%v, err=%s", dasquery, dasquery.Spec, dasquery.Filters, dasquery.Aggregators, err)
		w.Write([]byte(msg))
		return
	}
	limit, err := strconv.Atoi(r.FormValue("limit"))
	if err != nil {
		limit = 50
	}
	idx, err := strconv.Atoi(r.FormValue("idx"))
	if err != nil {
		idx = 0
	}
	path := r.URL.Path
	tmplData := make(map[string]interface{})

	// process requests based on the path
	base := config.Config.Base
	if path == base || path == base+"/" {
		w.Write([]byte(_top + _search + _cards + _bottom))
		return
	}
	// defer function will be fired when following processRequest will panic
	defer func() {
		if err := recover(); err != nil {
			logs.WithFields(logs.Fields{
				"Error": err,
				"Stack": utils.Stack(),
			}).Error("web server error")
			response := make(map[string]interface{})
			accept := r.Header["Accept"][0]
			if !strings.Contains(strings.ToLower(accept), "json") {
				response["Status"] = "fail"
				response["Reason"] = err
				response["PID"] = pid
				var templates DASTemplates
				msg := templates.DASRequest(config.Config.Templates, response)
				w.Write([]byte(_top + _search + _hiddenCards + msg + _bottom))
				return
			}
			response["status"] = "fail"
			response["reason"] = err
			response["pid"] = pid
			js, err := json.Marshal(&response)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
		}
	}()
	dasquery, err2 := dasql.Parse(query, inst, _dasmaps.DASKeys())
	if err2 != "" {
		w.Write([]byte(dasError(query, err2)))
		return
	}
	if pid == "" {
		pid = dasquery.Qhash
	}
	//         pid = dasquery.Qhash
	if len(pid) != 32 {
		http.Error(w, "DAS query pid is not valid", http.StatusInternalServerError)
		return
	}
	// Remove expire records from cache
	//         das.RemoveExpired(dasquery.Qhash)
	das.RemoveExpired(pid)
	// process given query
	response := processRequest(dasquery, pid, idx, limit)
	if path == base+"/cache" || path == base+"/cache/" {
		status := response["status"]
		if status != "ok" {
			w.Write([]byte(response["pid"].(string)))
			return
		}
		js, err := json.Marshal(&response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	} else if path == base+"/request" || path == base+"/request/" {
		status := response["status"]
		var page string
		if status == "ok" {
			data := response["data"].([]mongo.DASRecord)
			nres := response["nresults"].(int)
			presentationMap := _dasmaps.PresentationMap()
			page = PresentData(path, dasquery, data, presentationMap, nres, idx, limit)
		} else {
			tmplData["Base"] = config.Config.Base
			tmplData["PID"] = pid
			page = parseTmpl(config.Config.Templates, "check_pid.tmpl", tmplData)
			page += fmt.Sprintf("<script>setTimeout('ajaxCheckPid(\"%s\", \"request\", \"%s\", \"%s\", \"%s\", \"%d\")', %d)</script>", config.Config.Base, query, inst, pid, 2500, 2500)
		}
		if ajax == "" {
			w.Write([]byte(_top + _search + _hiddenCards + page + _bottom))
		} else {
			w.Write([]byte(page))
		}
	} else {
		http.Error(w, "Not implemented path", http.StatusInternalServerError)
	}
}

// POST methods

// SettingsHandler handlers Settings requests
func SettingsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var s = ServerSettings{}
	err := json.NewDecoder(r.Body).Decode(&s)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("VerboseHandler unable to marshal", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	utils.VERBOSE = s.Level
	if s.LogFormatter == "json" {
		logs.SetFormatter(&logs.JSONFormatter{})
	} else if s.LogFormatter == "text" {
		logs.SetFormatter(&logs.TextFormatter{})
	} else {
		logs.SetFormatter(&logs.TextFormatter{})
	}
	logs.WithFields(logs.Fields{
		"Verbose level": utils.VERBOSE,
		"Log formatter": s.LogFormatter,
	}).Info("update server settings")
	w.WriteHeader(http.StatusOK)
	return
}

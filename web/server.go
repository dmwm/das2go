package web

// das2go - DAS web server
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//
//
// Some links:  http://www.alexedwards.net/blog/golang-response-snippets
//              http://blog.golang.org/json-and-go
// Mgo/BSON:    https://labix.org/mgo
// Go patterns: http://www.golangpatterns.info/home
// Templates:   http://gohugo.io/templates/go-templates/
//              http://golang.org/pkg/html/template/

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/cmsauth"
	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/das"
	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
)

// import _ "net/http/pprof" is profiler, see https://golang.org/pkg/net/http/pprof/
import _ "net/http/pprof"

// global variables used in this module
var _dasmaps dasmaps.DASMaps
var _tdir, _top, _bottom, _search, _cards, _hiddenCards, _base string
var _cmsAuth cmsauth.CMSAuth
var _dbses []string

func processRequest(dasquery dasql.DASQuery, pid string, idx, limit int) map[string]interface{} {
	// defer function will propagate panic message to higher level
	defer utils.ErrPropagate("processRequest")

	log.Println("DAS WEB", dasquery, "ready", das.CheckDataReadiness(pid))
	response := make(map[string]interface{})
	if das.CheckDataReadiness(pid) { // data exists in cache and ready for retrieval
		status, data := das.GetData(dasquery, "merge", idx, limit)
		response["nresults"] = das.Count(pid)
		response["timestamp"] = das.GetTimestamp(pid)
		response["status"] = status
		response["pid"] = pid
		response["data"] = data
	} else if das.CheckData(pid) { // data exists in cache but still processing
		response["status"] = "processing"
		response["pid"] = pid
	} else { // no data in cache (even client supplied the pid), process it
		if utils.VERBOSE > 1 {
			log.Println("DAS QUERY spec:", dasquery.Spec, "fields:", dasquery.Fields, "pipe:", dasquery.Pipe, "aggregators:", dasquery.Aggregators, "instance:", dasquery.Instance)
		}
		go das.Process(dasquery, _dasmaps)
		response["status"] = "requested"
		response["pid"] = pid
	}
	response["idx"] = idx
	response["limit"] = limit
	return response
}

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
	tmplData["Guide"] = templates.Guide(_tdir, tmplData)
	page := templates.FAQ(_tdir, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// Examples returns list of DAS query examples
func examples() []string {
	examples := []string{"block_queries.txt", "file_queries.txt", "lumi_queries.txt", "mcm_queries.txt", "run_queries.txt", "dataset_queries.txt", "jobsummary_queries.txt", "misc_queries.txt", "site_queries.txt"}
	var out []string
	for _, fname := range examples {
		arr := strings.Split(fname, "_")
		msg := fmt.Sprintf("%s queries:", arr[0])
		out = append(out, strings.ToTitle(msg))
		for _, v := range strings.Split(utils.LoadExamples(fname), "\n") {
			e := fmt.Sprintf("%s", v)
			out = append(out, e)
		}
	}
	return out
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
	page := templates.Keys(_tdir, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// DBSDescription holds description of DBS instance
type DBSDescription struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// ServicesHandler handlers Services requests
func ServicesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var dbsList []DBSDescription
	for _, v := range _dbses {
		s := "physics instance"
		if strings.Contains(v, "global") {
			s = "global instance"
		}
		dbsList = append(dbsList, DBSDescription{Name: v, Description: s})
	}
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	tmplData["DBSList"] = dbsList
	page := templates.Services(_tdir, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// RequestHandler is used by web server to handle incoming requests
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	// check if DAS server started with hkey file (auth is required)
	status := _cmsAuth.CheckAuthnAuthz(r.Header)
	if !status {
		msg := "You are not allowed to access this resource"
		http.Error(w, msg, http.StatusForbidden)
		return
	}

	if v, err := strconv.Atoi(r.FormValue("verbose")); err == nil {
		log.Printf("DAS VERBOSE level=%d", v)
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
		limit = 10
	}
	idx, err := strconv.Atoi(r.FormValue("idx"))
	if err != nil {
		idx = 0
	}
	path := r.URL.Path
	tmplData := make(map[string]interface{})

	// process requests based on the path
	if path == "/das" || path == "/das/" {
		w.Write([]byte(_top + _search + _cards + _bottom))
		return
	}
	// defer function will be fired when following processRequest will panic
	defer func() {
		if err := recover(); err != nil {
			log.Println("DAS ERROR, web server error", err, utils.Stack())
			response := make(map[string]interface{})
			accept := r.Header["Accept"][0]
			if !strings.Contains(strings.ToLower(accept), "json") {
				response["Status"] = "fail"
				response["Reason"] = err
				response["PID"] = pid
				var templates DASTemplates
				errTmp := templates.DASError(_tdir, response)
				w.Write([]byte(_top + _search + _hiddenCards + errTmp + _bottom))
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
		panic(err2)
	}
	if pid == "" {
		pid = dasquery.Qhash
	}
	//         pid = dasquery.Qhash
	if len(pid) != 32 {
		http.Error(w, "DAS query pid is not valid", http.StatusInternalServerError)
	}
	// Remove expire records from cache
	//         das.RemoveExpired(dasquery.Qhash)
	das.RemoveExpired(pid)
	// process given query
	response := processRequest(dasquery, pid, idx, limit)
	if path == "/das/cache" || path == "/das/cache/" {
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
	} else if path == "/das/request" || path == "/das/request/" {
		status := response["status"]
		var page string
		if status == "ok" {
			data := response["data"].([]mongo.DASRecord)
			nres := response["nresults"].(int)
			presentationMap := _dasmaps.PresentationMap()
			page = PresentData(path, dasquery, data, presentationMap, nres, idx, limit)
		} else {
			tmplData["Base"] = _base
			tmplData["PID"] = pid
			page = parseTmpl(_tdir, "check_pid.tmpl", tmplData)
			page += fmt.Sprintf("<script>setTimeout('ajaxCheckPid(\"%s\", \"request\", \"%s\", \"%s\", \"%s\", \"%d\")', %d)</script>", _base, query, inst, pid, 2500, 2500)
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

// Server is proxy server. It defines /fetch public interface
func Server(port, afile string) {
	log.Printf("Start server localhost:%s", port)
	var tcss, tjs, timg, tyui string
	for _, item := range os.Environ() {
		val := strings.Split(item, "=")
		if val[0] == "YUI_ROOT" {
			tyui = val[1]
		} else if val[0] == "DAS_TMPLPATH" {
			_tdir = val[1]
		} else if val[0] == "DAS_JSPATH" {
			tjs = val[1]
		} else if val[0] == "DAS_CSSPATH" {
			tcss = val[1]
		} else if val[0] == "DAS_IMAGESPATH" {
			timg = val[1]
		}
	}
	// init CMS Authentication module
	_cmsAuth.Init(afile)

	// DAS templates
	_base = "/das"
	_dbses = []string{"prod/global", "prod/phys01", "prod/phys02", "prod/phys03", "prod/caf"}
	tmplData := make(map[string]interface{})
	tmplData["Base"] = _base
	tmplData["Time"] = time.Now()
	tmplData["Input"] = ""
	tmplData["DBSinstance"] = _dbses[0]
	tmplData["Views"] = []string{"list", "plain", "table", "json", "xml"}
	tmplData["DBSes"] = _dbses
	tmplData["CardClass"] = "show"
	tmplData["Version"] = utils.VERSION
	var templates DASTemplates
	_top = templates.Top(_tdir, tmplData)
	_bottom = templates.Bottom(_tdir, tmplData)
	_search = templates.SearchForm(_tdir, tmplData)
	_cards = templates.Cards(_tdir, tmplData)
	tmplData["CardClass"] = "hide"
	_hiddenCards = templates.Cards(_tdir, tmplData)

	// load DAS Maps if necessary
	if len(_dasmaps.Services()) == 0 {
		log.Println("Load DAS maps")
		_dasmaps.LoadMaps("mapping", "db")
		services := config.Services()
		if len(services) > 0 {
			_dasmaps.AssignServices(services)
		}
		log.Println("DAS services", _dasmaps.Services())
		log.Println("DAS keys", _dasmaps.DASKeys())
	}

	// create all required indexes in das.cache, das.merge collections
	indexes := []string{"qhash", "das.expire", "das.record", "dataset.name", "file.name"}
	mongo.CreateIndexes("das", "cache", indexes)
	mongo.CreateIndexes("das", "merge", indexes)

	// assign handlers
	http.Handle("/das/css/", http.StripPrefix("/das/css/", http.FileServer(http.Dir(tcss))))
	http.Handle("/das/js/", http.StripPrefix("/das/js/", http.FileServer(http.Dir(tjs))))
	http.Handle("/das/images/", http.StripPrefix("/das/images/", http.FileServer(http.Dir(timg))))
	http.Handle("/das/yui/", http.StripPrefix("/das/yui/", http.FileServer(http.Dir(tyui))))
	http.HandleFunc("/das/cli", CliHandler)
	http.HandleFunc("/das/faq", FAQHandler)
	http.HandleFunc("/das/keys", KeysHandler)
	http.HandleFunc("/das/services", ServicesHandler)
	http.HandleFunc("/das/", RequestHandler)
	http.HandleFunc("/das", RequestHandler)
	err := http.ListenAndServe(":"+port, nil)
	// NOTE: later this can be replaced with secure connection
	// replace ListenAndServe(addr string, handler Handler)
	// with TLS function
	// ListenAndServeTLS(addr string, certFile string, keyFile string, handler
	// Handler)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

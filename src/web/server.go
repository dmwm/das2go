/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS web server, it handles all DAS reuqests
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 * Some links:  http://www.alexedwards.net/blog/golang-response-snippets
 *              http://blog.golang.org/json-and-go
 * Mgo/BSON:    https://labix.org/mgo
 * Go patterns: http://www.golangpatterns.info/home
 * Templates:   http://gohugo.io/templates/go-templates/
 *              http://golang.org/pkg/html/template/
 */
package web

import (
	"das"
	"dasmaps"
	"dasql"
	"encoding/json"
	"fmt"
	"log"
	"mongo"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// profiler
import _ "net/http/pprof"

// global variables used in this module
var _dasmaps dasmaps.DASMaps
var _tdir, _top, _bottom, _search, _cards, _base string
var _dbses []string

func processRequest(dasquery dasql.DASQuery, pid string, idx, limit int) map[string]interface{} {
	//     log.Println("DAS WEB", dasquery, "FIELDS", dasquery.Fields, "SPEC", dasquery.Spec, "FILTERS", dasquery.Filters, "AGGRS", dasquery.Aggregators)
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
		qhash := das.Process(dasquery, _dasmaps)
		response["status"] = "requested"
		response["pid"] = qhash
	}
	response["idx"] = idx
	response["limit"] = limit
	return response
}

/*
 * RequestHandler is used by web server to handle incoming requests
 */
func RequestHandler(w http.ResponseWriter, r *http.Request) {
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
	//     log.Println("CALL", path, query, pid)
	tmplData := make(map[string]interface{})

	// process requests based on the path
	if path == "/das" || path == "/das/" {
		w.Write([]byte(_top + _search + _cards + _bottom))
		return
	} else {
		dasquery, err := dasql.Parse(query, inst, _dasmaps.DASKeys())
		if err != "" {
			w.Write([]byte(err))
		}
		if pid == "" {
			pid = dasquery.Qhash
		}
		if len(pid) != 32 {
			http.Error(w, "DAS query pid is not valid", http.StatusInternalServerError)
		}
		// Remove expire records from cache
		das.RemoveExpired(dasquery.Qhash)
		// process given query
		response := processRequest(dasquery, pid, idx, limit)
		if path == "/das/cache" {
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
		} else if path == "/das/request" {
			status := response["status"]
			//             log.Println("RESPONSE", response)
			var page string
			if status == "ok" {
				data := response["data"].([]mongo.DASRecord)
				presentationMap := _dasmaps.PresentationMap()
				page = PresentData(path, dasquery, data, presentationMap)
			} else {
				tmplData["Base"] = _base
				tmplData["PID"] = pid
				tmplData["Input"] = query
				tmplData["Interval"] = 2500
				tmplData["Method"] = "request"
				page = parseTmpl(_tdir, "check_pid.tmpl", tmplData)
			}
			if ajax == "" {
				w.Write([]byte(_top + _search + _cards + page + _bottom))
			} else {
				w.Write([]byte(page))
			}
		} else {
			//         t, _ := template.ParseFiles("src/templates/error.html")
			//         t.Execute(w, nil)
			http.Error(w, "Not implemented path", http.StatusInternalServerError)
		}
	}
}

// proxy server. It defines /fetch public interface
func Server(port string) {
	log.Printf("Start server localhost:%s/das", port)
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
	var templates DASTemplates
	_top = templates.Top(_tdir, tmplData)
	_bottom = templates.Bottom(_tdir, tmplData)
	_search = templates.SearchForm(_tdir, tmplData)
	_cards = templates.Cards(_tdir, tmplData)

	// load DAS Maps if neccessary
	if len(_dasmaps.Services()) == 0 {
		log.Println("Load DAS maps")
		_dasmaps.LoadMaps("mapping", "db")
		log.Println("DAS services", _dasmaps.Services())
		log.Println("DAS keys", _dasmaps.DASKeys())
	}

	// create all required indexes in das.cache, das.merge collections
	indexes := []string{"qhash", "das.expire", "das.record"}
	mongo.CreateIndexes("das", "cache", indexes)
	mongo.CreateIndexes("das", "merge", indexes)

	// assign handlers
	http.Handle("/das/css/", http.StripPrefix("/das/css/", http.FileServer(http.Dir(tcss))))
	http.Handle("/das/js/", http.StripPrefix("/das/js/", http.FileServer(http.Dir(tjs))))
	http.Handle("/das/images/", http.StripPrefix("/das/images/", http.FileServer(http.Dir(timg))))
	http.Handle("/das/yui/", http.StripPrefix("/das/yui/", http.FileServer(http.Dir(tyui))))
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

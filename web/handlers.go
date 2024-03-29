package web

// das2go - DAS web server handlers
//
// Copyright (c) 2015-2017 - Valentin Kuznetsov <vkuznet AT gmail dot com>

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/das"
	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	"github.com/prometheus/procfs"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
)

// TotalGetRequests counts total number of GET requests received by the server
var TotalGetRequests uint64

// TotalPostRequests counts total number of POST requests received by the server
var TotalPostRequests uint64

// ServerSettings controls server parameters
type ServerSettings struct {
	Level          int    `json:"level"`          // verbosity level
	RucioTokenCurl bool   `json:"rucioTokenCurl"` // use curl method to obtain Rucio Token
	ProfileFile    string `json:"profileFile"`    // send profile data to a given file
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
func dasError(query, msg, posLine string) string {
	tmplData := make(map[string]interface{})
	tmplData["Error"] = msg
	tmplData["Query"] = query
	tmplData["PositionLine"] = posLine
	var templates DASTemplates
	page := templates.DASError(config.Config.Templates, tmplData)
	return _top + _search + _hiddenCards + page + _bottom
}

// helper function to form no results response
func dasZero(base string) string {
	tmplData := make(map[string]interface{})
	tmplData["Base"] = base
	var templates DASTemplates
	page := templates.DASZeroResults(config.Config.Templates, tmplData)
	return page
}

func processRequest(dasquery dasql.DASQuery, pid string, idx, limit int) map[string]interface{} {
	// defer function will propagate error message to higher level
	defer utils.ErrPropagate("processRequest")

	// defer function profiler
	defer utils.MeasureTime("web/handlers/processRequest")()

	response := make(map[string]interface{})
	if das.CheckDataReadiness(pid) { // data exists in cache and ready for retrieval
		status, data := das.GetData(dasquery, "merge", idx, limit)
		ts := das.TimeStamp(dasquery)
		procTime := time.Now().Sub(time.Unix(ts, 0))
		nrec := das.Count(pid)
		size := das.Bytes(pid)
		response["bytes"] = size
		response["nresults"] = nrec
		response["timestamp"] = das.GetTimestamp(pid)
		response["status"] = status
		response["pid"] = pid
		response["data"] = data
		response["procTime"] = procTime
		log.Printf("%v pid=%v status=%v nrecords=%d idx=%v limit=%v bytes=%v processing_time=%v\n", dasquery, pid, status, nrec, idx, limit, size, procTime)
	} else if das.CheckData(pid) { // data exists in cache but still processing
		response["status"] = "processing"
		response["pid"] = pid
	} else { // no data in cache (even client supplied the pid), process it
		log.Printf("%v pid=%v\n", dasquery, pid)
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
		log.Printf("ERROR: user DN %s not found in Cric DNs records\n", userDN)
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
	// increment GET/POST counters
	if r.Method == "GET" {
		atomic.AddUint64(&TotalGetRequests, 1)
	}
	if r.Method == "POST" {
		atomic.AddUint64(&TotalPostRequests, 1)
	}

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
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	page := templates.CLI(config.Config.Templates, tmplData)
	w.WriteHeader(http.StatusOK)
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
	tmplData["Base"] = config.Config.Base
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

// Memory structure keeps track of server memory
type Memory struct {
	Total       uint64  `json:"total"`
	Free        uint64  `json:"free"`
	Used        uint64  `json:"used"`
	UsedPercent float64 `json:"usedPercent"`
}

// Mem structure keeps track of virtual/swap memory of the server
type Mem struct {
	Virtual Memory
	Swap    Memory
}

// MemStats represents runtime memory stats
type MemStats struct {
	Sys        uint64 // is the total bytes of memory obtained from the OS.
	Alloc      uint64 // is bytes of allocated heap objects.
	TotalAlloc uint64 // is cumulative bytes allocated for heap objects.
	HeapSys    uint64 // is bytes of heap memory obtained from the OS.
	HeapInuse  uint64 // is bytes in in-use spans
	StackSys   uint64 // is bytes of stack memory obtained from the OS
	StackInuse uint64 // is bytes in stack spans
	GCSys      uint64 // is bytes of memory in garbage collection metadata.
}

// StatusHandler handlers Status requests
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// check HTTP header
	var accept, content string
	if _, ok := r.Header["Accept"]; ok {
		accept = r.Header["Accept"][0]
	}
	if _, ok := r.Header["Content-Type"]; ok {
		content = r.Header["Content-Type"][0]
	}

	// get cpu and mem profiles
	m, _ := mem.VirtualMemory()
	s, _ := mem.SwapMemory()
	l, _ := load.Avg()
	c, _ := cpu.Percent(time.Millisecond, true)
	process, perr := process.NewProcess(int32(os.Getpid()))
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)

	// get unfinished queries
	var templates DASTemplates
	tmplData := make(map[string]interface{})
	queries := das.ProcessingQueries()
	tmplData["Queries"] = strings.Join(queries, "\n")
	tmplData["NQueries"] = len(queries)
	tmplData["Base"] = config.Config.Base
	tmplData["NGo"] = runtime.NumGoroutine()
	virt := Memory{Total: m.Total, Free: m.Free, Used: m.Used, UsedPercent: m.UsedPercent}
	swap := Memory{Total: s.Total, Free: s.Free, Used: s.Used, UsedPercent: s.UsedPercent}
	tmplData["Memory"] = Mem{Virtual: virt, Swap: swap}
	tmplData["Load"] = l
	tmplData["CPU"] = c
	tmplData["MemStats"] = MemStats{Sys: memstats.Sys, Alloc: memstats.Alloc, TotalAlloc: memstats.TotalAlloc, StackSys: memstats.StackSys, HeapSys: memstats.HeapSys, GCSys: memstats.GCSys, StackInuse: memstats.StackInuse, HeapInuse: memstats.HeapInuse}
	var cpuTotal, vsize, rss, openFDs, maxFDs, maxVsize float64
	if proc, err := procfs.NewProc(os.Getpid()); err == nil {
		if stat, err := proc.Stat(); err == nil {
			// CPUTime returns the total CPU user and system time in seconds.
			cpuTotal = float64(stat.CPUTime())
			vsize = float64(stat.VirtualMemory())
			rss = float64(stat.ResidentMemory())
		}
		if fds, err := proc.FileDescriptorsLen(); err == nil {
			openFDs = float64(fds)
		}
		if limits, err := proc.NewLimits(); err == nil {
			maxFDs = float64(limits.OpenFiles)
			maxVsize = float64(limits.AddressSpace)
		}
	} else {
		log.Println("unable to get procfs info", err)
	}
	tmplData["CPUTotal"] = cpuTotal
	tmplData["VSize"] = vsize
	tmplData["RSS"] = rss
	tmplData["OpenFDs"] = openFDs
	tmplData["MaxFDs"] = maxFDs
	tmplData["MaxVSize"] = maxVsize
	if perr == nil { // if we got process info
		conn, err := process.Connections()
		if err == nil {
			tmplData["Connections"] = conn
		}
		openFiles, err := process.OpenFiles()
		if err == nil {
			tmplData["OpenFiles"] = openFiles
		}
	}
	tmplData["Uptime"] = time.Since(Time0).Seconds()
	tmplData["getRequests"] = TotalGetRequests
	tmplData["postRequests"] = TotalPostRequests
	tmplData["getCalls"] = utils.TotalGetCalls
	tmplData["postCalls"] = utils.TotalPostCalls
	page := templates.Status(config.Config.Templates, tmplData)
	if strings.Contains(accept, "json") || strings.Contains(content, "json") {
		data, err := json.Marshal(tmplData)
		if err != nil {
			w.Write([]byte(fmt.Sprintf("unable to marshal data, error=%v", err)))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
		return
	}
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
	tmplData["Frontend"] = config.Config.Frontend
	page := templates.Services(config.Config.Templates, tmplData)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_top + page + _bottom))
}

// RequestHandler is used by web server to handle incoming requests
func RequestHandler(w http.ResponseWriter, r *http.Request) {

	// defer function profiler
	defer utils.MeasureTime("web/handlers/RequestHandler")()

	if v, err := strconv.Atoi(r.FormValue("verbose")); err == nil {
		log.Println("verbose level", v)
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
	// use template.HTMLEscapeString() to prevent from XSS atacks
	query := template.HTMLEscapeString(r.FormValue("input"))
	pid := template.HTMLEscapeString(r.FormValue("pid"))
	ajax := template.HTMLEscapeString(r.FormValue("ajax"))
	hash := template.HTMLEscapeString(r.FormValue("hash"))
	view := template.HTMLEscapeString(r.FormValue("view"))
	inst := template.HTMLEscapeString(r.FormValue("instance"))
	if inst == "" {
		inst = _dasmaps.DBSInstance()
		if inst == "" && len(config.Config.DbsInstances) > 0 { // case of dbs2go
			inst = config.Config.DbsInstances[0]
		}
	}
	if hash != "" {
		dasquery, err, _ := dasql.Parse(query, inst, _dasmaps.DASKeys())
		log.Printf("input=\"%s\" %s", query, dasquery)
		msg := fmt.Sprintf("%s spec=%v filters=%v aggregators=%v err=%s", dasquery, dasquery.Spec, dasquery.Filters, dasquery.Aggregators, err)
		w.Write([]byte(msg))
		return
	}
	limit, err := strconv.Atoi(r.FormValue("limit"))
	if err != nil {
		limit = 50
	}
	if view == "plain" {
		limit = -1 // always look-up all data for plain view
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
	// defer function will be fired when following processRequest will exit with error
	defer func() {
		if err := recover(); err != nil {
			log.Printf("ERROR: web server stack %v, error %v\n", utils.Stack(), err)
			response := make(map[string]interface{})
			accept := r.Header["Accept"][0]
			if !strings.Contains(strings.ToLower(accept), "json") {
				response["Status"] = "fail"
				response["Reason"] = err
				response["PID"] = pid
				var templates DASTemplates
				msg := templates.DASRequest(config.Config.Templates, response)
				//                 w.Write([]byte(_top + _search + _hiddenCards + msg + _bottom))
				w.Write([]byte(msg))
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
	dasquery, err2, pLine := dasql.Parse(query, inst, _dasmaps.DASKeys())
	log.Printf("input=\"%s\" %s", query, dasquery)
	if err2 != "" {
		w.Write([]byte(dasError(query, err2, pLine)))
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
		//         status := response["status"]
		//         if status != "ok" {
		//             w.Write([]byte(response["pid"].(string)))
		//             return
		//         }
		//         js, err := json.Marshal(&response)
		//         if err != nil {
		//             http.Error(w, err.Error(), http.StatusInternalServerError)
		//             return
		//         }
		//         w.Header().Set("Content-Type", "application/json")
		//         w.Write(js)
		msg := "DAS web server no longer support python clients, please switch to dasgoclient"
		http.Error(w, msg, http.StatusInternalServerError)
	} else if path == base+"/request" || path == base+"/request/" {
		status := response["status"]
		var procTime time.Duration
		if response["procTime"] != nil {
			procTime = response["procTime"].(time.Duration)
		}
		var page string
		if status == "ok" {
			data := response["data"].([]mongo.DASRecord)
			if view == "plain" {
				page = PresentDataPlain(path, dasquery, data)
				w.Write([]byte(page))
				return
			}
			nres := response["nresults"].(int)
			if nres == 0 {
				page = dasZero(config.Config.Base)
			} else {
				presentationMap := _dasmaps.PresentationMap()
				page = PresentData(path, dasquery, data, presentationMap, nres, idx, limit, procTime)
			}
		} else {
			tmplData["Base"] = config.Config.Base
			tmplData["PID"] = pid
			page = parseTmpl(config.Config.Templates, "check_pid.tmpl", tmplData)
			page += fmt.Sprintf("<script>setTimeout('ajaxCheckPid(\"%s\", \"request\", \"%s\", \"%s\", \"%s\", \"%s\", \"%d\")', %d)</script>", config.Config.Base, query, inst, pid, view, 2500, 2500)
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
		log.Println("ERROR: VerboseHandler unable to marshal", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	utils.VERBOSE = s.Level
	// change function profiler if necessary
	if s.ProfileFile != "" {
		utils.InitFunctionProfiler(s.ProfileFile)
	} else {
		utils.Profiler = nil
	}
	// change RucioTokenCurl with whatever is supplied in server settings POST request
	utils.RucioTokenCurl = s.RucioTokenCurl
	log.Printf("Set, verbose %v, rucio %v, profile %v\n", utils.VERBOSE, s.RucioTokenCurl, s.ProfileFile)
	w.WriteHeader(http.StatusOK)
	return
}

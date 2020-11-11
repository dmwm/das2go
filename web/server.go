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
	"crypto/tls"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/dmwm/cmsauth"
	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/services"
	"github.com/dmwm/das2go/utils"

	_ "expvar"         // to be used for monitoring, see https://github.com/divan/expvarmon
	_ "net/http/pprof" // profiler, see https://golang.org/pkg/net/http/pprof/

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
)

// Config describes DAS server configuration
// global variables used in this module
var _dasmaps dasmaps.DASMaps
var _top, _bottom, _search, _cards, _hiddenCards string
var _cmsAuth cmsauth.CMSAuth
var _auth bool

// Time0 represents initial time when we started the server
var Time0 time.Time

// UserDNs structure holds information about user DNs
type UserDNs struct {
	DNs  []string
	Time time.Time
}

// global variable which we initialize once
var _userDNs UserDNs

// helper function to get userDNs from Cric service
func userDNs() []string {
	var out []string
	var verbose bool
	if utils.VERBOSE > 0 {
		verbose = true
	}
	cricUrl := fmt.Sprintf("%s?json&preset=roles", services.CricUrl(""))
	cricRecords, err := cmsauth.GetCricData(cricUrl, verbose)
	if err != nil {
		log.Println("ERROR: unable to obtain cric records, error", err)
		return out
	}
	// convert cric records to list of DNs
	for _, rec := range cricRecords {
		for _, dn := range rec.DNs {
			out = append(out, dn)
		}
	}
	log.Printf("get %d cric DNs\n", len(out))
	return out
}

// helper function to get DAS keys description
func daskeysDescription() string {
	tmplData := make(map[string]interface{})
	tmplData["daskeys"] = _dasmaps.DASKeysMaps()
	var templates DASTemplates
	desc := templates.DasKeys(config.Config.Templates, tmplData)
	return desc
}

// helper function to produce UTC time prefixed output
func utcMsg(data []byte) string {
	//     return fmt.Sprintf("[" + time.Now().String() + "] " + string(data))
	s := string(data)
	v, e := url.QueryUnescape(s)
	if e == nil {
		return v
	}
	return s
}

// custom rotate logger
type rotateLogWriter struct {
	RotateLogs *rotatelogs.RotateLogs
}

func (w rotateLogWriter) Write(data []byte) (int, error) {
	return w.RotateLogs.Write([]byte(utcMsg(data)))
}

// Server is proxy server. It defines /fetch public interface
func Server(configFile string) {
	err := config.ParseConfig(configFile)
	if config.Config.LogFile != "" {
		logName := config.Config.LogFile + "-%Y%m%d"
		hostname, err := os.Hostname()
		if err == nil {
			logName = config.Config.LogFile + "-" + hostname + "-%Y%m%d"
		}
		rl, err := rotatelogs.New(logName)
		if err == nil {
			rotlogs := rotateLogWriter{RotateLogs: rl}
			log.SetOutput(rotlogs)
			log.SetFlags(log.LstdFlags | log.Lshortfile)
		} else {
			log.SetFlags(log.LstdFlags | log.Lshortfile)
		}
	} else {
		// log time, filename, and line number
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}
	if err != nil {
		log.Println("ERROR: unabel to parse config file", configFile)
	}

	utils.VERBOSE = config.Config.Verbose
	utils.UrlQueueLimit = config.Config.UrlQueueLimit
	utils.UrlRetry = config.Config.UrlRetry
	utils.DASMAPS = config.Config.DasMaps
	utils.TIMEOUT = config.Config.Timeout
	interval := time.Duration(config.Config.TLSCertsRenewInterval)
	utils.TLSCertsRenewInterval = time.Duration(interval * time.Second)
	utils.RucioTokenCurl = config.Config.RucioTokenCurl
	log.Println(config.Config.String())
	// init CMS Authentication module
	if config.Config.Hkey != "" {
		_cmsAuth.Init(config.Config.Hkey)
	}
	// enable function profiler
	if config.Config.ProfileFile != "" {
		utils.InitFunctionProfiler(config.Config.ProfileFile)
	}
	// enable DNS resolver
	if config.Config.UseDNSCache {
		utils.UseDNSCache = true
		log.Println("UseDNSCache", utils.UseDNSCache)
	}

	// load DAS Maps if necessary
	if len(_dasmaps.Services()) == 0 {
		log.Println("Load DAS maps")
		_dasmaps.LoadMaps("mapping", "db")
		if len(config.Config.Services) > 0 {
			_dasmaps.AssignServices(config.Config.Services)
		}
		log.Println("DAS services ", _dasmaps.Services())
		log.Println("DAS keys ", _dasmaps.DASKeys())
	}
	// list URLs we're going to use
	log.Println("DBSUrl: ", services.DBSUrl(config.Config.DbsInstances[0]))
	log.Println("PhedexUrl: ", services.PhedexUrl())
	log.Println("SitedbUrl: ", services.SitedbUrl())
	log.Println("CricUrl w/ site API: ", services.CricUrl("site"))
	log.Println("RucioUrl: ", services.RucioUrl())
	log.Println("RucioAuthUrl: ", utils.RucioAuth.Url())

	// DAS templates
	tmplData := make(map[string]interface{})
	tmplData["Base"] = config.Config.Base
	tmplData["Time"] = time.Now()
	tmplData["Input"] = ""
	tmplData["DBSinstance"] = config.Config.DbsInstances[0]
	tmplData["Views"] = []string{"list", "plain"}
	tmplData["DBSes"] = config.Config.DbsInstances
	tmplData["CardClass"] = "show"
	tmplData["Version"] = utils.VERSION
	tmplData["Daskeys"] = template.HTML(daskeysDescription())
	var templates DASTemplates
	_top = templates.Top(config.Config.Templates, tmplData)
	_bottom = templates.Bottom(config.Config.Templates, tmplData)
	_search = templates.SearchForm(config.Config.Templates, tmplData)
	_cards = templates.Cards(config.Config.Templates, tmplData)
	tmplData["CardClass"] = "hide"
	_hiddenCards = templates.Cards(config.Config.Templates, tmplData)

	// create all required indexes in das.cache, das.merge collections
	indexes := []string{"qhash", "das.expire", "das.record", "dataset.name", "file.name"}
	mongo.CreateIndexes("das", "cache", indexes)
	mongo.CreateIndexes("das", "merge", indexes)

	// assign handlers
	base := config.Config.Base
	http.Handle(base+"/css/", http.StripPrefix(base+"/css/", http.FileServer(http.Dir(config.Config.Styles))))
	http.Handle(base+"/js/", http.StripPrefix(base+"/js/", http.FileServer(http.Dir(config.Config.Jscripts))))
	http.Handle(base+"/images/", http.StripPrefix(base+"/images/", http.FileServer(http.Dir(config.Config.Images))))
	//     http.Handle(base+"/debug/pprof/", http.StripPrefix(base, http.RedirectHandler("/debug/pprof/", http.StatusTemporaryRedirect)))
	http.HandleFunc(fmt.Sprintf("%s/", config.Config.Base), AuthHandler)

	// init userDNs and update it periodically
	_auth = config.Config.AuthDN
	log.Println("enable user DN authentication", _auth)
	if _auth {
		_userDNs = UserDNs{DNs: userDNs(), Time: time.Now()}
		go func() {
			for {
				interval := config.Config.UpdateDNs
				if interval == 0 {
					interval = 60
				}
				d := time.Duration(interval) * time.Minute
				log.Println("userDNs will be updated in", d)
				time.Sleep(d) // sleep for next iteration
				_userDNs = UserDNs{DNs: userDNs(), Time: time.Now()}
			}
		}()
	}

	// start http(s) server
	Time0 = time.Now()
	addr := fmt.Sprintf(":%d", config.Config.Port)
	_, e1 := os.Stat(config.Config.ServerCrt)
	_, e2 := os.Stat(config.Config.ServerKey)
	if e1 == nil && e2 == nil {
		//start HTTPS server which require user certificates
		server := &http.Server{
			Addr: addr,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequestClientCert,
			},
		}
		log.Println("starting HTTPs server", addr)
		err = server.ListenAndServeTLS(config.Config.ServerCrt, config.Config.ServerKey)
	} else {
		// Start server without user certificates
		log.Println("starting HTTP server", addr)
		err = http.ListenAndServe(addr, nil)
	}

	if err != nil {
		log.Fatalf("LinstenAndServer: %v\n", err)
	}
}

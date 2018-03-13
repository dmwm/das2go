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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dmwm/cmsauth"
	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"

	// import _ "net/http/pprof" is profiler, see https://golang.org/pkg/net/http/pprof/

	_ "net/http/pprof"
)

// Config describes DAS server configuration
// global variables used in this module
var _dasmaps dasmaps.DASMaps
var _top, _bottom, _search, _cards, _hiddenCards string
var _cmsAuth cmsauth.CMSAuth
var _auth bool

// UserDNs structure holds information about user DNs
type UserDNs struct {
	DNs  []string
	Time time.Time
}

// global variable which we initialize once
var _userDNs UserDNs

// helper function to get userDNs from sitedb
func userDNs() []string {
	var out []string
	rurl := "https://cmsweb.cern.ch/sitedb/data/prod/people"
	resp := utils.FetchResponse(rurl, "")
	if resp.Error != nil {
		logs.WithFields(logs.Fields{
			"Error": resp.Error,
		}).Error("Unable to fetch SiteDB records", resp.Error)
		return out
	}
	var rec map[string]interface{}
	err := json.Unmarshal(resp.Data, &rec)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("Unable to unmarshal response", err)
		return out
	}
	desc := rec["desc"].(map[string]interface{})
	headers := desc["columns"].([]interface{})
	var idx int
	for i, h := range headers {
		if h.(string) == "dn" {
			idx = i
			break
		}
	}
	values := rec["result"].([]interface{})
	for _, item := range values {
		val := item.([]interface{})
		v := val[idx]
		if v != nil {
			out = append(out, v.(string))
		}
	}
	return out
}

// Server is proxy server. It defines /fetch public interface
func Server(configFile string) {
	err := config.ParseConfig(configFile)
	if err != nil {
		panic(err)
	}
	utils.VERBOSE = config.Config.Verbose
	utils.UrlQueueLimit = config.Config.UrlQueueLimit
	utils.UrlRetry = config.Config.UrlRetry
	utils.DASMAPS = config.Config.DasMaps
	logs.Info(config.Config.String())
	// init CMS Authentication module
	if config.Config.Hkey != "" {
		_cmsAuth.Init(config.Config.Hkey)
	}

	// DAS templates
	tmplData := make(map[string]interface{})
	tmplData["Base"] = config.Config.Base
	tmplData["Time"] = time.Now()
	tmplData["Input"] = ""
	tmplData["DBSinstance"] = config.Config.DbsInstances[0]
	tmplData["Views"] = []string{"list", "plain", "table", "json", "xml"}
	tmplData["DBSes"] = config.Config.DbsInstances
	tmplData["CardClass"] = "show"
	tmplData["Version"] = utils.VERSION
	var templates DASTemplates
	_top = templates.Top(config.Config.Templates, tmplData)
	_bottom = templates.Bottom(config.Config.Templates, tmplData)
	_search = templates.SearchForm(config.Config.Templates, tmplData)
	_cards = templates.Cards(config.Config.Templates, tmplData)
	tmplData["CardClass"] = "hide"
	_hiddenCards = templates.Cards(config.Config.Templates, tmplData)

	// load DAS Maps if necessary
	if len(_dasmaps.Services()) == 0 {
		logs.Info("Load DAS maps")
		_dasmaps.LoadMaps("mapping", "db")
		if len(config.Config.Services) > 0 {
			_dasmaps.AssignServices(config.Config.Services)
		}
		logs.Info("DAS services ", _dasmaps.Services())
		logs.Info("DAS keys ", _dasmaps.DASKeys())
	}

	// create all required indexes in das.cache, das.merge collections
	indexes := []string{"qhash", "das.expire", "das.record", "dataset.name", "file.name"}
	mongo.CreateIndexes("das", "cache", indexes)
	mongo.CreateIndexes("das", "merge", indexes)

	// assign handlers
	base := config.Config.Base
	http.Handle(base+"/css/", http.StripPrefix(base+"/css/", http.FileServer(http.Dir(config.Config.Styles))))
	http.Handle(base+"/js/", http.StripPrefix(base+"/js/", http.FileServer(http.Dir(config.Config.Jscripts))))
	http.Handle(base+"/images/", http.StripPrefix(base+"/images/", http.FileServer(http.Dir(config.Config.Images))))
	http.Handle(base+"/yui/", http.StripPrefix(base+"/yui/", http.FileServer(http.Dir(config.Config.YuiRoot))))
	http.HandleFunc(fmt.Sprintf("%s/", config.Config.Base), AuthHandler)

	// start http(s) server
	addr := fmt.Sprintf(":%d", config.Config.Port)
	_, e1 := os.Stat(config.Config.ServerCrt)
	_, e2 := os.Stat(config.Config.ServerKey)
	if e1 == nil && e2 == nil {
		//start HTTPS server which require user certificates
		_auth = true
		// init userDNs and update it periodically
		_userDNs = UserDNs{DNs: userDNs(), Time: time.Now()}
		go func() {
			for {
				interval := config.Config.UpdateDNs
				if interval == 0 {
					interval = 60
				}
				d := time.Duration(interval) * time.Minute
				logs.WithFields(logs.Fields{"Time": time.Now(), "Duration": d}).Info("userDNs are updated")
				time.Sleep(d) // sleep for next iteration
				_userDNs = UserDNs{DNs: userDNs(), Time: time.Now()}
			}
		}()

		server := &http.Server{
			Addr: addr,
			TLSConfig: &tls.Config{
				ClientAuth: tls.RequestClientCert,
			},
		}
		logs.WithFields(logs.Fields{"Addr": addr}).Info("Starting HTTPs server")
		err = server.ListenAndServeTLS(config.Config.ServerCrt, config.Config.ServerKey)
	} else {
		// Start server without user certificates
		_auth = false
		logs.WithFields(logs.Fields{"Addr": addr}).Info("Starting HTTP server")
		err = http.ListenAndServe(addr, nil)
	}

	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Fatal("ListenAndServe: ")
	}
}

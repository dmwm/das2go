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

// global variable which we initialize once
var _userDNs []string

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
	http.Handle("/das/css/", http.StripPrefix("/das/css/", http.FileServer(http.Dir(config.Config.Styles))))
	http.Handle("/das/js/", http.StripPrefix("/das/js/", http.FileServer(http.Dir(config.Config.Jscripts))))
	http.Handle("/das/images/", http.StripPrefix("/das/images/", http.FileServer(http.Dir(config.Config.Images))))
	http.Handle("/das/yui/", http.StripPrefix("/das/yui/", http.FileServer(http.Dir(config.Config.YuiRoot))))
	http.HandleFunc(fmt.Sprintf("%s/", config.Config.Base), AuthHandler)
	addr := fmt.Sprintf(":%d", config.Config.Port)
	if config.Config.ServerCrt != "" && config.Config.ServerKey != "" {
		// init userDNs
		_userDNs = userDNs()
		//start HTTPS server which require user certificates
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
		logs.WithFields(logs.Fields{"Addr": addr}).Info("Starting HTTP server")
		err = http.ListenAndServe(addr, nil)
	}

	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Fatal("ListenAndServe: ")
	}
}

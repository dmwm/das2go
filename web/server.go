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
	err = http.ListenAndServe(fmt.Sprintf(":%d", config.Config.Port), nil)
	// NOTE: later this can be replaced with secure connection
	// replace ListenAndServe(addr string, handler Handler)
	// with TLS function
	// ListenAndServeTLS(addr string, certFile string, keyFile string, handler
	// Handler)
	if err != nil {
		logs.Fatal("ListenAndServe: ", err)
	}
}

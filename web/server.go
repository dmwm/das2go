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
	"os"
	"strings"
	"time"

	"github.com/dmwm/cmsauth"
	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/dasmaps"
	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
)

// import _ "net/http/pprof" is profiler, see https://golang.org/pkg/net/http/pprof/
import _ "net/http/pprof"

// global variables used in this module
var _dasmaps dasmaps.DASMaps
var _tdir, _top, _bottom, _search, _cards, _hiddenCards, _base, _port string
var _cmsAuth cmsauth.CMSAuth
var _dbses []string

// Server is proxy server. It defines /fetch public interface
func Server(port, afile string) {
	_port = port
	logs.Info("server port ", port)
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
		logs.Info("Load DAS maps")
		_dasmaps.LoadMaps("mapping", "db")
		services := config.Services()
		if len(services) > 0 {
			_dasmaps.AssignServices(services)
		}
		logs.Info("DAS services ", _dasmaps.Services())
		logs.Info("DAS keys ", _dasmaps.DASKeys())
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
	http.HandleFunc(fmt.Sprintf("%s/", _base), AuthHandler)
	err := http.ListenAndServe(":"+port, nil)
	// NOTE: later this can be replaced with secure connection
	// replace ListenAndServe(addr string, handler Handler)
	// with TLS function
	// ListenAndServeTLS(addr string, certFile string, keyFile string, handler
	// Handler)
	if err != nil {
		logs.Fatal("ListenAndServe: ", err)
	}
}

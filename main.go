package main

// das2go - Go implementation of Data Aggregation System (DAS) for CMS
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/dmwm/das2go/utils"
	"github.com/dmwm/das2go/web"
)

// git version of our code
var version string

func info() string {
	goVersion := runtime.Version()
	tstamp := time.Now()
	return fmt.Sprintf("das2go git=%s go=%s date=%s", version, goVersion, tstamp)
}

func main() {
	var version bool
	flag.BoolVar(&version, "version", false, "Show version")
	var config string
	flag.StringVar(&config, "config", "dasconfig.json", "DAS server config JSON file")
	flag.Parse()
	utils.VERSION = info()
	utils.WEBSERVER = 1
	if version {
		fmt.Println("DAS version:", info())
		return
	}
	web.Server(config)
}

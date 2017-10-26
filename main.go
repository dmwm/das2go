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

func info() string {
	goVersion := runtime.Version()
	tstamp := time.Now()
	return fmt.Sprintf("git={{VERSION}} go=%s date=%s", goVersion, tstamp)
}

func main() {
	var afile string
	flag.StringVar(&afile, "afile", "", "DAS authentication key file")
	var port string
	flag.StringVar(&port, "port", "8212", "DAS server port number")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	var urlQueueLimit int
	flag.IntVar(&urlQueueLimit, "urlQueueLimit", 1000, "urlQueueLimit controls number of concurrent URL calls to remote data-services")
	var urlRetry int
	flag.IntVar(&urlRetry, "urlRetry", 3, "urlRetry controls number of retries we do with URL call")
	var version bool
	flag.BoolVar(&version, "version", false, "Show version")
	flag.Parse()
	utils.VERSION = info()
	utils.VERBOSE = verbose
	utils.UrlQueueLimit = int32(urlQueueLimit)
	utils.UrlRetry = urlRetry
	utils.WEBSERVER = 1
	if version {
		fmt.Println("DAS version:", info())
		return
	}
	web.Server(port, afile)
}

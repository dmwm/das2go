// das2go - Go implementation of Data Aggregation System (DAS) for CMS
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet@gmail.com>
//
package main

import (
	"flag"
	"utils"
	"web"
)

func main() {
	var afile string
	flag.StringVar(&afile, "afile", "", "DAS authentication key file")
	var port string
	flag.StringVar(&port, "port", "8212", "DAS server port number")
	var verbose int
	flag.IntVar(&verbose, "verbose", 0, "Verbose level, support 0,1,2")
	flag.Parse()
	utils.VERBOSE = verbose
	web.Server(port, afile)
}

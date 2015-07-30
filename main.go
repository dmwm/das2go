package main

import (
	"flag"
	"web"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "8212", "DAS server port number")
	flag.Parse()
	web.Server(port)
}

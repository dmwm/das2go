package main

import "web"
import "flag"

func main() {
	var port string
	flag.StringVar(&port, "port", "8212", "DAS server port number")
	flag.Parse()
	web.Server(port)
}

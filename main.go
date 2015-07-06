package main

import "web"
import "flag"

func main() {
	var port string
	flag.StringVar(&port, "port", "8000", "URL fetch server port number")
	flag.Parse()
	web.Server(port)
}

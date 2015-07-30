package main

import (
	"flag"
	"os"
	"path/filepath"
	"web"
)

func main() {
	var port, tdir, tcss, tjs, timg string
	flag.StringVar(&port, "port", "8212", "DAS server port number")
	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	flag.StringVar(&tdir, "tmpl-dir",
		filepath.Join(cwd, "src/templates"), "Template directory")
	flag.StringVar(&tcss, "css-dir",
		filepath.Join(cwd, "src/css"), "CSS directory")
	flag.StringVar(&tjs, "js-dir",
		filepath.Join(cwd, "src/js"), "JS directory")
	flag.StringVar(&timg, "img-dir",
		filepath.Join(cwd, "src/images"), "Image directory")
	flag.Parse()
	web.Server(port, tdir, tcss, tjs, timg)
}

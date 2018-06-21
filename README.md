das2go
======

[![Build Status](https://travis-ci.org/dmwm/das2go.svg?branch=master)](https://travis-ci.org/dmwm/das2go)
[![Go Report Card](https://goreportcard.com/badge/github.com/dmwm/das2go)](https://goreportcard.com/report/github.com/dmwm/das2go)
[![GoDoc](https://godoc.org/github.com/dmwm/das2go?status.svg)](https://godoc.org/github.com/dmwm/das2go)

Go implementation of DAS (Data Aggregation System for CMS)

### Installation & Usage

To compile the server you need a Go compiler, then perform the following:

```
# one time operation, setup your GOPATH and download the following
go get github.com/dmwm/cmsauth
go get github.com/vkuznet/x509proxy
go get gopkg.in/mgo.v2

# to build DAS server run
make
```

It will build ```das2go``` executable which you can fetch from UNIX shell.
By default it serves requests on localhost:8000,
feel free to modify code accoringly.

### Profiling DAS server
DAS server supports three ways to profile itself
- [net/http/pprof](https://golang.org/pkg/net/http/pprof/)
  - it can be done either by login to http://localhost:8217/debug/pprof/ or
  - `go tool pprof http://localhost:8217/debug/pprof/heap`
  - `go tool pprof http://localhost:8217/debug/pprof/block`
  - `go tool pprof http://localhost:8217/debug/pprof/profile`
  - `go tool pprof http://localhost:8217/debug/pprof/mutex`
- [expvar](https://github.com/divan/expvarmon)
  - compile and use expvarmon tool, e.g.
  `expvarmon -ports="8217"`
- [FlameGraph](http://brendanjryan.com/golang/profiling/2018/02/28/profiling-go-applications.html)
and [go-torch](https://github.com/uber/go-torch)
  - `go-torch -u http://localhost:8217 --seconds 10` will generate svg flame
    graph


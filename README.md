das2go
======

[![Build Status](https://travis-ci.org/vkuznet/das2go.svg?branch=master)](https://travis-ci.org/vkuznet/das2go)
[![Go Report Card](https://goreportcard.com/badge/github.com/vkuznet/das2go)](https://goreportcard.com/report/github.com/vkuznet/das2go)
[![GoDoc](https://godoc.org/github.com/vkuznet/das2go?status.svg)](https://godoc.org/github.com/vkuznet/das2go)

Go implementation of DAS (Data Aggregation System for CMS)

### Installation & Usage

To compile the server you need a Go compiler, then perform the following:

```
# one time operation, setup your GOPATH and download the following
go get github.com/vkuznet/cmsauth
go get github.com/vkuznet/x509proxy
go get gopkg.in/mgo.v2

# to build DAS server run
make
```

It will build ```das2go``` executable which you can fetch from UNIX shell.
By default it serves requests on localhost:8000,
feel free to modify code accoringly.

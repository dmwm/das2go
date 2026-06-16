das2go
======

[![Build Status](https://travis-ci.org/dmwm/das2go.svg?branch=master)](https://travis-ci.org/dmwm/das2go)
[![Go CI build](https://github.com/dmwm/das2go/actions/workflows/go-ci.yml/badge.svg)](https://github.com/dmwm/das2go/actions/workflows/go-ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/dmwm/das2go)](https://goreportcard.com/report/github.com/dmwm/das2go)
[![GoDoc](https://godoc.org/github.com/dmwm/das2go?status.svg)](https://godoc.org/github.com/dmwm/das2go)

Go implementation of DAS (Data Aggregation System for CMS)

### Site query semantics

DAS uses `site` for current data location information, as reported by
Rucio/CRIC. Examples:

```
site dataset=/WJets_matchingup_7TeV-madgraph/Summer10-START36_V10_FastSim-v3/DQM
block dataset=/AlCaP0/Run2011A-ALCARECOEcalCalEtaCalib-v4/ALCARECO site=T1_US_FNAL*
file dataset=/Cosmics/Run2010B-TkAlCosmics0T-v1/ALCARECO site=T1_US_FNAL_Buffer
```

DAS uses `origin_site` for DBS `origin_site_name`, i.e. original block
placement. The direct DBS `origin_site=` filter is supported at block
granularity with a dataset constraint:

```
origin_site dataset=/QCD_Pt_0to5_TuneZ2_7TeV_pythia6/wteo-qcd_tunez2_pt0to5_pythia_fall10_387-250136cb6ade55a0822a3f1b6b851d5a/USER instance=cms_dbs_ph_analysis_02
block dataset=/WJets_matchingup_7TeV-madgraph/Summer10-START36_V10_FastSim-v3/DQM origin_site=T2_US_Nebraska
```

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

### Adding debugging information
It is possible to change verbosity level and log type of running DAS server.
To change verbosity level please issue the following command:
```
# increase verbose level to 1
scurl -X POST -d '{"level":1}' http://localhost:8217/das/server
# set verbose level to 0
scurl -X POST -d '{"level":0}' http://localhost:8217/das/server
```
To change log type please use this command:
```
# to change log formatter to json
scurl -X POST -d '{"logFormatter":"json"}' http://localhost:8217/das/server
# to change log formatter to text
scurl -X POST -d '{"logFormatter":"text"}' http://localhost:8217/das/server
```

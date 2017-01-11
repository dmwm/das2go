package utils

// DAS utils module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//
// Some links: http://www.alexedwards.net/blog/golang-response-snippets
// http://blog.golang.org/json-and-go
// http://golang.org/pkg/html/template/
// https://labix.org/mgo

import (
	"bytes"
	"container/heap"
	"crypto/tls"
	"errors"
	"github.com/vkuznet/x509proxy"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

// Certs returns array of certificates
func Certs() (tlsCerts []tls.Certificate) {
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")
	if WEBSERVER > 0 {
		log.Println("X509_USER_PROXY", uproxy)
		log.Println("X509_USER_KEY", uckey)
		log.Println("X509_USER_CERT", ucert)
	}
	if len(uproxy) > 0 {
		// use local implementation of LoadX409KeyPair instead of tls one
		x509cert, err := x509proxy.LoadX509Proxy(uproxy)
		if err != nil {
			log.Println("Fail to parser proxy X509 certificate", err)
			return
		}
		tlsCerts = []tls.Certificate{x509cert}
	} else if len(uckey) > 0 {
		x509cert, err := tls.LoadX509KeyPair(ucert, uckey)
		if err != nil {
			log.Println("Fail to parser user X509 certificate", err)
			return
		}
		tlsCerts = []tls.Certificate{x509cert}
	} else {
		return
	}
	return
}

// HttpClient is HTTP client for urlfetch server
func HttpClient() (client *http.Client) {
	// create HTTP client
	certs := Certs()
	if WEBSERVER > 0 {
		log.Println("Number of certificates", len(certs))
	}
	if len(certs) == 0 {
		client = &http.Client{}
		return
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: certs,
			InsecureSkipVerify: true},
	}
	if WEBSERVER > 0 {
		log.Println("Create TLSClientConfig")
	}
	client = &http.Client{Transport: tr}
	return
}

// create global HTTP client and re-use it through the code
var client = HttpClient()

// ResponseType structure is what we expect to get for our URL call.
// It contains a request URL, the data chunk and possible error from remote
type ResponseType struct {
	Url   string
	Data  []byte
	Error error
}

// UrlRequest structure holds details about url request's attributes
type UrlRequest struct {
	rurl string
	args string
	out  chan<- ResponseType
	ts   int64
}

// A UrlFetchQueue implements heap.Interface and holds UrlRequests
type UrlFetchQueue []*UrlRequest

// Len provides len implemenation for UrlFetchQueue
func (q UrlFetchQueue) Len() int { return len(q) }

// Less provides Less implemenation for UrlFetchQueue
func (q UrlFetchQueue) Less(i, j int) bool { return q[i].ts < q[j].ts }

// Swap provides swap implemenation for UrlFetchQueue
func (q UrlFetchQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

// Push provides push implemenation for UrlFetchQueue
func (q *UrlFetchQueue) Push(x interface{}) {
	item := x.(*UrlRequest)
	*q = append(*q, item)
}

// Pop provides Pop implemenation for UrlFetchQueue
func (q *UrlFetchQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

var (
	UrlQueueSize      int32 // keep track of running URL requests
	UrlQueueLimit     int32 // how many URL requests we can handle at a time, 0 means no limit
	UrlRetry          int   // how many times we'll retry given url call
	UrlRequestChannel = make(chan UrlRequest)
)

func init() {
	if WEBSERVER > 0 {
		log.Println("DAS URLFetchWorker")
	}
	go URLFetchWorker(UrlRequestChannel)
}

// URLFetchWorker has three channels: in channel for incoming requests
// (in a form of URL strings), out channel for outgoing responses in a form of
// ResponseType structure and quit channel
func URLFetchWorker(in <-chan UrlRequest) {
	urlRequests := &UrlFetchQueue{}
	heap.Init(urlRequests)
	// loop forever to accept url requests
	// a given request will be placed in internal Queue and we'll process it
	// only in a limited queueSize. Every request is processed via fetch
	// function which will decrement queueSize once it's done with request.
	for {
		select {
		case request := <-in:
			// put new request to urlRequests queue and increment queueSize
			heap.Push(urlRequests, &request)
		default:
			if urlRequests.Len() > 0 && UrlQueueSize < UrlQueueLimit {
				r := heap.Pop(urlRequests)
				request := r.(*UrlRequest)
				go fetch(request.rurl, request.args, request.out)
			}
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}
}

// Problem with too many open files
// http://craigwickesser.com/2015/01/golang-http-to-many-open-files/

// FetchResponse fetches data for provided URL, args is a json dump of arguments
func FetchResponse(rurl, args string) ResponseType {
	// increment UrlQueueSize since we'll process request
	atomic.AddInt32(&UrlQueueSize, 1)
	defer atomic.AddInt32(&UrlQueueSize, -1) // decrement UrlQueueSize since we done with this request
	if VERBOSE > 1 {
		log.Println("### HTTP request, UrlQueueSize", UrlQueueSize, "UrlQueueLimit", UrlQueueLimit)
	}
	var response ResponseType
	response.Url = rurl
	response.Data = []byte{}
	if validate_url(rurl) == false {
		response.Error = errors.New("Invalid URL")
		return response
	}
	var req *http.Request
	if len(args) > 0 {
		jsonStr := []byte(args)
		req, _ = http.NewRequest("POST", rurl, bytes.NewBuffer(jsonStr))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, _ = http.NewRequest("GET", rurl, nil)
		req.Header.Add("Accept-Encoding", "identity")
		if strings.Contains(rurl, "sitedb") {
			req.Header.Add("Accept", "application/json")
		}
	}
	if VERBOSE > 1 {
		dump1, err1 := httputil.DumpRequestOut(req, true)
		log.Println("### HTTP request", string(dump1), err1)
	}
	resp, err := client.Do(req)
	if VERBOSE > 1 {
		dump2, err2 := httputil.DumpResponse(resp, true)
		log.Println("### HTTP response", string(dump2), err2)
	}
	if err != nil {
		response.Error = err
		return response
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		response.Error = err
		return response
	}
	response.Data = body
	return response
}

// Fetch data for provided URL and redirect results to given channel
// This wrapper function look-up UrlQueueLimit and either redirect to
// URULFetchWorker go-routine or pass the call to local fetch function
func Fetch(rurl string, args string, out chan<- ResponseType) {
	if UrlQueueLimit > 0 {
		request := UrlRequest{rurl: rurl, args: args, out: out, ts: time.Now().Unix()}
		UrlRequestChannel <- request
	} else {
		fetch(rurl, args, out)
	}
}

// local function which fetch response for given url/args and place it into response channel
// By defat
func fetch(rurl string, args string, ch chan<- ResponseType) {
	//    log.Println("Receive", rurl)
	var resp, r ResponseType
	startTime := time.Now()
	resp = FetchResponse(rurl, args)
	if resp.Error != nil {
		log.Println("DAS WARNING, fail to fetch data", rurl, "error", resp.Error)
		for i := 1; i <= UrlRetry; i++ {
			sleep := time.Duration(i) * time.Second
			time.Sleep(sleep)
			r = FetchResponse(rurl, args)
			if r.Error == nil {
				break
			}
			log.Println("DAS WARNING", rurl, "retry", i, "error", r.Error)
		}
		resp = r
	}
	if resp.Error != nil {
		log.Println("DAS ERROR, fail to fetch data", rurl, "retries", UrlRetry, "error", resp.Error)
	}
	endTime := time.Now()
	if VERBOSE > 0 {
		if args == "" {
			log.Println("DAS GET", rurl, endTime.Sub(startTime))
		} else {
			log.Println("DAS POST", rurl, args, endTime.Sub(startTime))
		}
	}
	ch <- resp
}

// Helper function which validates given URL
func validate_url(rurl string) bool {
	if len(rurl) > 0 {
		pat := "(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]"
		matched, err := regexp.MatchString(pat, rurl)
		if err == nil {
			if matched == true {
				return true
			}
		}
		log.Println("ERROR invalid URL:", rurl)
	}
	return false
}

// Response represents final response in a form of JSON structure
// we use custorm representation
func Response(rurl string, data []byte) []byte {
	b := []byte(`{"url":`)
	u := []byte(rurl)
	c := []byte(",")
	d := []byte(`"data":`)
	e := []byte(`}`)
	a := [][]byte{b, u, c, d, data, e}
	s := []byte(" ")
	r := bytes.Join(a, s)
	return r

}

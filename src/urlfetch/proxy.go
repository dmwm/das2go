/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: URL fetch proxy server concurrently fetches data from
 *              provided URL list. It provides a POST HTTP interface
 *              "/fetch" which accepts urls as newline separated encoded
 *              string
 * Created    : Wed Mar 20 13:29:48 EDT 2013
 * License    : MIT
 *
 */
package urlfetch

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"x509proxy"
)

/*
 * Return array of certificates
 */
func Certs() (tls_certs []tls.Certificate) {
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")
	log.Println("X509_USER_PROXY", uproxy)
	log.Println("X509_USER_KEY", uckey)
	log.Println("X509_USER_CERT", ucert)
	if len(uproxy) > 0 {
		// use local implementation of LoadX409KeyPair instead of tls one
		x509cert, err := x509proxy.LoadX509Proxy(uproxy)
		if err != nil {
			log.Println("Fail to parser proxy X509 certificate", err)
			return
		}
		tls_certs = []tls.Certificate{x509cert}
	} else if len(uckey) > 0 {
		x509cert, err := tls.LoadX509KeyPair(ucert, uckey)
		if err != nil {
			log.Println("Fail to parser user X509 certificate", err)
			return
		}
		tls_certs = []tls.Certificate{x509cert}
	} else {
		return
	}
	return
}

/*
 * HTTP client for urlfetch server
 */
func HttpClient() (client *http.Client) {
	// create HTTP client
	certs := Certs()
	log.Println("Number of certificates", len(certs))
	if len(certs) == 0 {
		client = &http.Client{}
		return
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{Certificates: certs,
			InsecureSkipVerify: true},
	}
	log.Println("Create TLSClientConfig")
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

// A URL fetch Worker. It has three channels: in channel for incoming requests
// (in a form of URL strings), out channel for outgoing responses in a form of
// ResponseType structure and quit channel
func Worker(in <-chan string, out chan<- ResponseType, quit <-chan bool) {
	for {
		select {
		case url := <-in:
			//            log.Println("Receive", url)
			go Fetch(url, out)
		case <-quit:
			//            log.Println("Quit Worker")
			return
		default:
			time.Sleep(time.Duration(100) * time.Millisecond)
			//            log.Println("Waiting for request")
		}
	}
}

/*
 * Fetch(url string, ch chan<- []byte)
 * Fetch data for provided URL and redirect results to given channel
 */
func Fetch(url string, ch chan<- ResponseType) {
	//    log.Println("Receive", url)
	startTime := time.Now()
	var response ResponseType
	response.Url = url
	if validate_url(url) == false {
		response.Error = errors.New("Invalid URL")
		ch <- response
		return
	}
	resp, err := client.Get(url)
	if err != nil {
		response.Error = err
		ch <- response
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		response.Error = err
		ch <- response
		return
	}
	response.Data = body
	endTime := time.Now()
	log.Println(url, endTime.Sub(startTime))
	ch <- response
}

/*
 * Helper function which validates given URL
 */
func validate_url(url string) bool {
	if len(url) > 0 {
		pat := "(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]"
		matched, err := regexp.MatchString(pat, url)
		if err == nil {
			if matched == true {
				return true
			}
		}
		log.Println("ERROR invalid URL:", url)
	}
	return false
}

// represent final response in a form of JSON structure
// we use custorm representation
func response(url string, data []byte) []byte {
	b := []byte(`{"url":`)
	u := []byte(url)
	c := []byte(",")
	d := []byte(`"data":`)
	e := []byte(`}`)
	a := [][]byte{b, u, c, d, data, e}
	s := []byte(" ")
	r := bytes.Join(a, s)
	return r

}

/*
 * RequestHandler is used by web server to handle incoming requests
 */
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	// we only accept POST requests with urls (this is by design)
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// parse input request parameter, in this case we should pass urls
	r.ParseForm()
	urls := []string{}
	for k, v := range r.Form {
		if k == "urls" {
			urls = strings.Split(v[0], "\n")
		}
	}
	num := len(urls)
	if num > 0 {
		first := urls[0]
		last := urls[num-1]
		log.Println("Fetch", num, "URLs:", first, "...", last)
	} else {
		w.Write([]byte("No URLs provided\n"))
		return
	}
	// Process url list
	quit := make(chan bool)
	in := make(chan string)
	out := make(chan ResponseType)
	umap := map[string]int{}
	rmax := 3 // maximum number of retries
	// start worker
	go Worker(in, out, quit)
	// fill in-channel with request urls, keep url/retries in umap
	for _, url := range urls {
		in <- url
		umap[url] = 0 // attemps per url
	}
	// Start inf. loop to catch our response channel (out)
	// we check every response for errors and either discard it from umap or
	// retry several times until we reach a threshold rmax
	exit := false
	for {
		select {
		case r := <-out:
			if r.Error != nil {
				retry := umap[r.Url]
				//                log.Println("ERROR", r.Url, r.Error.Error(), "retry", retry)
				if retry < rmax {
					retry += 1
					// incremenet sleep duration with every retry
					sleep := time.Duration(retry) * time.Second
					time.Sleep(sleep)
					in <- r.Url
					umap[r.Url] = retry
				} else {
					//                    log.Println("ERROR", r.Url, r.Error.Error(), "exceed retries")
					delete(umap, r.Url) // remove Url from map
				}
			} else {
				w.Write(response(r.Url, r.Data))
				w.Write([]byte("\n"))
				delete(umap, r.Url) // remove Url from map
			}
		default:
			if len(umap) == 0 {
				//                log.Println("No more requests")
				exit = true
			}
			time.Sleep(time.Duration(100) * time.Millisecond)
			//            log.Println("Waiting for response", len(umap))
		}
		if exit {
			break
		}
	}
	//    log.Println("out of loop")
	quit <- true

	// close all channels we used
	close(in)
	close(out)
}

// proxy server. It defines /fetch public interface
func Server(port string) {
	log.Printf("Start server localhost:%s/fetch", port)
	http.HandleFunc("/fetch", RequestHandler)
	err := http.ListenAndServe(":"+port, nil)
	// NOTE: later this can be replaced with secure connection
	// replace ListenAndServe(addr string, handler Handler)
	// with TLS function
	// ListenAndServeTLS(addr string, certFile string, keyFile string, handler
	// Handler)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

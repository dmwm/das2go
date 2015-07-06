/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS web server, it handles all DAS reuqests
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 * Some links: http://www.alexedwards.net/blog/golang-response-snippets
 * http://blog.golang.org/json-and-go
 * http://golang.org/pkg/html/template/
 * https://labix.org/mgo
 */
package web

import (
	"bytes"
	"crypto/tls"
	"das"
	"dasmaps"
	"errors"
	//     "html/template"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
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
	query := r.FormValue("query")
	limit, err := strconv.Atoi(r.FormValue("limit"))
	if err != nil {
		limit = 10
	}
	idx, err := strconv.Atoi(r.FormValue("idx"))
	if err != nil {
		idx = 0
	}
	path := r.URL.Path
	response := make(map[string]interface{})

	// load DAS Maps if neccessary
	var dasmaps dasmaps.DASMaps
	uri := "mongodb://localhost:8230"
	if len(dasmaps.Services()) == 0 {
		log.Println("Load DAS maps")
		dasmaps.LoadMaps(uri, "mapping", "db")
		log.Println("DAS services", dasmaps.Services())
	}

	// process requests based on the path
	if path == "/das" {
		log.Println("Process /das", query, limit, idx)
	} else if path == "/das/request" {
		log.Println("Process request", query, limit, idx)
	} else if path == "/das/cache" {
		log.Printf("Process cache request, query=%s, idx=%d, limit=%d\n", query, idx, limit)
		status, qhash := das.Process(query, dasmaps)
		response["status"] = status
		response["qhash"] = qhash
		response["idx"] = idx
		response["limit"] = limit
		js, err := json.Marshal(&response)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
	} else {
		//         t, _ := template.ParseFiles("src/templates/error.html")
		//         t.Execute(w, nil)
		http.Error(w, "Not implemented path", http.StatusInternalServerError)
	}
}

// proxy server. It defines /fetch public interface
func Server(port string) {
	log.Printf("Start server localhost:%s/das", port)
	http.HandleFunc("/das/request", RequestHandler)
	http.HandleFunc("/das/cache", RequestHandler)
	http.HandleFunc("/das", RequestHandler)
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

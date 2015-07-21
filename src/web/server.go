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
	"das"
	"dasmaps"
	"dasql"
	//     "html/template"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

/*
 * RequestHandler is used by web server to handle incoming requests
 */
func RequestHandler(w http.ResponseWriter, r *http.Request) {
	query := r.FormValue("input")
	dasquery := dasql.Parse(query)
	log.Printf("Process %s\n", dasquery)

	pid := r.FormValue("pid")
	if pid == "" {
		pid = dasquery.Qhash
	}
	if len(pid) != 32 {
		http.Error(w, "DAS query pid is not valid", http.StatusInternalServerError)
	}
	log.Println("input", query, pid)
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

	// Remove expire records from cache
	das.RemoveExpired(dasquery.Qhash)

	// process requests based on the path
	if path == "/das" {
		log.Println("Process /das", query, limit, idx)
	} else if path == "/das/request" {
		log.Println("Process request", query, limit, idx)
	} else if path == "/das/cache" {
		if das.CheckDataReadiness(pid) { // data exists in cache and ready for retrieval
			status, data := das.GetData(pid, "merge")
			response["nresults"] = das.Count(pid)
			response["timestamp"] = das.GetTimestamp(pid)
			response["status"] = status
			response["pid"] = pid
			response["data"] = data
		} else if das.CheckData(pid) { // data exists in cache but still processing
			w.Write([]byte(pid))
			return
			//             response["status"] = "processing"
			//             response["pid"] = pid
		} else { // no data in cache (even client supplied the pid), process it
			qhash := das.Process(dasquery, dasmaps)
			w.Write([]byte(qhash))
			return
			//             response["status"] = "requested"
			//             response["pid"] = qhash
		}
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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"time"

	_ "expvar"         // to be used for monitoring, see https://github.com/divan/expvarmon
	_ "net/http/pprof" // profiler, see https://golang.org/pkg/net/http/pprof/
)

func checkHttpEndpoint(endpoint, pat string) bool {
	timeout := time.Duration(5 * time.Second)
	client := http.Client{Timeout: timeout}
	resp, err := client.Get(endpoint)

	if err != nil {
		log.Printf("ERROR: unable to fetch data, url %v, error %v\n", endpoint, err)
		return false
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("ERROR: unable to read response body, status %v, error %v\n", resp.Status, err)
		return false
	}
	matched, _ := regexp.MatchString(pat, string(data))
	if !matched {
		log.Printf("ERROR: unable to read response body with pattern %v, error %v\n", pat, err)
		return false
	}
	return true
}

func checkProcess(pat string) bool {
	cmd := fmt.Sprintf("ps auxw | grep \"%s\" | grep -v grep", pat)
	out, err := exec.Command("sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("ERROR: unable to find process patter %v, error %v\n", pat, err)
		return false
	}
	matched, _ := regexp.MatchString(pat, fmt.Sprintf("%s", out))
	if matched {
		return true
	}
	return false
}

// helper function to start underlying das2go server
// for pipe usage see https://zupzup.org/io-pipe-go/
func start(config string, pw *io.PipeWriter) {
	cmd := exec.Command("das2go", "-config", config)
	cmd.Stdout = pw
	cmd.Stderr = pw
	err := cmd.Run()
	if err != nil {
		log.Printf("ERROR: unable to start DAS server, error %v\n", err)
		return
	}
}

func monitor(port int64, config string) {
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	go func() {
		if _, err := io.Copy(os.Stdout, pr); err != nil {
			log.Println("ERROR: unable to pipe das2go output", err)
			return
		}
	}()
	pat := "das2go -config"
	// check local server
	status := checkProcess(pat)
	if !status {
		log.Printf("DAS server is not running, pattern %v, status %v\n", pat, status)
		start(config, pw)
	}
	// check running process, it should respond on localhost
	endpoint := fmt.Sprintf("http://localhost:%d/das/status", port)
	pat = "DAS server status"
	for {
		status = checkHttpEndpoint(endpoint, pat)
		if !status {
			log.Printf("DAS HTTP endpoint failure, endpoint %v, pattern %v, status %v, restarting ...\n", endpoint, pat, status)
			start(config, pw)
		}
		sleep := time.Duration(10) * time.Second
		time.Sleep(sleep)
	}
}

func main() {
	var config string
	flag.StringVar(&config, "config", "config.json", "DAS server config")
	flag.Parse()
	// parse DAS config file and find our on which port it is running
	data, e := os.ReadFile(config)
	if e != nil {
		log.Fatalf("unable to open %s\n", config)
	}
	var c map[string]interface{}
	e = json.Unmarshal(data, &c)
	if e != nil {
		log.Fatalf("unabel to unmarshal %s\n", config)
	}
	port := int64(c["port"].(float64))
	go monitor(port, config)
	http.ListenAndServe(":8218", nil)
}

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"time"

	_ "expvar"         // to be used for monitoring, see https://github.com/divan/expvarmon
	_ "net/http/pprof" // profiler, see https://golang.org/pkg/net/http/pprof/

	logs "github.com/sirupsen/logrus"
)

func checkHttpEndpoint(endpoint, pat string) bool {
	timeout := time.Duration(5 * time.Second)
	client := http.Client{Timeout: timeout}
	resp, err := client.Get(endpoint)

	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
			"Url":   endpoint,
		}).Error("Unable to fetch data")
		return false
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error":  err,
			"Status": resp.Status,
		}).Error("Unable to read response body")
		return false
	}
	matched, _ := regexp.MatchString(pat, string(data))
	if matched {
		logs.WithFields(logs.Fields{
			"Error":   err,
			"Pattern": pat,
		}).Error("Unable to read response body with pattern")
		return true
	}
	return false
}

func checkProcess(pat string) bool {
	cmd := fmt.Sprintf("ps auxw | grep %s | grep -v grep", pat)
	out, err := exec.Command("sh", "-c", cmd).Output()
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error":   err,
			"Pattern": pat,
		}).Error("Unable to find process pattern")
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
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("unable to start DAS server")
		return
	}
}

func monitor(port int64, config string) {
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	go func() {
		if _, err := io.Copy(os.Stdout, pr); err != nil {
			logs.WithFields(logs.Fields{
				"Error": err,
			}).Error("Unable to pipe das2go output")
			return
		}
	}()
	pat := "das2go -config"
	// check local server
	status := checkProcess(pat)
	if !status {
		logs.Info("DAS server is not running, starting ...")
		start(config, pw)
	}
	// check running process, it should respond on localhost
	endpoint := fmt.Sprintf("http://localhost:%d", port)
	for {
		status = checkHttpEndpoint(endpoint, pat)
		if !status {
			logs.Warn("DAS HTTP endpoint failure, re-starting ...")
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
	data, e := ioutil.ReadFile(config)
	if e != nil {
		logs.WithFields(logs.Fields{
			"Config": config,
		}).Error("Unable to open")
		os.Exit(1)
	}
	var c map[string]interface{}
	e = json.Unmarshal(data, &c)
	if e != nil {
		logs.WithFields(logs.Fields{
			"Config": c,
		}).Error("Unable to unmarshal")
		os.Exit(1)
	}
	port := int64(c["port"].(float64))
	go monitor(port, config)
	http.ListenAndServe(":8218", nil)
}

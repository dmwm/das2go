package utils

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	logs "github.com/sirupsen/logrus"
)

// DAS RucioAuth module
//
// Copyright (c) 2018 - Valentin Kuznetsov <vkuznet AT gmail dot com>

// RucioValidity
var RucioValidity int64

// RucioAuth represents instance of rucio authentication module
var RucioAuth RucioAuthModule

// RucioAuthModule structure holds all information about Rucio authentication
type RucioAuthModule struct {
	account string
	agent   string
	token   string
	url     string
	ts      time.Time
}

// Token returns Rucio authentication token
func (r *RucioAuthModule) Token() string {
	t := time.Now().Unix() - r.ts.Unix()
	if r.token != "" && t < RucioValidity {
		return r.token
	}
	FetchRucioTokenViaCurl(r.url)
	r.ts = time.Now()
	return r.token
}

// Account returns Rucio authentication account
func (r *RucioAuthModule) Account() string {
	if r.account == "" {
		r.account = "das"
	}
	return r.account
}

// Agent returns Rucio authentication agent
func (r *RucioAuthModule) Agent() string {
	if r.agent == "" {
		r.agent = "dasgoserver"
	}
	return r.agent
}

// Url returns Rucio authentication url
func (r *RucioAuthModule) Url() string {
	if r.url == "" {
		r.url = "https://cms-rucio-authz.cern.ch/auth/x509"
	}
	return r.url
}

// run go-routine to periodically obtain rucio token
// FetchRucioToken request new Rucio token
func FetchRucioToken(rurl string) string {
	req, _ := http.NewRequest("GET", rurl, nil)
	req.Header.Add("Accept-Encoding", "identity")
	req.Header.Add("X-Rucio-Account", RucioAuth.Account())
	req.Header.Set("User-Agent", RucioAuth.Agent())
	client := HttpClient()
	resp, err := client.Do(req)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("unable to Http client")
		return ""
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("unable to close the response body")
		return ""
	}
	if v, ok := resp.Header["X-Rucio-Auth-Token"]; ok {
		return v[0]
	}
	return ""
}

// FetchRucioTokenViaCurl is a helper function to get Rucio token by using curl command
func FetchRucioTokenViaCurl(rurl string) string {
	proxy := os.Getenv("X509_USER_PROXY")
	account := fmt.Sprint("X-Rucio-Account:%s", RucioAuth.Account())
	agent := RucioAuth.Agent()
	cmd := fmt.Sprintf("curl -v --key %s --cert %s -H \"%s\" -A %s %s", proxy, proxy, account, agent, rurl)
	fmt.Println(cmd)
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("unable to execute")
		return ""
	}
	var token string
	for _, v := range strings.Split(string(out), "\n") {
		if strings.Contains(v, "X-Rucio-Auth-Token") {
			arr := strings.Split(v, ":")
			token = strings.Trim(arr[len(arr)-1], " ")
		}
	}
	return token
}

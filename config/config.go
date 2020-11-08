package config

// configuration module for das2go
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
)

// Configuration stores DAS configuration parameters
type Configuration struct {
	Port                  int      `json:"port"`                  // DAS port number
	Uri                   string   `json:"uri"`                   // DAS mongodb URI
	Services              []string `json:"services"`              // DAS services
	UrlQueueLimit         int32    `json:"urlQueueLimit"`         // DAS url queue limit
	UrlRetry              int      `json:"urlRetry"`              // DAS url retry number
	Templates             string   `json:"templates"`             // location of DAS templates
	Jscripts              string   `json:"jscripts"`              // location of DAS JavaScript files
	Images                string   `json:"images"`                // location of DAS images
	Styles                string   `json:"styles"`                // location of DAS CSS styles
	Hkey                  string   `json:"hkey"`                  // DAS HKEY file
	Base                  string   `json:"base"`                  // DAS base path
	DbsInstances          []string `json:"dbsInstances"`          // list of DBS instances
	Views                 []string `json:"views"`                 // list of supported views
	Verbose               int      `json:"verbose"`               // verbosity level
	DasMaps               string   `json:"dasmaps"`               // location of dasmaps
	DasExamples           string   `json:"dasexamples"`           // location of dasexamples
	ServerKey             string   `json:"serverkey"`             // server key for https
	ServerCrt             string   `json:"servercrt"`             // server certificate for https
	UpdateDNs             int      `json:"updateDNs"`             // interval in minutes to update user DNs
	Timeout               int      `json:"timeout"`               // query time out
	Frontend              string   `json:"frontend"`              // frontend URI to use
	RucioTokenCurl        bool     `json:"rucioTokenCurl"`        // use curl method to obtain Rucio Token
	ProfileFile           string   `json:"profileFile"`           // send profile data to a given file
	TLSCertsRenewInterval int      `json:"tlsCertsRenewInterval"` // renewal interval for TLS certs
	LogFile               string   `json:"logFile"`               // log file name
	UseDNSCache           bool     `json:"useDNSCache"`           // use DNS Cache
	AuthDN                bool     `json:"authDN"`                // user user DN authentication
}

// Config variable represents configuration object
var Config Configuration

// String returns string representation of DAS Config
func (c *Configuration) String() string {
	return fmt.Sprintf("<Config port=%d uri=%s services=%v queueLimit=%d retry=%d templates=%s js=%s images=%s css=%s hkey=%s base=%s dbs=%v views=%v maps=%s examples=%s updateDNs=%d crt=%s key=%s timeout=%d frontend=%s useDNScache=%v>", c.Port, c.Uri, c.Services, c.UrlQueueLimit, c.UrlRetry, c.Templates, c.Jscripts, c.Images, c.Styles, c.Hkey, c.Base, c.DbsInstances, c.Views, c.DasMaps, c.DasExamples, c.UpdateDNs, c.ServerCrt, c.ServerKey, c.Timeout, c.Frontend, c.UseDNSCache)
}

// ParseConfig parse given config file
func ParseConfig(configFile string) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Printf("Unable to read: file %s, error %v\n", configFile, err)
		return err
	}
	err = json.Unmarshal(data, &Config)
	if err != nil {
		log.Printf("Unable to parse: file %s, error %v\n", configFile, err)
		return err
	}
	if Config.Frontend == "" {
		log.Printf("The frontend record is not set: file %s, error %v\n", configFile, err)
		return errors.New("No frontend record found in config")
	}
	if Config.TLSCertsRenewInterval == 0 {
		Config.TLSCertsRenewInterval = 600
	}
	return nil
}

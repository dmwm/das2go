package config

// configuration module for das2go
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	logs "github.com/sirupsen/logrus"
)

// Configuration stores DAS configuration parameters
type Configuration struct {
	Port          int      `json:"port"`          // DAS port number
	Uri           string   `json:"uri"`           // DAS mongodb URI
	Services      []string `json:"services"`      // DAS services
	UrlQueueLimit int32    `json:"urlQueueLimit"` // DAS url queue limit
	UrlRetry      int      `json:"urlRetry"`      // DAS url retry number
	Templates     string   `json:"templates"`     // location of DAS templates
	Jscripts      string   `json:"jscripts"`      // location of DAS JavaScript files
	Images        string   `json:"images"`        // location of DAS images
	Styles        string   `json:"styles"`        // location of DAS CSS styles
	YuiRoot       string   `json:"yuiRoot"`       // location of YUI ROOT
	Hkey          string   `json:"hkey"`          // DAS HKEY file
	Base          string   `json:"base"`          // DAS base path
	DbsInstances  []string `json:"dbsInstances"`  // list of DBS instances
	Views         []string `json:"views"`         // list of supported views
	Verbose       int      `json:"verbose"`       // verbosity level
}

// global variables
var Config Configuration

// String returns string representation of DAS Config
func (c *Configuration) String() string {
	return fmt.Sprintf("<Config port=%d uri=%s services=%v queueLimit=%d retry=%d templates=%s js=%s images=%s css=%s yui=%s hkey=%s base=%s dbs=%v views=%v>", c.Port, c.Uri, c.Services, c.UrlQueueLimit, c.UrlRetry, c.Templates, c.Jscripts, c.Images, c.Styles, c.YuiRoot, c.Hkey, c.Base, c.DbsInstances, c.Views)
}

func ParseConfig(configFile string) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		logs.WithFields(logs.Fields{"configFile": configFile}).Fatal("Unable to read", err)
		return err
	}
	err = json.Unmarshal(data, &Config)
	if err != nil {
		logs.WithFields(logs.Fields{"configFile": configFile}).Fatal("Unable to parse", err)
		return err
	}
	return nil
}

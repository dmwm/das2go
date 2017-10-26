package config

// configuration module for das2go
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/dmwm/das2go/utils"
)

// Configuration structure
type Configuration struct {
	Uri      string
	Services []string
}

// global config object
var _config Configuration

// ParseConfig function to parse configuration file
func ParseConfig() Configuration {
	var fname string
	for _, item := range os.Environ() {
		value := strings.Split(item, "=")
		if value[0] == "DAS_CONFIG" {
			fname = value[1]
			break
		}
	}
	if fname == "" {
		panic("DAS_CONFIG environment variable is not set")
	}
	if utils.WEBSERVER > 0 {
		log.Println("DAS_CONFIG", fname)
	}
	file, _ := os.Open(fname)
	decoder := json.NewDecoder(file)
	conf := Configuration{}
	err := decoder.Decode(&conf)
	if err != nil {
		panic(err)
	}
	if utils.WEBSERVER > 0 {
		log.Println("DAS configuration", conf)
	}
	return conf
}

// Uri function extracts URI from configuration
func Uri() string {
	if _config.Uri == "" {
		_config = ParseConfig()
	}
	return _config.Uri
}

// Services function extracts URI from configuration
func Services() []string {
	if len(_config.Services) == 0 {
		_config = ParseConfig()
	}
	return _config.Services
}

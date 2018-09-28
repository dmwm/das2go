package utils

import "time"

// DAS RucioAuth module
//
// Copyright (c) 2018 - Valentin Kuznetsov <vkuznet AT gmail dot com>

// RucioValidity
var RucioValidity int64

// RucioAuthModule structure holds all information about Rucio authentication
type RucioAuthModule struct {
	token   string
	account string
	ts      time.Time
}

// Token returns Rucio authentication token
func (r *RucioAuthModule) Token() string {
	t := time.Now().Unix() - r.ts.Unix()
	if r.token != "" && t < RucioValidity {
		return r.token
	}
	r.ts = time.Now()
	// obtain rucio token
	return "X-Rucio-Auth-Token"
}

// Account returns Rucio authentication account
func (r *RucioAuthModule) Account() string {
	if r.account != "" {
		return r.account
	}
	// obtain rucio account info
	return "rucio-account"
}

// run go-routine to periodically obtain rucio token
func FetchRucioToken(period int) {
	var rucio RucioAuthModule
	for {
		rucio.Token()
		sleep := time.Duration(period) * time.Second
		time.Sleep(sleep)
	}
}

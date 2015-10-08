package web

import (
	"crypto/hmac"
	"crypto/sha1"
	"fmt"
	"hash"
	"io/ioutil"
	"net/http"
	"strings"
)

// helper function which checks Authentication
func checkAuthentication(headers http.Header) bool {
	var val interface{}
	val = headers["cms-auth-status"]
	if val == nil {
		return false
	}
	values := val.([]string)
	if len(values) == 1 && values[0] == "NONE" {
		// user authentication is optional
		return true
	}
	var prefix, suffix, hmacValue string
	for kkk, values := range headers {
		key := strings.ToLower(kkk)
		if (strings.HasPrefix(key, "cms-authn") || strings.HasPrefix(key, "cms-authz")) && key != "cms-authn-hmac" {
			prefix += fmt.Sprintf("h%xv%x", len(key), len(values))
			suffix += fmt.Sprintf("%s%s", key, values)
			if strings.HasPrefix(key, "cms-authn") {
				headers[strings.Replace(key, "cms-authn-", "", 1)] = values
			}
		}
		if key == "cms-authn-hmac" {
			hmacValue = values[0]
		}
	}
	value := []byte(fmt.Sprintf("%s#%s", prefix, suffix))
	var sha1hex hash.Hash
	if len(_thkey) != 0 {
		hkey, err := ioutil.ReadFile(_thkey)
		if err != nil {
			fmt.Println("DAS ERROR, unable to read DAS_HKEY_FILE", _thkey)
			return false
		}
		sha1hex = hmac.New(sha1.New, hkey)
	} else {
		sha1hex = sha1.New()
	}
	sha1hex.Write(value)
	hmacFound := fmt.Sprintf("%x", sha1hex.Sum(nil))
	fmt.Println("### cms-authn-hmac", hmacValue)
	fmt.Println("### found     hmac", hmacFound)
	if hmacFound != hmacValue {
		return false
	}
	return true
}

// helper function which checks Authorization
func checkAuthorization(header http.Header) bool {
	return true
}

// helper function which checks Authentication and Authorization
func checkAuthnAuthz(header http.Header) bool {
	status := checkAuthentication(header)
	if !status {
		return status
	}
	status = checkAuthorization(header)
	return status

}

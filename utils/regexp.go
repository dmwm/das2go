package utils

import (
	"regexp"
)

var PatternInt = regexp.MustCompile("(^[0-9-]$|^[0-9-][0-9]*$)")
var PatternSite = regexp.MustCompile("^T[0-9]_[A-Z]+(_)[A-Z]+")
var PatternSE = regexp.MustCompile("^[a-z]+(\\.)[a-z]+(\\.)")
var PatternUrl = regexp.MustCompile("(https|http)://[-A-Za-z0-9_+&@#/%?=~_|!:,.;]*[-A-Za-z0-9+&@#/%=~_|]")

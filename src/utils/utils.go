/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS utils module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 */
package utils

import (
	"fmt"
	"log"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"time"
)

// global variable for this module which we're going to use across
// many modules
var VERBOSE bool

// helper function to return Stack
func Stack() string {
	trace := make([]byte, 2048)
	count := runtime.Stack(trace, false)
	return fmt.Sprintf("\nStack of %d bytes: %s\n", count, trace)
}

// error helper function which can be used in defer ErrPropagate()
func ErrPropagate(api string) {
	if err := recover(); err != nil {
		log.Println("DAS ERROR", api, "error", err, Stack())
		panic(fmt.Sprintf("%s:%s", api, err))
	}
}

// error helper function which can be used in goroutines as
// ch := make(chan interface{})
// go func() {
//    defer ErrPropagate2Channel(api, ch)
//    someFunction()
// }()
func ErrPropagate2Channel(api string, ch chan interface{}) {
	if err := recover(); err != nil {
		log.Println("DAS ERROR", api, "error", err, Stack())
		ch <- fmt.Sprintf("%s:%s", api, err)
	}
}

// Helper function to run any given function in defered go routine
func GoDeferFunc(api string, f func()) {
	ch := make(chan interface{})
	go func() {
		defer ErrPropagate2Channel(api, ch)
		f()
		ch <- "ok" // send to channel that we can read it later in case of success of f()
	}()
	err := <-ch
	if err != nil && err != "ok" {
		panic(err)
	}
}

// helper function to find item in a list
func FindInList(a string, arr []string) bool {
	for _, e := range arr {
		if e == a {
			return true
		}
	}
	return false
}

// helper function to check item in a list
func InList(a string, list []string) bool {
	check := 0
	for _, b := range list {
		if b == a {
			check += 1
		}
	}
	if check != 0 {
		return true
	}
	return false
}

// helper function to return keys from a map
func MapKeys(rec map[string]interface{}) []string {
	keys := make([]string, 0, len(rec))
	for k := range rec {
		keys = append(keys, k)
	}
	return keys
}

// helper function to compare list of strings
func EqualLists(list1, list2 []string) bool {
	count := 0
	for _, k := range list1 {
		if InList(k, list2) {
			count += 1
		} else {
			return false
		}
	}
	if len(list2) == count {
		return true
	}
	return false
}

// helper function to check that entries from list1 are all appear in list2
func CheckEntries(list1, list2 []string) bool {
	var out []string
	for _, k := range list1 {
		if InList(k, list2) {
			//             count += 1
			out = append(out, k)
		}
	}
	if len(out) == len(list1) {
		return true
	}
	return false
}

// helper function to convert expire timestamp (int) into seconds since epoch
func Expire(expire int) int64 {
	tstamp := strconv.Itoa(expire)
	if len(tstamp) == 10 {
		return int64(expire)
	}
	return int64(time.Now().Unix() + int64(expire))
}

// helper function to convert given time into Unix timestamp
func UnixTime(ts string) int64 {
	// YYYYMMDD, always use 2006 as year 01 for month and 02 for date since it is predefined int Go parser
	const layout = "20060102"
	t, err := time.Parse(layout, ts)
	if err != nil {
		panic(err)
	}
	return int64(t.Unix())
}

// helper function to convert given time into Unix timestamp
func Unix2DASTime(ts int64) string {
	// YYYYMMDD, always use 2006 as year 01 for month and 02 for date since it is predefined int Go parser
	const layout = "20060102"
	t := time.Unix(ts, 0)
	return t.Format(layout)
}

// helper function to convert given time into Dashboard timestamp
func DashboardTime(ts string) string {
	// YYYYMMDD, always use 2006 as year 01 for month and 02 for date since it is predefined int Go parser
	const layout = "20060102"
	t, err := time.Parse(layout, ts)
	if err != nil {
		panic(err)
	}
	return t.Format("2006-01-02 15:04:05") // represent t in given format
}

// helper function to convert given time into Conddb timestamp
func ConddbTime(ts string) string {
	// YYYYMMDD, always use 2006 as year 01 for month and 02 for date since it is predefined int Go parser
	const layout = "20060102"
	t, err := time.Parse(layout, ts)
	if err != nil {
		panic(err)
	}
	return t.Format("02-Jan-06-15:04") // represent t in given format
}

// helper function to convert input list into set
func List2Set(arr []string) []string {
	var out []string
	for _, key := range arr {
		if !InList(key, out) {
			out = append(out, key)
		}
	}
	return out
}

// helper function to convert Unix time into human readable form
func TimeFormat(ts float64) string {
	layout := "2006-01-02 15:04:05"
	return time.Unix(int64(ts), 0).UTC().Format(layout)
}

// helper function to convert size into human readable form
func SizeFormat(val float64) string {
	base := 1000. // CMS convert is to use power of 10
	xlist := []string{"", "KB", "MB", "GB", "TB", "PB"}
	for _, vvv := range xlist {
		if val < base {
			return fmt.Sprintf("%3.1f%s", val, vvv)
		}
		val = val / base
	}
	return fmt.Sprintf("%3.1f%s", val, xlist[len(xlist)])
}

// helper function to test if given value is integer
func IsInt(val string) bool {
	pat := "(^[0-9-]$|^[0-9-][0-9]*$)"
	matched, _ := regexp.MatchString(pat, val)
	if matched {
		return true
	}
	return false
}

// helper function to perform sum operation over provided array of values
func Sum(data []interface{}) float64 {
	out := 0.0
	for _, val := range data {
		if val != nil {
			out += val.(float64)
		}
	}
	return out
}

// helper function to perform Max operation over provided array of values
func Max(data []interface{}) float64 {
	out := 0.0
	for _, val := range data {
		if val != nil {
			v := val.(float64)
			if v > out {
				out = v
			}
		}
	}
	return out
}

// helper function to perform Min operation over provided array of values
func Min(data []interface{}) float64 {
	out := 1e100
	for _, val := range data {
		if val == nil {
			continue
		}
		v := val.(float64)
		if v < out {
			out = v
		}
	}
	return out
}

// helper function to perform Mean operation over provided array of values
func Mean(data []interface{}) float64 {
	return Sum(data) / float64(len(data))
}

// helper function to perform Avg operation over provided array of values
func Avg(data []interface{}) float64 {
	return Mean(data)
}

// helper function to perform Median operation over provided array of values
func Median(data []interface{}) float64 {
	var input sort.Float64Slice
	var median float64
	for _, v := range data {
		input = append(input, v.(float64))
	}
	input.Sort()
	l := len(input)
	if l == 0 {
		return 0
	} else if l%2 == 0 {
		median = (input[l/2-1] + input[l/2+1]) / 2.
	} else {
		median = float64(input[l/2])
	}
	return median
}

// implement sort for []int type
type IntList []int

func (s IntList) Len() int           { return len(s) }
func (s IntList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s IntList) Less(i, j int) bool { return s[i] < s[j] }

// implement sort for []string type
type StringList []string

func (s StringList) Len() int           { return len(s) }
func (s StringList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StringList) Less(i, j int) bool { return s[i] < s[j] }

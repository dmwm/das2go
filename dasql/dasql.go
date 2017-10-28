package dasql

// DAS Query Language (QL) implementation for DAS server
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
)

// DASQuery provides basic structure to hold DAS query record
type DASQuery struct {
	relaxedQuery string
	Query        string              `json:"query"`
	Qhash        string              `json:"hash"`
	Spec         bson.M              `json:"spec"`
	Fields       []string            `json:"fields"`
	Pipe         string              `json:"pipe"`
	Instance     string              `json:"instance"`
	Detail       bool                `json:"detail"`
	System       string              `json:"system"`
	Filters      map[string][]string `json:"filters"`
	Aggregators  [][]string          `json:"aggregators"`
	Error        string              `json:"error"`
	Time         time.Time           `json:"tstamp"`
}

// String method implements own formatter using DASQuery rather then *DASQuery, since
// former will be invoked on both pointer and values and therefore used by fmt/log
// http://stackoverflow.com/questions/16976523/in-go-why-isnt-my-stringer-interface-method-getting-invoked-when-using-fmt-pr
func (q DASQuery) String() string {
	if utils.VERBOSE == 0 {
		return fmt.Sprintf("<DASQuery=\"%s\" inst=%s hash=%s system=%s>", q.Query, q.Instance, q.Qhash, q.System)
	}
	return fmt.Sprintf("<DASQuery=\"%s\" inst=%s hash=%s system=%s fields=%s spec=%s filters=%s aggrs=%s detail=%v>", q.Query, q.Instance, q.Qhash, q.System, q.Fields, q.Spec, q.Filters, q.Aggregators, q.Detail)
}

// Marshall method return query representation in JSON format
func (q DASQuery) Marshall() string {
	rec, err := json.Marshal(q)
	if err != nil {
		return fmt.Sprintf("DASQuery fail to parse, error %v", err)
	}
	return string(rec)
}

func operators() []string {
	return []string{"(", ")", ">", "<", "!", "[", "]", ",", "=", "in", "between", "last"}
}
func relax(query string) string {
	for _, oper := range operators() {
		if oper == "in" || oper == "between" || oper == "last" {
			continue
		} else {
			newOp := " " + oper + " "
			query = strings.Replace(query, oper, newOp, -1)
		}
	}
	arr := strings.Split(query, " ")
	out := []string{}
	nnval := ""
	qlen := len(arr)
	idx := 0
	for idx < qlen {
		sval := string(arr[idx])
		if sval == "" {
			idx += 1
			continue
		}
		if idx+2 < qlen {
			nnval = string(arr[idx+2])
		} else {
			nnval = "NA"
		}
		if nnval == "=" {
			if sval == "<" || sval == ">" || sval == "!" {
				out = append(out, sval+string(nnval)+" ")
				idx += 3
			} else {
				out = append(out, sval)
				idx += 1
			}
		} else {
			out = append(out, sval)
			idx += 1
		}
	}
	return strings.Join(out, " ")
}

func qlError(query string, idx int, msg string) string {
	fullmsg := fmt.Sprintf("DAS QL ERROR, query=%v, idx=%v, msg=%v", query, idx, msg)
	logs.WithFields(logs.Fields{
		"Query": query,
		"idx":   idx,
		"msg":   msg,
	}).Error("DAS QL error")
	return fullmsg
}
func parseArray(rquery string, odx int, oper string, val string) ([]string, int, string) {
	qlerr := ""
	out := []string{}
	if !(oper == "in" || oper == "between") {
		qlerr = qlError(rquery, odx, "Invalid operator '"+oper+"' for DAS array")
		return out, -1, qlerr
	}
	// we receive relatex query, let's split it by spaces and extract array part
	arr := strings.Split(rquery, " ")
	query := strings.Join(arr[odx:], " ")
	idx := strings.Index(query, "[")
	jdx := strings.Index(query, "]")
	vals := strings.Split(string(query[idx+1:jdx]), ",")
	var values []string
	if oper == "in" {
		values = vals
	} else if oper == "between" {
		minr, e1 := strconv.Atoi(strings.TrimSpace(vals[0]))
		if e1 != nil {
			qlerr = qlError(rquery, odx, fmt.Sprintf("%v", e1))
			return out, -1, qlerr
		}
		maxr, e2 := strconv.Atoi(strings.TrimSpace(vals[1]))
		if e2 != nil {
			qlerr = qlError(rquery, odx, fmt.Sprintf("%v", e2))
			return out, -1, qlerr
		}
		for v := minr; v <= maxr; v++ {
			values = append(values, fmt.Sprintf("%d", v))
		}
	} else {
		qlerr = qlError(rquery, odx, "Invalid operator '"+oper+"' for DAS array")
		return out, -1, qlerr
	}
	for _, v := range values {
		// here we had originally conversion of input value string into integer
		// turns out it is not required since these parameters will be passed
		// to url where we need string type
		val := strings.Replace(v, " ", "", -1)
		out = append(out, val)
	}
	// find position of last bracket in array of tokens
	for key, val := range arr {
		if val == "]" {
			jdx = key
			break
		}
	}
	return out, jdx + 2 - odx, qlerr
}
func parseQuotes(query string, idx int, quote string) (string, int) {
	out := "parseQuotes"
	step := 1
	return out, step
}
func specEntry(key, oper string, val interface{}) bson.M {
	rec := bson.M{}
	if oper == "=" || oper == "last" {
		rec[key] = val
	} else if oper == "in" || oper == "between" {
		rec[key] = val
	}
	return rec
}
func updateSpec(spec, entry bson.M) {
	for key, value := range entry {
		spec[key] = value
	}
}
func qhash(query, inst string) string {
	data := []byte(query + inst)
	arr := md5.Sum(data)
	return hex.EncodeToString(arr[:])
}
func parseLastValue(val string) []string {
	var out []string
	var t0 int64
	v, e := strconv.ParseInt(val[:len(val)-1], 10, 64) // parse string into int64
	if e != nil {
		panic(e)
	}
	if strings.HasSuffix(val, "h") {
		t0 = time.Now().Unix() - v*60*60
	} else if strings.HasSuffix(val, "m") {
		t0 = time.Now().Unix() - v*60
	} else if strings.HasSuffix(val, "s") {
		t0 = time.Now().Unix() - v
	} else if strings.HasSuffix(val, "d") {
		t0 = time.Now().Unix() - v*24*60*60
	} else if strings.HasSuffix(val, "m") {
		t0 = time.Now().Unix() - v*30*24*60*60
	} else if strings.HasSuffix(val, "y") {
		t0 = time.Now().Unix() - v*365*24*60*60
	} else {
		msg := fmt.Sprintf("Unsupported value=%s for last operator", val)
		panic(msg)
	}
	out = append(out, fmt.Sprintf("%d", t0))
	out = append(out, fmt.Sprintf("%d", time.Now().Unix()))
	return out
}

// Parse method provides DAS query parser
func Parse(query, inst string, daskeys []string) (DASQuery, string) {
	time0 := time.Now()
	var qlerr string
	var rec DASQuery
	relaxedQuery := relax(query)
	parts := strings.SplitN(relaxedQuery, "|", 2)
	pipe := ""
	if len(parts) > 1 {
		relaxedQuery = strings.Trim(parts[0], " ")
		pipe = strings.Trim(parts[1], " ")
	}
	nan := "_NA_"
	specials := []string{"date", "system", "instance", "detail"}
	specOps := []string{"in", "between"}
	fields := []string{}
	spec := bson.M{}
	arr := strings.Split(relaxedQuery, " ")
	qlen := len(arr)
	nval := nan
	nnval := nan
	idx := 0
	for idx < qlen {
		val := strings.Replace(string(arr[idx]), " ", "", -1)
		if val == "," {
			idx += 1
			continue
		}
		if idx+1 < qlen {
			nval = strings.Replace(string(arr[idx+1]), " ", "", -1)
		} else {
			nval = nan
		}
		if idx+2 < qlen {
			nnval = strings.Replace(string(arr[idx+2]), " ", "", -1)
		} else {
			nnval = nan
		}
		if utils.VERBOSE > 2 {
			logs.WithFields(logs.Fields{
				"idx":   idx,
				"val":   val,
				"nval":  nval,
				"nnval": nnval,
			}).Debug("process")
		}
		if nval != nan && (nval == "," || utils.InList(nval, daskeys) == true) {
			if utils.InList(val, daskeys) {
				fields = append(fields, val)
			}
			idx += 1
			continue
		} else if utils.InList(nval, operators()) {
			firstNextNextValue := string(nnval[0])
			if !utils.InList(val, append(daskeys, specials...)) {
				qlerr = qlError(relaxedQuery, idx, "Wrong DAS key: "+val)
				return rec, qlerr
			}
			if firstNextNextValue == "[" {
				value, step, qlerr := parseArray(relaxedQuery, idx+2, nval, val)
				if qlerr != "" {
					return rec, qlerr
				}
				updateSpec(spec, specEntry(val, nval, value))
				idx += step
			} else if utils.InList(nval, specOps) {
				msg := "operator " + nval + " should be followed by square bracket"
				qlerr = qlError(relaxedQuery, idx, msg)
				return rec, qlerr
			} else if nval == "last" {
				updateSpec(spec, specEntry(val, nval, parseLastValue(nnval)))
				idx += 2
			} else if firstNextNextValue == "\"" || firstNextNextValue == "'" {
				value, step := parseQuotes(relaxedQuery, idx, firstNextNextValue)
				updateSpec(spec, specEntry(val, nval, value))
				idx += step
			} else {
				updateSpec(spec, specEntry(val, nval, nnval))
				idx += 2
			}
			idx += 1
		} else if nval == nan && nnval == nan {
			if utils.InList(val, daskeys) == true {
				fields = append(fields, val)
				idx += 1
				continue
			} else {
				qlerr = qlError(relaxedQuery, idx, "Not a DAS key")
				return rec, qlerr
			}
		} else {
			qlerr = qlError(relaxedQuery, idx, "unable to parse DAS query")
			return rec, qlerr
		}

	}
	// if no selection keys are given, we'll use spec dictionary keys
	if len(fields) == 0 {
		for key := range spec {
			fields = append(fields, key)
		}
	}
	// remove special keys from fields
	var clean_fields []string
	for _, key := range fields {
		if !utils.InList(key, specials) {
			clean_fields = append(clean_fields, key)
		}
	}
	fields = clean_fields
	filters, aggregators, qlerror := parsePipe(pipe)

	// default DBS instance
	if inst == "" {
		inst = "prod/global"
	}
	// remove instance from spec
	instance := spec["instance"]
	if instance != nil {
		inst = instance.(string)
		delete(spec, "instance")
	}

	// remove detail from spec
	detail := false
	if spec["detail"] != nil || len(filters) != 0 {
		detail = true
		delete(spec, "detail")
	}

	// find out which system to use
	var system string
	if spec["system"] != nil {
		system = spec["system"].(string)
		delete(spec, "system")
	}

	rec.Query = query
	rec.relaxedQuery = relaxedQuery
	rec.Spec = spec
	rec.Fields = fields
	rec.Qhash = qhash(relaxedQuery, inst)
	rec.Pipe = pipe
	rec.Instance = inst
	rec.Detail = detail
	rec.Filters = filters
	rec.Aggregators = aggregators
	rec.System = system
	rec.Time = time0
	return rec, qlerror
}

func parsePipe(pipe string) (map[string][]string, [][]string, string) {
	qlerr := ""
	filters := make(map[string][]string)
	aggregators := [][]string{}
	var item, next, nnext, nnnext, cfilter string
	nan := "_NA_"
	aggrs := []string{"sum", "min", "max", "avg", "median", "count"}
	opers := []string{">", "<", ">=", "<=", "=", "!="}
	idx := 0
	arr := strings.Split(pipe, " ")
	qlen := len(arr)
	if qlen == 0 {
		return filters, aggregators, qlerr
	}
	for idx < qlen {
		item = arr[idx]
		if idx+1 < qlen {
			next = arr[idx+1]
		} else {
			next = nan
		}
		if idx+2 < qlen {
			nnext = arr[idx+2]
		} else {
			nnext = nan
		}
		if idx+3 < qlen {
			nnnext = arr[idx+3]
		} else {
			nnnext = nan
		}
		if item == "grep" {
			cfilter = item
			filters["grep"] = append(filters[item], next)
			idx += 2
		} else if item == "," {
			if cfilter == "grep" {
				if utils.InList(nnext, opers) {
					val := fmt.Sprintf("%s%s%s", next, nnext, nnnext)
					filters[cfilter] = append(filters[cfilter], val)
					idx += 2
				} else {
					filters[cfilter] = append(filters[cfilter], next)
				}
				idx += 2
			} else {
				idx += 1
			}
		} else if item == "sort" {
			cfilter = item
			filters[item] = append(filters[item], next)
			idx += 2
		} else if item == "unique" {
			cfilter = item
			filters[item] = append(filters[item], "1")
			idx += 1
		} else if utils.InList(item, aggrs) {
			cfilter = item
			left := next
			val := nnext
			right := nnnext
			if left != "(" || right != ")" || idx+3 >= qlen {
				msg := "Wrong aggregator representation, please check your query"
				qlerr = qlError(pipe, idx, msg)
				return filters, aggregators, qlerr
			}
			pair := []string{item, val}
			aggregators = append(aggregators, pair)
			idx += 3
		} else {
			idx += 1
		}
	}
	return filters, aggregators, qlerr
}

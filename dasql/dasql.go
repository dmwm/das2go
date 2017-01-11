package dasql

// DAS Query Language (QL) implementation for DAS server
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/vkuznet/das2go/utils"
	"gopkg.in/mgo.v2/bson"
	"strconv"
	"strings"
	"time"
)

type DASQuery struct {
	Query, relaxed_query, Qhash string
	Spec                        bson.M
	Fields                      []string
	Pipe                        string
	Instance                    string
	Filters                     map[string][]string
	Aggregators                 [][]string
	Error                       string
}

// implement own formatter using DASQuery rather then *DASQuery, since
// former will be invoked on both pointer and values and therefore used by fmt/log
// http://stackoverflow.com/questions/16976523/in-go-why-isnt-my-stringer-interface-method-getting-invoked-when-using-fmt-pr
func (q DASQuery) String() string {
	return fmt.Sprintf("<DASQuery=%s, inst=%s, hash=%s>", q.Query, q.Instance, q.Qhash)
}

func operators() []string {
	return []string{"(", ")", ">", "<", "!", "[", "]", ",", "=", "in", "between", "last"}
}
func relax(query string) string {
	for _, oper := range operators() {
		if oper == "in" || oper == "between" {
			continue
		} else {
			new_oper := " " + oper + " "
			query = strings.Replace(query, oper, new_oper, -1)
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
	fmt.Println(fullmsg)
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
func spec_entry(key, oper string, val interface{}) bson.M {
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

// DAS query parser
func Parse(query, inst string, daskeys []string) (DASQuery, string) {
	var qlerr string
	var rec DASQuery
	relaxed_query := relax(query)
	parts := strings.SplitN(relaxed_query, "|", 2)
	pipe := ""
	if len(parts) > 1 {
		relaxed_query = strings.Trim(parts[0], " ")
		pipe = strings.Trim(parts[1], " ")
	}
	nan := "_NA_"
	specials := []string{"date", "system", "instance"}
	spec_ops := []string{"in", "between"}
	fields := []string{}
	spec := bson.M{}
	arr := strings.Split(relaxed_query, " ")
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
		if utils.VERBOSE > 1 {
			fmt.Printf("Process idx='%d', val='%s', nval='%s', nnval='%s'\n", idx, val, nval, nnval)
		}
		if nval != nan && (nval == "," || utils.InList(nval, daskeys) == true) {
			if utils.InList(val, daskeys) {
				fields = append(fields, val)
			}
			idx += 1
			continue
		} else if utils.InList(nval, operators()) {
			first_nnval := string(nnval[0])
			if !utils.InList(val, append(daskeys, specials...)) {
				qlerr = qlError(relaxed_query, idx, "Wrong DAS key: "+val)
				return rec, qlerr
			}
			if first_nnval == "[" {
				value, step, qlerr := parseArray(relaxed_query, idx+2, nval, val)
				if qlerr != "" {
					return rec, qlerr
				}
				updateSpec(spec, spec_entry(val, nval, value))
				idx += step
			} else if utils.InList(nval, spec_ops) {
				msg := "operator " + nval + " should be followed by square bracket"
				qlerr = qlError(relaxed_query, idx, msg)
				return rec, qlerr
			} else if nval == "last" {
				updateSpec(spec, spec_entry(val, nval, parseLastValue(nnval)))
				idx += 2
			} else if first_nnval == "\"" || first_nnval == "'" {
				value, step := parseQuotes(relaxed_query, idx, first_nnval)
				updateSpec(spec, spec_entry(val, nval, value))
				idx += step
			} else {
				updateSpec(spec, spec_entry(val, nval, nnval))
				idx += 2
			}
			idx += 1
		} else if nval == nan && nnval == nan {
			if utils.InList(val, daskeys) == true {
				fields = append(fields, val)
				idx += 1
				continue
			} else {
				qlerr = qlError(relaxed_query, idx, "Not a DAS key")
				return rec, qlerr
			}
		} else {
			qlerr = qlError(relaxed_query, idx, "unable to parse DAS query")
			return rec, qlerr
		}

	}
	// if no selection keys are given, we'll use spec dictionary keys
	if len(fields) == 0 {
		for key := range spec {
			fields = append(fields, key)
		}
	}
	filters, aggregators, qlerror := parsePipe(pipe)

	// remove instance from spec
	instance := spec["instance"]
	if len(inst) == 0 && instance != nil {
		inst = instance.(string)
		delete(spec, "instance")
	}
	if inst == "" {
		inst = "prod/global" // default DBS instance
	}

	rec.Query = query
	rec.relaxed_query = relaxed_query
	rec.Spec = spec
	rec.Fields = fields
	rec.Qhash = qhash(relaxed_query, inst)
	rec.Pipe = pipe
	rec.Instance = inst
	rec.Filters = filters
	rec.Aggregators = aggregators
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
	//     fmt.Println("### pipe", pipe)
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
		//         fmt.Println("### item", idx, item, next, nnext, nnnext, arr, cfilter)
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

/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS QL parser
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 */
package dasql

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
	"strconv"
	"strings"
	"utils"
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
	return fmt.Sprintf("<DASQuery=%s, hash=%s>", q.Query, q.Qhash)
}

func operators() []string {
	return []string{"(", ")", ">", "<", "!", "[", "]", ",", "="}
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
	log.Println(fullmsg)
	return fullmsg
}
func parseArray(query string, idx int, oper string, val string) ([]int, int, string) {
	qlerr := ""
	out := []int{}
	if oper != "in" || oper != "between" {
		qlerr = qlError(query, idx, "Invalid operator '"+oper+"' for DAS array")
		return out, -1, qlerr
	}
	query = string(query[idx : len(query)-1])
	idx = strings.Index(query, "[")
	jdx := strings.Index(query, "]")
	values := strings.Split(string(query[idx+1:jdx]), ",")
	for _, v := range values {
		val, err := strconv.Atoi(strings.Replace(v, " ", "", -1))
		if err != nil {
			qlError(query, idx, "Fail to parse array value: "+v)
		}
		out = append(out, val)
	}
	return out, jdx + 1, qlerr
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
	}
	return rec
}
func updateSpec(spec, entry bson.M) {
	for key, value := range entry {
		spec[key] = value
	}
}
func qhash(query string) string {
	data := []byte(query)
	arr := md5.Sum(data)
	return hex.EncodeToString(arr[:])
}

// TODO: I need to add pipe parsing
//
func Parse(query string) (DASQuery, string) {
	var qlerr string
	var rec DASQuery
	relaxed_query := relax(query)
	parts := strings.SplitN(relaxed_query, "|", 2)
	pipe := ""
	if len(parts) > 1 {
		pipe = parts[len(parts)-1]
	}
	nan := "_NA_"
	instance := "prod/global"
	daskeys := []string{"file", "dataset", "lumi", "run"}
	specials := []string{"date", "system", "instance"}
	spec_ops := []string{"in", "between"}
	fields := []string{}
	spec := bson.M{}
	arr := strings.Split(parts[0], " ")
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
		//         log.Printf("Process idx='%d', val='%s', nval='%s', nnval='%s'\n", idx, val, nval, nnval)
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
				value, step, qlerr := parseArray(relaxed_query, idx, nval, val)
				if qlerr != "" {
					return rec, qlerr
				}
				updateSpec(spec, spec_entry(val, nval, value))
				idx += step
			} else if utils.InList(nval, spec_ops) {
				msg := "operator " + nval + " should be followed by square bracket"
				qlerr = qlError(relaxed_query, idx, msg)
				return rec, qlerr
			} else if first_nnval == "\"" || first_nnval == "'" {
				value, step := parseQuotes(relaxed_query, idx, first_nnval)
				updateSpec(spec, spec_entry(val, nval, value))
				idx += step
			} else {
				updateSpec(spec, spec_entry(val, nval, nnval))
				idx += 3
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
			qlerr = qlError(relaxed_query, idx, "We should not be here")
			return rec, qlerr
		}

	}
	// if no selection keys are given, we'll use spec dictionary keys
	if len(fields) == 0 {
		for key, _ := range spec {
			fields = append(fields, key)
		}
	}
	filters, aggregators, qlerror := parsePipe(pipe)

	rec.Query = query
	rec.relaxed_query = relaxed_query
	rec.Spec = spec
	rec.Fields = fields
	rec.Qhash = qhash(relaxed_query)
	rec.Pipe = pipe
	rec.Instance = instance
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
	aggrs := []string{"sum", "min", "max", "avg"}
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
			}
			idx += 2
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

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
	"labix.org/v2/mgo/bson"
	"log"
	"strconv"
	"strings"
	"utils"
)

type DASQuery struct {
	Query, relaxed_query, Qhash string
	Spec                        bson.M
	Fields                      []string
	pipe                        string
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

func ql_error(query string, idx int, msg string) {
	log.Printf("query=%v, idx=%v, msg=%v", query, idx, msg)
	log.Fatal("QL error")
}
func parse_array(query string, idx int, oper string, val string) ([]int, int) {
	if oper != "in" || oper != "between" {
		ql_error(query, idx, "Invalid operator '"+oper+"' for DAS array")
	}
	query = string(query[idx : len(query)-1])
	idx = strings.Index(query, "[")
	jdx := strings.Index(query, "]")
	out := []int{}
	values := strings.Split(string(query[idx+1:jdx]), ",")
	for _, v := range values {
		val, err := strconv.Atoi(strings.Replace(v, " ", "", -1))
		if err != nil {
			ql_error(query, idx, "Fail to parse array value: "+v)
		}
		out = append(out, val)
	}
	return out, jdx + 1
}
func parse_quotes(query string, idx int, quote string) (string, int) {
	out := "parse_quotes"
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
func update_spec(spec, entry bson.M) {
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
func Parse(query string) DASQuery {
	relaxed_query := relax(query)
	parts := strings.SplitN(relaxed_query, "|", 2)
	pipe := ""
	if len(parts) > 1 {
		pipe = parts[len(parts)-1]
	}
	nan := "_NA_"
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
				ql_error(relaxed_query, idx, "Wrong DAS key: "+val)
			}
			if first_nnval == "[" {
				value, step := parse_array(relaxed_query, idx, nval, val)
				update_spec(spec, spec_entry(val, nval, value))
				idx += step
			} else if utils.InList(nval, spec_ops) {
				msg := "operator " + nval + " should be followed by square bracket"
				ql_error(relaxed_query, idx, msg)
			} else if first_nnval == "\"" || first_nnval == "'" {
				value, step := parse_quotes(relaxed_query, idx, first_nnval)
				update_spec(spec, spec_entry(val, nval, value))
				idx += step
			} else {
				update_spec(spec, spec_entry(val, nval, nnval))
				idx += 3
			}
			idx += 1
		} else if nval == nan && nnval == nan {
			if utils.InList(val, daskeys) == true {
				fields = append(fields, val)
				idx += 1
				continue
			} else {
				ql_error(relaxed_query, idx, "Not a DAS key")
			}
		} else {
			ql_error(relaxed_query, idx, "We should not be here")
		}

	}

	var rec DASQuery
	rec.Query = query
	rec.relaxed_query = relaxed_query
	rec.Spec = spec
	rec.Fields = fields
	rec.Qhash = qhash(relaxed_query)
	rec.pipe = pipe
	return rec
}

func parse_pipe(query string, filters []string, aggregators []string) {
}

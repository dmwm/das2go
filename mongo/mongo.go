package mongo

// DAS mongo module
//
// Copyright (c) 2015-2016 - Valentin Kuznetsov <vkuznet AT gmail dot com>
//
// References : https://gist.github.com/boj/5412538
//              https://gist.github.com/border/3489566

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"log"
	"strings"
	"time"

	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/utils"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// DASRecord define DAS record
type DASRecord map[string]interface{}

// ToString provides string representation of DAS record
func (r DASRecord) ToString() string {
	var out []string
	for _, k := range utils.MapKeys(r) {
		switch v := r[k].(type) {
		case int:
			out = append(out, fmt.Sprintf("%s:%d", k, v))
		case float64:
			d := int(v)
			if float64(d) == v {
				out = append(out, fmt.Sprintf("%s:%d", k, d))
			} else {
				out = append(out, fmt.Sprintf("%s:%f", k, v))
			}
		default:
			s := fmt.Sprintf("%s:%#v", k, r[k])
			out = append(out, strings.Replace(s, ", ", ",\n   ", -1))
		}
	}
	return strings.Join(out, "\n")
}

// ToHtml provides string representation of DAS record
func (r DASRecord) ToHtml() string {
	var out []string
	for _, k := range utils.MapKeys(r) {
		switch v := r[k].(type) {
		case int:
			out = append(out, fmt.Sprintf("%s:%d\n", k, v))
		case float64:
			d := int(v)
			if float64(d) == v {
				out = append(out, fmt.Sprintf("%s:%d\n", k, d))
			} else {
				out = append(out, fmt.Sprintf("%s:%f\n", k, v))
			}
		default:
			s := fmt.Sprintf("%s:%#v\n", k, r[k])
			out = append(out, strings.Replace(s, ", ", ",\n   ", -1))
		}
	}
	return strings.Join(out, "\n")
}

// DASErrorRecord provides DAS error record
func DASErrorRecord(msg, etype string, ecode int) DASRecord {
	erec := make(DASRecord)
	erec["error"] = html.EscapeString(msg)
	erec["type"] = html.EscapeString(etype)
	erec["code"] = ecode
	return erec
}

// GetValue function to get int value from DAS record for given key
func GetValue(rec DASRecord, key string) interface{} {
	var val DASRecord
	keys := strings.Split(key, ".")
	if len(keys) > 1 {
		value, ok := rec[keys[0]]
		if !ok {
			log.Printf("unable to find key-value, record %+v, key\n", rec, key)
			return ""
		}
		switch v := value.(type) {
		case DASRecord:
			val = v
		case []DASRecord:
			if len(v) > 0 {
				val = v[0]
			} else {
				return ""
			}
		case []interface{}:
			vvv := v[0]
			if vvv != nil {
				val = vvv.(DASRecord)
			} else {
				return ""
			}
		default:
			log.Printf("ERROR: unknown type %T, das record %+v, mongo record %+v, key %v, keys %v\n", v, v, rec, key, keys)
			return ""
		}
		if len(keys) == 2 {
			return GetValue(val, keys[1])
		}
		return GetValue(val, strings.Join(keys[1:], "."))
	}
	value := rec[key]
	return value
}

// helper function to return single entry (e.g. from a list) of given value
func singleEntry(data interface{}) interface{} {
	switch v := data.(type) {
	case []interface{}:
		return v[0]
	default:
		return v
	}
}

// GetStringValue function to get string value from DAS record for given key
func GetStringValue(rec DASRecord, key string) (string, error) {
	value := GetValue(rec, key)
	val := fmt.Sprintf("%v", value)
	return val, nil
}

// GetSingleStringValue function to get string value from DAS record for given key
func GetSingleStringValue(rec DASRecord, key string) (string, error) {
	value := singleEntry(GetValue(rec, key))
	val := fmt.Sprintf("%v", value)
	return val, nil
}

// GetIntValue function to get int value from DAS record for given key
func GetIntValue(rec DASRecord, key string) (int, error) {
	value := GetValue(rec, key)
	val, ok := value.(int)
	if ok {
		return val, nil
	}
	return 0, fmt.Errorf("Unable to cast value for key '%s'", key)
}

// GetInt64Value function to get int value from DAS record for given key
func GetInt64Value(rec DASRecord, key string) (int64, error) {
	value := GetValue(rec, key)
	out, ok := value.(int64)
	if ok {
		return out, nil
	}
	return 0, fmt.Errorf("Unable to cast value for key '%s'", key)
}

// MongoConnection defines connection to MongoDB
type MongoConnection struct {
	Session *mgo.Session
}

// Connect provides connection to MongoDB
func (m *MongoConnection) Connect() *mgo.Session {
	var err error
	if m.Session == nil {
		m.Session, err = mgo.Dial(config.Config.Uri)
		if err != nil {
			panic(err)
		}
		//         m.Session.SetMode(mgo.Monotonic, true)
		m.Session.SetMode(mgo.Strong, true)
	}
	return m.Session.Clone()
}

// global object which holds MongoDB connection
var _Mongo MongoConnection

// Insert records into MongoDB
func Insert(dbname, collname string, records []DASRecord) {

	// defer function profiler
	defer utils.MeasureTime("mongo/Insert")()

	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	for _, rec := range records {
		if err := c.Insert(&rec); err != nil {
			log.Println("Fail to insert DAS record", err)
		}
	}
}

// Get records from MongoDB
func Get(dbname, collname string, spec bson.M, idx, limit int) []DASRecord {

	// defer function profiler
	defer utils.MeasureTime("mongo/Get")()

	out := []DASRecord{}
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	var err error
	if limit > 0 {
		err = c.Find(spec).Skip(idx).Limit(limit).All(&out)
	} else {
		err = c.Find(spec).Skip(idx).All(&out)
	}
	if err != nil {
		log.Println("ERROR: unable to get records", err)
	}
	return out
}

// GetSorted records from MongoDB sorted by given key
func GetSorted(dbname, collname string, spec bson.M, skeys []string) []DASRecord {

	// defer function profiler
	defer utils.MeasureTime("mongo/GetSorted")()

	out := []DASRecord{}
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	err := c.Find(spec).Sort(skeys...).All(&out)
	if err != nil {
		log.Println("unable to sort recrds", err)
		// try to fetch all unsorted data
		err = c.Find(spec).All(&out)
		if err != nil {
			log.Println("ERROR: unable to find records", err)
			out = append(out, DASErrorRecord(fmt.Sprintf("%v", err), utils.MongoDBErrorName, utils.MongoDBError))
		}
	}
	return out
}

// helper function to present in bson selected fields
func sel(q ...string) (r bson.M) {
	r = make(bson.M, len(q))
	for _, s := range q {
		r[s] = 1
	}
	return
}

// GetFilteredSorted get records from MongoDB filtered and sorted by given key
func GetFilteredSorted(dbname, collname string, spec bson.M, fields, skeys []string, idx, limit int) []DASRecord {

	// defer function profiler
	defer utils.MeasureTime("mongo/GetFiltered/Sorted")()

	out := []DASRecord{}
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	var err error
	fields = append(fields, "das") // always extract das part of the record
	if limit > 0 {
		if len(skeys) > 0 {
			err = c.Find(spec).Skip(idx).Limit(limit).Select(sel(fields...)).Sort(skeys...).All(&out)
		} else {
			err = c.Find(spec).Skip(idx).Limit(limit).Select(sel(fields...)).All(&out)
		}
	} else {
		if len(skeys) > 0 {
			err = c.Find(spec).Select(sel(fields...)).Sort(skeys...).All(&out)
		} else {
			err = c.Find(spec).Select(sel(fields...)).All(&out)
		}
	}
	if err != nil {
		log.Println("ERROR: unable to fetch from MOngoDB", time.Now(), err)
	}
	return out
}

// Update inplace for given spec
func Update(dbname, collname string, spec, newdata bson.M) {

	// defer function profiler
	defer utils.MeasureTime("mongo/Update")()

	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	err := c.Update(spec, newdata)
	if err != nil {
		log.Printf("ERROR: unable to update record, spec %v, data %+v, error %v\n", spec, newdata, err)
	}
}

// Count gets number records from MongoDB
func Count(dbname, collname string, spec bson.M) int {

	// defer function profiler
	defer utils.MeasureTime("mongo/Count")()

	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	nrec, err := c.Find(spec).Count()
	if err != nil {
		log.Printf("ERROR: unable to count records, spec %+v, error %v\n", spec, err)
	}
	return nrec
}

// Remove records from MongoDB
func Remove(dbname, collname string, spec bson.M) {

	// defer function profiler
	defer utils.MeasureTime("mongo/Remove")()

	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	_, err := c.RemoveAll(spec)
	if err != nil && err != mgo.ErrNotFound {
		log.Printf("ERROR: untable to remove records, spec %+v, error %v\n", spec, err)
	}
}

// LoadJsonData stream from series of bytes
func LoadJsonData(data []byte) DASRecord {
	r := make(DASRecord)
	err := json.Unmarshal(data, &r)
	if err != nil {
		log.Printf("ERROR: unable to unmarshal records, data %v, error %v\n", string(data), err)
	}
	return r
}

// CreateIndexes creates DAS cache indexes
func CreateIndexes(dbname, collname string, keys []string) {
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	for _, key := range keys {
		index := mgo.Index{
			Key:        []string{key},
			Unique:     false,
			Background: true,
			//             Sparse:     true,
		}
		err := c.EnsureIndex(index)
		if err != nil {
			log.Printf("ERROR: unable to ensure index, index %v, error %v\n", index, err)
		}
	}
}

// GetBytesFromDASRecord converts DASRecord map into bytes
func GetBytesFromDASRecord(data DASRecord) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Convert2DASRecord converts given interface to DAS Record data type
func Convert2DASRecord(item interface{}) DASRecord {
	switch r := item.(type) {
	case map[string]interface{}:
		rec := make(DASRecord)
		for kkk, vvv := range r {
			rec[kkk] = vvv
		}
		return rec
	case DASRecord:
		return r
	}
	return nil
}

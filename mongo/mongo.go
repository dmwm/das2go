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
	"log"
	"strings"
	"time"

	"github.com/dmwm/das2go/config"
	"github.com/dmwm/das2go/utils"
	logs "github.com/sirupsen/logrus"
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
func DASErrorRecord(msg string) DASRecord {
	erec := make(DASRecord)
	erec["error"] = msg
	return erec
}

// GetValue function to get int value from DAS record for given key
func GetValue(rec DASRecord, key string) interface{} {
	var val DASRecord
	keys := strings.Split(key, ".")
	if len(keys) > 1 {
		switch v := rec[keys[0]].(type) {
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
			logs.WithFields(logs.Fields{
				"Time": time.Now(),
				"Type": fmt.Sprintf("%T", v),
				"data": v,
			}).Error("Unknown type")
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

// GetStringValue function to get string value from DAS record for given key
func GetStringValue(rec DASRecord, key string) (string, error) {
	value := GetValue(rec, key)
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
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Error("Unable to get records")
	}
	return out
}

// GetSorted records from MongoDB sorted by given key
func GetSorted(dbname, collname string, spec bson.M, skeys []string) []DASRecord {
	out := []DASRecord{}
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	err := c.Find(spec).Sort(skeys...).All(&out)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Error": err,
		}).Warn("Unable to sort records")
		// try to fetch all unsorted data
		err = c.Find(spec).All(&out)
		if err != nil {
			logs.WithFields(logs.Fields{
				"Error": err,
			}).Error("Unable to find records")
			out = append(out, DASErrorRecord(fmt.Sprintf("%v", err)))
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
		logs.WithFields(logs.Fields{"Time": time.Now(), "Error": err}).Error("Unable to fetch from MongoDB")
	}
	return out
}

// Update inplace for given spec
func Update(dbname, collname string, spec, newdata bson.M) {
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	err := c.Update(spec, newdata)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Time":  time.Now(),
			"Error": err,
			"Spec":  spec,
			"data":  newdata,
		}).Error("Unable to update record")
	}
}

// Count gets number records from MongoDB
func Count(dbname, collname string, spec bson.M) int {
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	nrec, err := c.Find(spec).Count()
	if err != nil {
		logs.WithFields(logs.Fields{
			"Time":  time.Now(),
			"Error": err,
			"Spec":  spec,
		}).Error("Unable to count records")
	}
	return nrec
}

// Remove records from MongoDB
func Remove(dbname, collname string, spec bson.M) {
	s := _Mongo.Connect()
	defer s.Close()
	c := s.DB(dbname).C(collname)
	_, err := c.RemoveAll(spec)
	if err != nil && err != mgo.ErrNotFound {
		logs.WithFields(logs.Fields{
			"Time":  time.Now(),
			"Error": err,
			"Spec":  spec,
		}).Error("Unable to remove records")
	}
}

// LoadJsonData stream from series of bytes
func LoadJsonData(data []byte) DASRecord {
	r := make(DASRecord)
	err := json.Unmarshal(data, &r)
	if err != nil {
		logs.WithFields(logs.Fields{
			"Time":  time.Now(),
			"Error": err,
			"Data":  string(data),
		}).Error("Unable to unmarshal records")
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
			logs.WithFields(logs.Fields{
				"Time":  time.Now(),
				"Error": err,
				"Index": index,
			}).Error("Unable to ensure index")
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

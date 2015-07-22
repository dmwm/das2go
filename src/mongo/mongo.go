/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: DAS mongo module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 * References : https://gist.github.com/boj/5412538
 *              https://gist.github.com/border/3489566
 */
package mongo

import (
	"config"
	"encoding/json"
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"strings"
)

type DASRecord map[string]interface{}

// function to get string value from DAS record for given key
func GetStringValue(rec DASRecord, key string) (string, error) {
	keys := strings.Split(key, ".")
	if len(keys) > 1 {
		val := rec[keys[0]].(DASRecord)
		if len(keys) == 2 {
			return GetStringValue(val, keys[1])
		}
		return GetStringValue(val, strings.Join(keys[1:len(keys)], "."))
	}
	value := rec[key]
	val, ok := value.(string)
	if ok {
		return val, nil
	}
	return "", fmt.Errorf("Unable to cast value for key '%s'", key)
}

// function to get int value from DAS record for given key
func GetIntValue(rec DASRecord, key string) (int, error) {
	keys := strings.Split(key, ".")
	if len(keys) > 1 {
		val := rec[keys[0]].(DASRecord)
		if len(keys) == 2 {
			return GetIntValue(val, keys[1])
		}
		return GetIntValue(val, strings.Join(keys[1:len(keys)], "."))
	}
	value := rec[key]
	val, ok := value.(int)
	if ok {
		return val, nil
	}
	return 0, fmt.Errorf("Unable to cast value for key '%s'", key)
}

// function to get int value from DAS record for given key
func GetInt64Value(rec DASRecord, key string) (int64, error) {
	keys := strings.Split(key, ".")
	if len(keys) > 1 {
		val := rec[keys[0]].(DASRecord)
		if len(keys) == 2 {
			return GetInt64Value(val, keys[1])
		}
		return GetInt64Value(val, strings.Join(keys[1:len(keys)], "."))
	}
	value := rec[key]
	val, ok := value.(int64)
	if ok {
		return val, nil
	}
	return 0, fmt.Errorf("Unable to cast value for key '%s'", key)
}

// insert into MongoDB
func Insert(dbname, collname string, records []DASRecord) {
	session, err := mgo.Dial(config.Uri())
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(dbname).C(collname)
	for _, rec := range records {
		if err := c.Insert(&rec); err != nil {
			log.Println("Fail to insert DAS record", err)
		}
	}
}

// get records from MongoDB
func Get(dbname, collname string, spec bson.M, idx, limit int) []DASRecord {
	out := []DASRecord{}
	session, err := mgo.Dial(config.Uri())
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(dbname).C(collname)
	if limit > 0 {
		err = c.Find(spec).Skip(idx).Limit(limit).All(&out)
	} else {
		err = c.Find(spec).Skip(idx).All(&out)
	}
	if err != nil {
		panic(err)
	}
	return out
}

// get records from MongoDB sorted by given key
func GetSorted(dbname, collname string, spec bson.M, skey string) []DASRecord {
	out := []DASRecord{}
	session, err := mgo.Dial(config.Uri())
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(dbname).C(collname)
	err = c.Find(spec).Sort(skey).All(&out)
	if err != nil {
		panic(err)
	}
	return out
}

// update inplace for given spec
func Update(dbname, collname string, spec, newdata bson.M) {
	session, err := mgo.Dial(config.Uri())
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(dbname).C(collname)
	err = c.Update(spec, newdata)
	if err != nil {
		panic(err)
	}
}

// get number records from MongoDB
func Count(dbname, collname string, spec bson.M) int {
	session, err := mgo.Dial(config.Uri())
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(dbname).C(collname)
	nrec := 0
	nrec, err = c.Find(spec).Count()
	if err != nil {
		panic(err)
	}
	return nrec
}

// remove records from MongoDB
func Remove(dbname, collname string, spec bson.M) {
	session, err := mgo.Dial(config.Uri())
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(dbname).C(collname)
	_, err = c.RemoveAll(spec)
	if err != nil && err != mgo.ErrNotFound {
		panic(err)
	}
}

func LoadJsonData(data []byte) DASRecord {
	r := make(DASRecord)
	err := json.Unmarshal(data, &r)
	if err != nil {
		panic(err)
	}
	return r
}

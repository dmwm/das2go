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
	"encoding/json"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

type DASRecord map[string]interface{}

func GetStringValue(rec DASRecord, key string) string {
	value := rec[key]
	val, ok := value.(string)
	if ok {
		return val
	}
	panic("Wrong type")
}
func GetIntValue(rec DASRecord, key string) int {
	value := rec[key]
	val, ok := value.(int)
	if ok {
		return val
	}
	panic("Wrong type")
}

// insert into MongoDB
func Insert(uri, dbname, collname string, records []DASRecord) {
	session, err := mgo.Dial(uri)
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
	//     if err := c.Insert(&records); err != nil {
	//         log.Println("Fail to insert DAS record", err)
	//     }
}

// get records from MongoDB
func Get(uri, dbname, collname string, spec bson.M) []DASRecord {
	out := []DASRecord{}
	session, err := mgo.Dial(uri)
	if err != nil {
		panic(err)
	}
	defer session.Close()
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(dbname).C(collname)
	err = c.Find(spec).All(&out)
	if err != nil {
		panic(err)
	}
	return out
}

// get records from MongoDB sorted by given key
func GetSorted(uri, dbname, collname string, spec bson.M, skey string) []DASRecord {
	out := []DASRecord{}
	session, err := mgo.Dial(uri)
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
func Update(uri, dbname, collname string, spec, newdata bson.M) {
	session, err := mgo.Dial(uri)
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
func Count(uri, dbname, collname string, spec bson.M) int {
	session, err := mgo.Dial(uri)
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
func Remove(uri, dbname, collname string, spec bson.M) {
	session, err := mgo.Dial(uri)
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

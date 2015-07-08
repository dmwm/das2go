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
	if err := c.Insert(&records); err != nil {
		log.Println("Faile to isert DAS record", err)
	}
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

func LoadJsonData(data []byte) DASRecord {
	r := make(DASRecord)
	err := json.Unmarshal(data, &r)
	if err != nil {
		panic(err)
	}
	return r
}

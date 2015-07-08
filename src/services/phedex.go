/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: Phedex module
 * Created    : Fri Jun 26 14:25:01 EDT 2015
 *
 */
package services

import (
	"encoding/json"
	"mongo"
)

/*
type filereplicas struct {
	file    interface{} `xml:file`
	replica interface{} `xml:replica`
}

func PhedexUnmarshal(data []byte, api string) mongo.DASRecord {
	var rec mongo.DASRecord
	var freplica filereplicas
	log.Println("### call", api)
	if api == "filereplicas" || api == "fileReplicas4dataset" {
		err := xml.Unmarshal(data, &freplica)
		if err != nil {
			log.Println("ERROR", api, "unable to unmarshal the data")
		}
		log.Println(freplica)
	}
	return rec
}
*/

// helper function to load data stream and return DAS records
func loadPhedexData(data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	var rec mongo.DASRecord
	err := json.Unmarshal(data, &rec)
	if err != nil {
		panic(err)
	}
	out = append(out, rec)
	return out
}

// Unmarshal Phedex data stream and return DAS records based on api
func PhedexUnmarshal(api string, data []byte) []mongo.DASRecord {
	rec := loadPhedexData(data)
	return rec
}

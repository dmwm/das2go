/*
 *
 * Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
 * Description: RunRegistry module
 * Created    : Fri Jun 26 14:25:01 EDT 2015

 DAS calls
http://localhost:8081/runregistry/api/GLOBAL/runsummary/json/number%2CstartTime%2CstopTime%2Ctriggers%2CrunClassName%2CrunStopReason%2Cbfield%2CgtKey%2Cl1Menu%2ChltKeyDescription%2ClhcFill%2ClhcEnergy%2CrunCreated%2Cmodified%2ClsCount%2ClsRanges/none/data {"filter": {"number": ">= 165103 and <= 165110"}}

 *
*/
package services

import (
	"encoding/json"
	"fmt"
	"mongo"
)

// helper function to load RunRegistry data stream
func loadRunRegistryData(api string, data []byte) []mongo.DASRecord {
	var out []mongo.DASRecord
	err := json.Unmarshal(data, &out)
	if err != nil {
		msg := fmt.Sprintf("RunRegistry unable to unmarshal the data into DAS record, api=%s, data=%s, error=%v", api, string(data), err)
		panic(msg)
	}
	return out
}

// Unmarshal RunRegistry data stream and return DAS records based on api
func RunRegistryUnmarshal(api string, data []byte) []mongo.DASRecord {
	records := loadRunRegistryData(api, data)
	return records
}

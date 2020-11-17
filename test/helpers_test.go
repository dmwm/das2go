package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/services"
)

func TestOrderByRunLumis(t *testing.T) {
	var records []mongo.DASRecord
	var vals []interface{}
	vals = append(vals, json.Number("1"))
	var run interface{}
	run = json.Number("1")
	rec := mongo.DASRecord{"run": mongo.DASRecord{"run_number": run}, "lumi": mongo.DASRecord{"number": vals}}
	records = append(records, rec)
	vals = append(vals, json.Number("2"))
	rec = mongo.DASRecord{"run": mongo.DASRecord{"run_number": run}, "lumi": mongo.DASRecord{"number": vals}}
	records = append(records, rec)
	fmt.Println("records", records)
	results := services.OrderByRunLumis(records)
	var runs, lumis []json.Number
	for _, r := range results {
		fmt.Println("rec", r)
		runs = append(runs, mongo.GetValue(r, "run.run_number").(json.Number))
		for _, l := range mongo.GetValue(r, "lumi.number").([]json.Number) {
			lumis = append(lumis, l)
		}
	}
	if len(runs) != 1 {
		fmt.Println("runs", runs)
		t.Error("Fail to collect runs in OrderByRunLumis")
	}
	if len(lumis) != 3 {
		fmt.Println("lumis", lumis)
		t.Error("Fail to collect lumis in OrderByRunLumis")
	}
}

package main

import (
	"fmt"
	"testing"

	"github.com/dmwm/das2go/mongo"
	"github.com/dmwm/das2go/services"
)

func TestOrderByRunLumis(t *testing.T) {
	var records []mongo.DASRecord
	rec := mongo.DASRecord{"run": mongo.DASRecord{"run_number": float64(1)}, "lumi": mongo.DASRecord{"number": []float64{1}}}
	records = append(records, rec)
	rec = mongo.DASRecord{"run": mongo.DASRecord{"run_number": float64(1)}, "lumi": mongo.DASRecord{"number": []float64{2, 3}}}
	records = append(records, rec)
	fmt.Println("records", records)
	results := services.OrderByRunLumis(records)
	var runs, lumis []float64
	for _, r := range results {
		fmt.Println("rec", r)
		runs = append(runs, mongo.GetValue(r, "run.run_number").(float64))
		for _, l := range mongo.GetValue(r, "lumi.number").([]float64) {
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

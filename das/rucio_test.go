package das

import (
	"testing"

	"github.com/dmwm/das2go/dasql"
	"github.com/dmwm/das2go/mongo"
	"gopkg.in/mgo.v2/bson"
)

func TestRucioDatasetDIDsURL(t *testing.T) {
	dmap := mongo.DASRecord{"url": "https://cms-rucio.cern.ch/replicas/cms"}
	got := rucioDatasetDIDsURL("/Primary/Processed/TIER", dmap)
	want := "https://cms-rucio.cern.ch/dids/cms/Primary/Processed/TIER/dids"
	if got != want {
		t.Fatalf("rucioDatasetDIDsURL() = %q, want %q", got, want)
	}
}

func TestRucioDatasetDIDsURLPreservesPrefixAndEscapesBlockSeparator(t *testing.T) {
	dmap := mongo.DASRecord{"url": "https://cmsweb.cern.ch/rucio/replicas/cms"}
	got := rucioDatasetDIDsURL([]string{"/Primary/Processed/TIER#suffix"}, dmap)
	want := "https://cmsweb.cern.ch/rucio/dids/cms/Primary/Processed/TIER%23suffix/dids"
	if got != want {
		t.Fatalf("rucioDatasetDIDsURL() = %q, want %q", got, want)
	}
}

func TestDatasetFileVariantsStartWithSameBlockResolution(t *testing.T) {
	want := "https://cms-rucio.cern.ch/dids/cms/Primary/Processed/TIER/dids"
	for _, urn := range []string{"file4dataset", "file4dataset_site"} {
		dmap := mongo.DASRecord{
			"system": "rucio",
			"urn":    urn,
			"url":    "https://cms-rucio.cern.ch/replicas/cms",
			"expire": 3600,
			"lookup": "file",
			"das_map": []interface{}{
				map[string]interface{}{"das_key": "file", "rec_key": "file.name"},
			},
		}
		query := dasql.DASQuery{Spec: bson.M{"dataset": "/Primary/Processed/TIER"}}
		if urn == "file4dataset_site" {
			query.Spec["site"] = "T1_TEST_Disk"
		}
		_, _, urls, _ := ProcessLogic(query, []mongo.DASRecord{dmap}, nil)
		if _, ok := urls[want]; !ok || len(urls) != 1 {
			t.Errorf("%s URLs = %v, want only %q", urn, urls, want)
		}
	}
}

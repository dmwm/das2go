package services

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/dmwm/das2go/dasql"
	"gopkg.in/mgo.v2/bson"
)

func TestRucioFilesForDatasetCompleteAndPartialBlocks(t *testing.T) {
	var mutex sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		requests = append(requests, r.URL.EscapedPath()+"?"+r.URL.RawQuery)
		mutex.Unlock()
		w.Header().Set("Content-Type", "application/x-json-stream")
		switch {
		case strings.Contains(r.URL.Path, "complete") && strings.HasSuffix(r.URL.Path, "/datasets"):
			fmt.Fprintln(w, `{"rse":"T1_TEST_Disk","length":2,"available_length":2}`)
		case strings.Contains(r.URL.Path, "complete") && strings.HasPrefix(r.URL.Path, "/dids/"):
			fmt.Fprintln(w, `{"name":"/store/complete-1.root","type":"FILE"}`)
			fmt.Fprintln(w, `{"name":"/store/complete-2.root","type":"FILE"}`)
		case strings.Contains(r.URL.Path, "partial") && strings.HasSuffix(r.URL.Path, "/datasets"):
			fmt.Fprintln(w, `{"rse":"T1_TEST_Disk","length":3,"available_length":1}`)
		case strings.Contains(r.URL.Path, "partial") && strings.HasPrefix(r.URL.Path, "/replicas/"):
			fmt.Fprintln(w, `{"name":"/store/partial-at-site.root","states":{"T1_TEST_Disk":"AVAILABLE"}}`)
			fmt.Fprintln(w, `{"name":"/store/partial-elsewhere.root","states":{"T2_TEST_Disk":"AVAILABLE"}}`)
			fmt.Fprintln(w, `{"name":"/store/partial-deleting.root","states":{"T1_TEST_Disk":"BEING_DELETED"}}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()
	t.Setenv("RUCIO_URL", server.URL)

	query := dasql.DASQuery{Spec: bson.M{
		"dataset": "/Primary/Processed/TIER",
		"site":    "T1_TEST_Disk",
	}}
	data := []byte("{\"name\":\"/Primary/Processed/TIER#complete\"}\n" +
		"{\"name\":\"/Primary/Processed/TIER#partial\"}\n")
	records := RucioUnmarshal(query, "file4dataset_site", data)

	var names []string
	for _, rec := range records {
		if name, ok := rec["name"].(string); ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	want := []string{
		"/store/complete-1.root",
		"/store/complete-2.root",
		"/store/partial-at-site.root",
	}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("file names = %v, want %v", names, want)
	}

	mutex.Lock()
	gotRequests := append([]string(nil), requests...)
	mutex.Unlock()
	assertRequestContaining(t, gotRequests, "complete", "/datasets?deep=True")
	assertRequestContaining(t, gotRequests, "complete", "/dids/")
	assertRequestContaining(t, gotRequests, "partial", "/datasets?deep=True")
	assertRequestContaining(t, gotRequests, "partial", "/replicas/")
	for _, request := range gotRequests {
		if strings.Contains(request, "complete") && strings.HasPrefix(request, "/replicas/") && !strings.Contains(request, "/datasets?") {
			t.Fatalf("complete block unexpectedly used file-level fallback: %s", request)
		}
		if strings.Contains(request, "partial") && strings.HasPrefix(request, "/dids/") {
			t.Fatalf("partial block unexpectedly used compact block contents: %s", request)
		}
	}
}

func TestRucioBlockCompletenessUsesRequestedSite(t *testing.T) {
	records := loadRucioData("test", []byte(
		"{\"rse\":\"T1_FULL_Disk\",\"length\":4,\"available_length\":4}\n"+
			"{\"rse\":\"T2_PARTIAL_Disk\",\"length\":4,\"available_length\":2}\n"))
	tests := []struct {
		site     string
		relevant bool
		complete bool
	}{
		{"", true, true},
		{"T1_FULL_Disk", true, true},
		{"T2_PARTIAL_Disk", true, false},
		{"T2_*", true, false},
		{"T3_MISSING_Disk", false, false},
	}
	for _, test := range tests {
		relevant, complete := rucioBlockCompleteness(test.site, records)
		if relevant != test.relevant || complete != test.complete {
			t.Errorf("site %q: got (%v, %v), want (%v, %v)", test.site, relevant, complete, test.relevant, test.complete)
		}
	}
}

func TestDatasetFileVariantsDifferOnlyBySiteFilter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-json-stream")
		if strings.HasSuffix(r.URL.Path, "/datasets") {
			fmt.Fprintln(w, `{"rse":"T1_TEST_Disk","length":2,"available_length":1}`)
			return
		}
		fmt.Fprintln(w, `{"name":"/store/at-t1.root","states":{"T1_TEST_Disk":"AVAILABLE"}}`)
		fmt.Fprintln(w, `{"name":"/store/at-t2.root","states":{"T2_TEST_Disk":"AVAILABLE"}}`)
	}))
	defer server.Close()
	t.Setenv("RUCIO_URL", server.URL)
	data := []byte("{\"name\":\"/Primary/Processed/TIER#partial\"}\n")

	all := RucioUnmarshal(dasql.DASQuery{Spec: bson.M{"dataset": "/Primary/Processed/TIER"}}, "file4dataset", data)
	atSite := RucioUnmarshal(dasql.DASQuery{Spec: bson.M{
		"dataset": "/Primary/Processed/TIER",
		"site":    "T1_TEST_Disk",
	}}, "file4dataset_site", data)

	if len(all) != 2 {
		t.Fatalf("file4dataset returned %d files, want 2", len(all))
	}
	if len(atSite) != 1 || atSite[0]["name"] != "/store/at-t1.root" {
		t.Fatalf("file4dataset_site returned %v, want only /store/at-t1.root", atSite)
	}
}

func TestFile4BlockSiteFilteringAfterBranchMove(t *testing.T) {
	data := []byte("{\"name\":\"/store/at-t1.root\",\"states\":{\"T1_TEST_Disk\":\"AVAILABLE\"}}\n" +
		"{\"name\":\"/store/at-t2.root\",\"states\":{\"T2_TEST_Disk\":\"AVAILABLE\"}}\n")
	query := dasql.DASQuery{Spec: bson.M{"site": "T1_TEST_Disk"}}
	records := RucioUnmarshal(query, "file4block_site", data)

	if len(records) != 1 || records[0]["name"] != "/store/at-t1.root" {
		t.Fatalf("file4block_site returned %v, want only /store/at-t1.root", records)
	}
}

func assertRequestContaining(t *testing.T, requests []string, block, fragment string) {
	t.Helper()
	for _, request := range requests {
		if strings.Contains(request, block) && strings.Contains(request, fragment) {
			return
		}
	}
	t.Errorf("no request for block %q contains %q; requests: %v", block, fragment, requests)
}

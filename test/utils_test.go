package main

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/dmwm/das2go/utils"
)

// TestAdjustURL
func TestAdjustURL(t *testing.T) {
	url := "http://bla.com//bla"
	expect := "http://bla.com/bla"
	res := utils.AdjustUrl(url)
	if res != expect {
		log.Printf("%s != %s", url, expect)
		t.Error("Fail TestAdjustUrl")
	}
	url = "https://bla.com//bla"
	expect = "https://bla.com/bla"
	res = utils.AdjustUrl(url)
	if res != expect {
		log.Printf("%s != %s", url, expect)
		t.Error("Fail TestAdjustUrl")
	}
}

// TestInList
func TestInList(t *testing.T) {
	vals := []string{"1", "2", "3"}
	res := utils.InList("1", vals)
	if res == false {
		t.Error("Fail TestInList")
	}
	res = utils.InList("5", vals)
	if res == true {
		t.Error("Fail TestInList")
	}
}

// TestCheckEntries
func TestCheckEntries(t *testing.T) {
	list1 := []string{"1", "2"}
	list2 := []string{"1", "2", "3"}
	res := utils.CheckEntries(list1, list2)
	if res == false {
		t.Error("Fail TestCheckEntries")
	}
	list1 = []string{"1"}
	list2 = []string{"2", "3"}
	res = utils.CheckEntries(list1, list2)
	if res == true {
		t.Error("Fail TestCheckEntries")
	}
}

// TestFindInList
func TestFindInList(t *testing.T) {
	list := []string{"1", "2"}
	res := utils.FindInList("2", list)
	if res != true {
		t.Error("Fail in TestFindInList")
	}
}

// TestEqualLists
func TestEqualLists(t *testing.T) {
	list := []string{"1", "2"}
	res := utils.EqualLists(list, list)
	if res != true {
		t.Error("Fail in TestEqualLists")
	}
}

// TestMapKeys
func TestMapKeys(t *testing.T) {
	dict := make(map[string]interface{})
	keys := []string{"1", "2", "3"}
	for _, k := range keys {
		dict[k] = k
	}
	res := utils.MapKeys(dict)
	for _, v := range res {
		if !utils.InList(v, keys) {
			t.Error("Fail TestMapKeys")
		}
	}
}

// TestSizeFormat
func TestSizeFormat(t *testing.T) {
	v := 1025
	res := utils.SizeFormat(v)
	s := fmt.Sprintf("%d (1.0KB)", v)
	if res != s {
		t.Errorf("Fail TestSizeFormat %v\n", res)
	}
	v = 1024*1024 + 1
	res = utils.SizeFormat(v)
	s = fmt.Sprintf("%d (1.0MB)", v)
	if res != s {
		t.Errorf("Fail TestSizeFormat, %v\n", res)
	}
	v = 1024*1024*1024 + 1
	res = utils.SizeFormat(v)
	s = fmt.Sprintf("%d (1.1GB)", v)
	if res != s {
		t.Errorf("Fail TestSizeFormat, %v\n", res)
	}
	v = 1024*1024*1024*1024 + 1
	res = utils.SizeFormat(v)
	s = fmt.Sprintf("%d (1.1TB)", v)
	if res != s {
		t.Errorf("Fail TestSizeFormat, %v\n", res)
	}
}

// helper funcion to fethc Urls
func fetchUrls(niterations int) {
	rurl := "https://jsonplaceholder.typicode.com/todos"
	out := make(chan utils.ResponseType)
	defer close(out)
	umap := map[string]int{}
	client := utils.HttpClient()
	for i := 0; i < niterations; i++ {
		furl := fmt.Sprintf("%s/%d", rurl, i)
		umap[furl] = 1 // keep track of processed urls below
		go utils.Fetch(client, furl, "", out)
	}

	// collect all results from out channel
	exit := false
	for {
		select {
		case r := <-out:
			log.Println("repsonse", r.String())
			delete(umap, r.Url)
		default:
			if len(umap) == 0 { // no more requests, merge data records
				exit = true
			}
			time.Sleep(1 * time.Millisecond) // wait for response
		}
		if exit || len(umap) == 0 { // no more requests, merge data records
			break
		}
	}
}

// TestFetchUrlWithTimeout should yield WARNING on timeout
func TestFetch(t *testing.T) {
	fetchUrls(5)
}

// TestCerts should test certificate manager
func TestCerts(t *testing.T) {
	uproxy := os.Getenv("X509_USER_PROXY")
	uckey := os.Getenv("X509_USER_KEY")
	ucert := os.Getenv("X509_USER_CERT")
	if uproxy == "" && uckey == "" && ucert == "" {
		// nothing to test
		return
	}
	var tlsManager utils.TLSCertsManager
	certs, err := tlsManager.GetCerts()
	if err != nil {
		t.Errorf("Fail TestCerts %v\n", err)
	}
	notAfter := utils.CertExpire(certs)
	ts := time.Now().Add(time.Duration(600 * time.Second))
	log.Println("certs expire", notAfter)
	if ts.After(notAfter) {
		t.Errorf("Fail TestCerts: current certificate expired in 600 seconds\n")
	}
}

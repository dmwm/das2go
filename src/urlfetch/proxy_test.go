package urlfetch

import "testing"
import "regexp"

// Local helper functions
func parse(output string) bool {
    test, err := regexp.MatchString("</html>", output)
    if  err != nil {
        return false
    }
    return test
}
func test_getdata4urls(urls []string) bool {
    ch := make(chan []byte)
    for _, url := range urls {
        go Fetch(url, ch)
    }
    for i := 0; i<len(urls); i++ {
        res := string(<-ch)
        if  ! parse(res) {
            return false
        }
    }
    return true
}
func test_getdata(url string) bool {
    ch := make(chan []byte)
    go Fetch(url, ch)
    res := string(<-ch)
    return parse(res)
}

// Test function
func TestFetch(t *testing.T) {
    url1 := "http://www.google.com"
    url2 := "http://www.golang.org"
    urls := []string{url1, url2}
    var test bool
    test = test_getdata(url1)
    if  ! test {
        t.Log("test getdata call", url1)
        t.Fail()
    }
    test = test_getdata4urls(urls)
    if  ! test {
        t.Log("test getdata call with multiple urls", urls)
        t.Fail()
    }
}

package chanserver_test

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/esote/queue/internal/chanserver"
)

func Test(t *testing.T) {
	ch, err := chanserver.New()
	if err != nil {
		t.Fatal(err)
	}
	const status = http.StatusNotImplemented
	ch.SetStatusCode(status)
	url := &url.URL{
		Scheme: "http",
		Host:   ch.Addr.String(),
		Path:   "/xyz",
	}
	const body = "body"
	req, err := http.NewRequest(http.MethodDelete, url.String(),
		strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != status {
		t.Fatal(err)
	}
	timer := time.NewTimer(5 * time.Millisecond)
	var r *http.Request
	select {
	case r = <-ch.Reqs:
	case <-timer.C:
		t.Fatal("no request received")
	}
	rbody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(rbody) != body {
		t.Fatalf("want %s, have %s", body, string(rbody))
	}
	if r.URL.Path != url.Path || r.URL.RawQuery != url.RawQuery {
		t.Fatalf("want %s, have %s", r.URL.String(),
			url.String())
	}
	if r.Method != req.Method {
		t.Fatalf("want %s, have %s", req.Method, r.Method)
	}
	if err = ch.Close(); err != nil {
		t.Fatal(err)
	}
}

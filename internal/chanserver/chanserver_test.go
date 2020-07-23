package chanserver_test

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/esote/queue/internal/chanserver"
)

func Test(t *testing.T) {
	ch, err := chanserver.New()
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err = (&http.Client{}).Do(req); err != nil {
		t.Fatal(err)
	}
	var r *http.Request
	select {
	case r = <-ch.Reqs:
		break
	default:
		t.Fatal("nothing received from reqs")
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
	if err = ch.Close(); err != nil {
		t.Fatal(err)
	}
}

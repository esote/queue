package httpq_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/esote/queue/internal/chanserver"
	"github.com/esote/queue/internal/tmpdb"
	"github.com/esote/queue/pkg/httpq"
)

// TODO: race test

func Test(t *testing.T) {
	ch, err := chanserver.New()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	file, err := tmpdb.New()
	if err != nil {
		t.Fatal(err)
	}
	//defer tmpdb.Clean()
	q, err := httpq.New(file, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := &httpq.Request{
		Body:   []byte("body"),
		Method: "PUT",
		URL: &url.URL{
			Scheme: "http",
			Host:   ch.Addr.String(),
			Path:   "/xyz",
		},
	}
	if err = q.Enqueue(req); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	var r *http.Request
	select {
	case r = <-ch.Reqs:
		break
	default:
		t.Fatal("no request received")
	}
	if err = cmpReqs(req, r); err != nil {
		t.Fatal(err)
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
}

func cmpReqs(r1 *httpq.Request, r2 *http.Request) error {
	body, err := ioutil.ReadAll(r2.Body)
	defer r2.Body.Close()
	if err != nil {
		return err
	}
	if !bytes.Equal(r1.Body, body) {
		return fmt.Errorf("bodies not equal: r1 %s, r2 %s",
			string(r1.Body), string(body))
	}
	if r1.Method != r2.Method {
		return fmt.Errorf("methods not equal: r1 %s, r2 %s", r1.Method,
			r2.Method)
	}
	if r1.URL.Path != r2.URL.Path {
		return fmt.Errorf("paths not equal: r1 %s, r2 %s", r1.URL.Path,
			r2.URL.Path)
	}
	return nil
}

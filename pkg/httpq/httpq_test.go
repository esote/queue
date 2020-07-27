package httpq_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/esote/queue"
	"github.com/esote/queue/internal/chanserver"
	"github.com/esote/queue/pkg/httpq"
)

func TestSimple(t *testing.T) {
	ch, err := chanserver.New()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	errors := make(chan error, 1)
	defer close(errors)
	cfg := &httpq.Config{
		Workers:           2,
		DefaultMaxRetries: 0,
		Client: &http.Client{
			Timeout: 50 * time.Millisecond,
		},
		Errors: errors,
	}
	q, err := httpq.New(queue.NewMemoryQueue(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	req := &httpq.Request{
		Body:   []byte("body"),
		Method: http.MethodGet,
		URL: &url.URL{
			Scheme: "http",
			Host:   ch.Addr.String(),
			Path:   "/xyz",
		},
	}
	if err = q.Enqueue(req); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(100 * time.Millisecond)
	var r *http.Request
	select {
	case r = <-ch.Reqs:
		timer.Stop()
	case err := <-errors:
		t.Fatal(err)
	case <-timer.C:
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

func TestRetries(t *testing.T) {
	ch, err := chanserver.New()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	// Chanserver will give response 404 to all requests.
	ch.SetStatusCode(404)
	errors := make(chan error, 1)
	defer close(errors)
	cfg := &httpq.Config{
		Workers:           2,
		DefaultMaxRetries: 3,
		Client: &http.Client{
			Timeout: 50 * time.Millisecond,
		},
		Errors: errors,
	}
	q, err := httpq.New(queue.NewMemoryQueue(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	req := &httpq.Request{
		Body:   []byte("body"),
		Method: http.MethodGet,
		URL: &url.URL{
			Scheme: "http",
			Host:   ch.Addr.String(),
		},
	}
	if err = q.Enqueue(req); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cfg.DefaultMaxRetries; i++ {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ch.Reqs:
		case err := <-errors:
			t.Fatal(err)
		case <-timer.C:
			t.Fatalf("request %d not received", i)
		}
	}
}

func TestPassNilCfg(t *testing.T) {
	ch, err := chanserver.New()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	q, err := httpq.New(queue.NewMemoryQueue(), []byte("password"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	req := &httpq.Request{
		Body:   nil,
		Method: http.MethodGet,
		URL: &url.URL{
			Scheme: "http",
			Host:   ch.Addr.String(),
		},
	}
	if err = q.Enqueue(req); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(5 * time.Second)
	select {
	case <-ch.Reqs:
		timer.Stop()
	case <-timer.C:
		t.Fatal("no request received")
	}
}

func TestRace(t *testing.T) {
	const n = 5
	cfg := &httpq.Config{
		Workers:           n,
		DefaultMaxRetries: 0,
		Client: &http.Client{
			Timeout: 5 * time.Millisecond,
		},
	}
	q, err := httpq.New(queue.NewMemoryQueue(), nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	req := &httpq.Request{
		Body:   nil,
		Method: http.MethodGet,
		URL: &url.URL{
			Scheme: "http",
			Host:   "198.51.100.5", // TEST-NET-2
		},
	}
	var wg sync.WaitGroup
	wg.Add(n)
	quit := make(chan struct{}, n)
	defer close(quit)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-quit:
					return
				default:
				}
				_ = q.Enqueue(req)
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < n; i++ {
		quit <- struct{}{}
	}
	wg.Wait()
}

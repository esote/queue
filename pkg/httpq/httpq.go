// Package httpq is a specialization of queue.AsyncQueue for HTTP requests.
package httpq

import (
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/esote/enc"
	"github.com/esote/queue"
)

// Request represents a future HTTP request sent by the queue.
type Request struct {
	Body   []byte
	Method string
	URL    *url.URL
}

// HTTPQueue sends HTTP requests.
type HTTPQueue interface {
	// Add request to the queue. Safe for concurrent use.
	Enqueue(req *Request) error

	// Close the queue.
	io.Closer
}

// Config is used to configure the behaviour of the HTTP queue.
type Config struct {
	Workers           int
	DefaultMaxRetries int
	Client            *http.Client
	Errors            chan<- error
}

type request struct {
	*Request
	Retries int
}

type httpQueue struct {
	pass       []byte
	q          queue.AsyncQueue
	client     *http.Client
	maxRetries int
	errors     chan<- error
}

// New constructs an async HTTP queue. When a nil config is given, reasonable
// defaults will be used. If pass is non-nil, requests stored in the queue are
// encrypted with the password (see github.com/esote/enc for details).
func New(q queue.Queue, pass []byte, cfg *Config) (HTTPQueue, error) {
	if q == nil {
		return nil, errors.New("httpq: q is nil")
	}
	if cfg == nil {
		cfg = &Config{
			Workers:           5,
			DefaultMaxRetries: 3,
			Client: &http.Client{
				Timeout: time.Second,
			},
		}
	} else {
		if cfg.Client == nil {
			return nil, errors.New("httpq: config has nil Client")
		}
		if cfg.DefaultMaxRetries < 0 {
			cfg.DefaultMaxRetries = 0
		}
	}
	httpq := &httpQueue{
		pass:       pass,
		client:     cfg.Client,
		maxRetries: cfg.DefaultMaxRetries,
		errors:     cfg.Errors,
	}
	async, err := queue.NewAsyncQueue(q, httpq.handler, cfg.Workers)
	if err != nil {
		return nil, err
	}
	httpq.q = async
	return httpq, nil
}

func (q *httpQueue) Enqueue(req *Request) error {
	return q.enqueue(&request{
		Request: req,
		Retries: q.maxRetries,
	})
}

func (q *httpQueue) enqueue(req *request) error {
	data, err := q.encode(req)
	if err != nil {
		return err
	}
	return q.q.Enqueue(data)
}

func (q *httpQueue) Close() error {
	return q.q.Close()
}

// TODO: benchmark encoder/decoder pools vs. only buffer pools vs no pools.
func (q *httpQueue) encode(v interface{}) ([]byte, error) {
	if q.pass == nil {
		var b bytes.Buffer
		if err := gob.NewEncoder(&b).Encode(v); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	}
	data, _, err := enc.Encrypt(q.pass, v)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (q *httpQueue) decode(data []byte, v interface{}) error {
	if q.pass == nil {
		return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
	}
	return enc.Decrypt(data, q.pass, v)
}

func (q *httpQueue) handler(data []byte, err error) {
	if err != nil {
		q.log(err)
		return
	}
	var req request
	if err = q.decode(data, &req); err != nil {
		q.log(err)
		return
	}
	httpReq, err := http.NewRequest(req.Method, req.URL.String(),
		bytes.NewReader(req.Body))
	if err != nil {
		q.log(err)
		return
	}
	resp, err := q.client.Do(httpReq)
	if err != nil {
		q.log(err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && req.Retries > 0 {
		req.Retries--
		if err = q.enqueue(&req); err != nil {
			q.log(err)
		}
	}
	return
}

func (q *httpQueue) log(err error) {
	if q.errors != nil {
		select {
		case q.errors <- err:
		default:
		}
	}
}

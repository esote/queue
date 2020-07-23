// Package httpq is an async queue for HTTP requests.
package httpq

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
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
	Enqueue(req *Request) error
	io.Closer
}

const (
	open int32 = iota
	closed
)

type httpQueue struct {
	closed int32

	pass []byte

	// Pool of encoders and decoders.
	codec *codec

	inner queue.Queue
	q     queue.AsyncQueue

	cond *sync.Cond
	mu   *sync.Mutex

	client     *http.Client
	maxRetries int
	errors     chan<- error
}

type request struct {
	*Request
	retries int
}

// Config is used to configure the behaviour of the HTTP queue.
type Config struct {
	Workers           int
	DefaultMaxRetries int
	Client            *http.Client
	Errors            chan<- error
}

// New constructs a SQLite3-backed async HTTP queue. When a nil config is given,
// reasonable defaults will be used. If pass is non-nil, requests stored in the
// queue are encrypted with the password (see github.com/esote/enc for details).
func New(file string, pass []byte, cfg *Config) (HTTPQueue, error) {
	cfg = normalizeConfig(cfg)
	inner, err := queue.NewSqlite3Queue(file)
	if err != nil {
		return nil, err
	}
	q := &httpQueue{
		closed:     open,
		pass:       pass,
		inner:      inner,
		client:     cfg.Client,
		maxRetries: cfg.DefaultMaxRetries,
		errors:     cfg.Errors,
	}
	if pass == nil {
		q.codec = newCodec()
	}
	q.cond = sync.NewCond(q.mu)
	q.q, err = queue.NewAsyncQueue(inner, q.handler, cfg.Workers)
	if err != nil {
		_ = q.Close()
		return nil, err
	}
	return q, nil
}

func normalizeConfig(cfg *Config) *Config {
	if cfg == nil {
		cfg = &Config{
			Workers:           5,
			DefaultMaxRetries: 3,
		}
	}
	if cfg.DefaultMaxRetries < 0 {
		cfg.DefaultMaxRetries = 0
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{
			Timeout: 3 * time.Second,
		}
	}
	return cfg
}

func (q *httpQueue) Enqueue(req *Request) error {
	err := q.enqueue(&request{
		Request: req,
		retries: q.maxRetries,
	})
	if err == nil {
		q.cond.Signal()
	}
	return err
}

func (q *httpQueue) enqueue(req *request) error {
	data, err := q.encode(req)
	if err != nil {
		return err
	}
	return q.q.Enqueue(data)
}

func (q *httpQueue) encode(v interface{}) ([]byte, error) {
	if q.pass == nil {
		return q.codec.Encode(v)
	}
	data, _, err := enc.Encrypt(q.pass, v)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (q *httpQueue) decode(data []byte, v interface{}) error {
	if q.pass == nil {
		return q.codec.Decode(data, v)
	}
	return enc.Decrypt(data, q.pass, v)
}

func (q *httpQueue) Close() error {
	// Mark queue as closed to avoid race between q.cond and q.q's workers.
	if !atomic.CompareAndSwapInt32(&q.closed, open, closed) {
		return errors.New("httpq: close on closed queue")
	}
	// Unblock all handlers waiting for new data.
	q.cond.Broadcast()
	var err error
	if q.q != nil {
		err = q.q.Close()
	}
	if err2 := q.inner.Close(); err == nil {
		err = err2
	}
	return err
}

func (q *httpQueue) handler(data []byte, err error) {
	if atomic.LoadInt32(&q.closed) == closed {
		return
	}
	if err == queue.ErrEmpty {
		// Wait until data is enqueued.
		// TODO: compare using cond vs channel
		q.mu.Lock()
		q.cond.Wait()
		q.mu.Unlock()
		return
	}
	if err != nil {
		q.log(err)
		return
	}
	var req request
	if err = q.decode(data, &req); err != nil {
		q.log(err)
		return
	}
	// TODO: benchmark setting req.Close = false.
	httpReq, err := http.NewRequest(req.Method, req.URL.String(),
		bytes.NewReader(req.Body))
	if err != nil {
		q.log(err)
		return
	}
	resp, err := q.client.Do(httpReq)
	_ = resp.Body.Close()
	if err != nil {
		q.log(err)
		return
	}
	if resp.StatusCode != http.StatusOK && req.retries > 0 {
		// Try request again.
		req.retries--
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

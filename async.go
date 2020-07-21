package queue

import (
	"errors"
	"io"
	"sync"
)

// AsyncQueue processes queue data asynchronously.
type AsyncQueue interface {
	// Add data to the queue. Safe for concurrent use.
	Enqueue(data []byte) error

	// Close the queue. Can be done at any point after the queue is
	// constructed.
	io.Closer
}

// Handler operates on data from the async queue. Data and err come directly
// from the inner queue's dequeue operation, and so the handler should take care
// to handle non-nil errors, including ErrEmpty.
type Handler func(data []byte, err error)

type aqueue struct {
	q       Queue
	handler Handler
	workers int
	done    chan struct{}
	wg      sync.WaitGroup
}

// NewAsyncQueue creates an async queue that processes inner queue data through
// a handler and worker pool. Closing the async queue does NOT close the inner
// queue.
func NewAsyncQueue(q Queue, handler Handler, workers int) (AsyncQueue, error) {
	if q == nil {
		return nil, errors.New("queue: async: queue is nil")
	}
	if handler == nil {
		return nil, errors.New("queue: async: handler is nil")
	}
	if workers <= 0 {
		return nil, errors.New("queue: async: workers <= 0")
	}
	aq := &aqueue{
		q:       q,
		handler: handler,
		workers: workers,
		done:    make(chan struct{}, workers),
	}
	aq.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go aq.consume()
	}
	return aq, nil
}

func (q *aqueue) Enqueue(data []byte) error {
	return q.q.Enqueue(data)
}

func (q *aqueue) Close() error {
	for i := 0; i < q.workers; i++ {
		q.done <- struct{}{}
	}
	q.wg.Wait()
	return nil
}

func (q *aqueue) consume() {
	defer q.wg.Done()
	for {
		select {
		case <-q.done:
			return
		default:
		}
		q.handle()
	}
}

func (q *aqueue) handle() {
	defer func() {
		// Continue normal execution even if handler panics.
		_ = recover()
	}()
	q.handler(q.q.Dequeue())
}

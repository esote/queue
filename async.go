package queue

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// AsyncQueue processes queue data asynchronously.
type AsyncQueue interface {
	// Add data to the queue. Safe for concurrent use.
	Enqueue(data []byte) error

	// Close the queue. Can be done at any point after the queue is
	// constructed.
	io.Closer
}

// Handler operates on data from the async queue. Err comes from the inner
// queue's dequeue operation when err is not ErrEmpty.
type Handler func(data []byte, err error)

type async struct {
	q       Queue
	handler Handler
	workers int

	state int32
	wait  chan struct{}
	done  chan struct{}
	wg    sync.WaitGroup
}

const (
	open int32 = iota
	closed
)

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
	aq := &async{
		q:       q,
		handler: handler,
		workers: workers,
		state:   open,
		wait:    make(chan struct{}, workers),
		done:    make(chan struct{}, workers),
	}
	aq.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go aq.consume()
	}
	return aq, nil
}

func (q *async) Enqueue(data []byte) error {
	if atomic.LoadInt32(&q.state) == closed {
		return errors.New("async: enqueue on closed queue")
	}
	err := q.q.Enqueue(data)
	if err == nil {
		select {
		case q.wait <- struct{}{}:
		default:
		}
	}
	return err
}

func (q *async) Close() error {
	if atomic.LoadInt32(&q.state) == closed {
		return errors.New("async: close on closed queue")
	}
	for i := 0; i < q.workers; i++ {
		q.done <- struct{}{}
	}
	q.wg.Wait()
	atomic.StoreInt32(&q.state, closed)
	close(q.done)
	close(q.wait)
	return nil
}

func (q *async) consume() {
	defer q.wg.Done()
	wait := false
	for {
		if wait {
			select {
			case <-q.wait:
			case <-q.done:
				return
			}
		} else {
			select {
			case <-q.done:
				return
			default:
			}
		}
		wait = q.handle()
	}
}

func (q *async) handle() bool {
	defer func() {
		// Continue normal execution even if handler panics.
		_ = recover()
	}()
	data, err := q.q.Dequeue()
	if err == ErrEmpty {
		return true
	}
	q.handler(data, err)
	return false
}

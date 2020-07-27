package queue_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/esote/queue"
)

func TestAsyncSimple(t *testing.T) {
	const n = 5
	v := uint32(n)
	handler := func(data []byte, err error) {
		if err != nil {
			t.Fatal(err)
		}
		atomic.AddUint32(&v, ^uint32(0))
	}
	q, err := queue.NewAsyncQueue(queue.NewMemoryQueue(), handler, 3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err = q.Enqueue(nil); err != nil {
			t.Fatal(err)
		}
	}
	// Wait for v to become 0.
	time.Sleep(10 * time.Millisecond)
	if atomic.LoadUint32(&v) != 0 {
		t.Fatal("async: v != 0")
	}
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncNoWaiting(t *testing.T) {
	const n = 5
	handler := func(data []byte, err error) {
		time.Sleep(time.Second)
	}
	q, err := queue.NewAsyncQueue(queue.NewMemoryQueue(), handler, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err = q.Enqueue(nil); err != nil {
			t.Fatal(err)
		}
	}
	// Close queue while data is still being handled asynchronously.
	if err = q.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncPanic(t *testing.T) {
	done := make(chan struct{}, 1)
	defer close(done)
	handler := func(data []byte, err error) {
		defer func() {
			done <- struct{}{}
		}()
		panic("panic")
	}
	q, err := queue.NewAsyncQueue(queue.NewMemoryQueue(), handler, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	// Test that we are able to receive from done twice, meaning the queue
	// continues to work even after the handler panics.
	for i := 0; i < 2; i++ {
		if err = q.Enqueue(nil); err != nil {
			t.Fatal(err)
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-done:
			timer.Stop()
		case <-timer.C:
			t.Fatal("data not dequeued")
		}
	}
}

func TestAsyncRace(t *testing.T) {
	handler := func(data []byte, err error) {}
	const n = 5
	q, err := queue.NewAsyncQueue(queue.NewMemoryQueue(), handler, n)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
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
				_ = q.Enqueue(nil)
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < n; i++ {
		quit <- struct{}{}
	}
	wg.Wait()
}

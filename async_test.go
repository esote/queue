package queue_test

import (
	"testing"
	"time"

	"github.com/esote/queue"
)

func newAsyncs(qs []queue.Queue, handler queue.Handler, workers int) ([]queue.AsyncQueue, error) {
	var aqs []queue.AsyncQueue
	for _, q := range qs {
		aq, err := queue.NewAsyncQueue(q, handler, workers)
		if err != nil {
			return nil, err
		}
		aqs = append(aqs, aq)
	}
	return aqs, nil
}

func TestAsyncRace(t *testing.T) {
	handler := func(data []byte, err error) {}
	qs, err := newQueues()
	if err != nil {
		t.Fatal(err)
	}
	defer closeQueues(qs)
	aqs, err := newAsyncs(qs, handler, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range aqs {
		if err = asyncRace(q); err != nil {
			t.Fatal(err)
		}
	}
}

// Close an async queue while data is being handled.
func asyncRace(q queue.AsyncQueue) error {
	quit := make(chan struct{})
	defer close(quit)
	time.AfterFunc(250*time.Millisecond, func() {
		quit <- struct{}{}
	})
loop:
	for {
		select {
		case <-quit:
			break loop
		default:
		}
		if err := q.Enqueue([]byte{1}); err != nil {
			return err
		}
	}
	time.Sleep(25 * time.Millisecond)
	if err := q.Close(); err != nil {
		return err
	}
	return nil
}

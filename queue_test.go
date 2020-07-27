package queue_test

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/esote/queue"
	"github.com/esote/queue/internal/tmpdb"
)

func TestMain(m *testing.M) {
	tmpdb.AddFile("test.db") // test.db is from example_test.go
	ret := m.Run()
	tmpdb.Clean()
	os.Exit(ret)
}

func newQueues() (map[string]queue.Queue, error) {
	queues := map[string]queue.Queue{
		"memory": queue.NewMemoryQueue(),
	}
	file, err := tmpdb.New()
	if err != nil {
		return nil, err
	}
	queues["sqlite3"], err = queue.NewSqlite3Queue(file)
	if err != nil {
		return nil, err
	}
	return queues, nil
}

func TestQueueSimple(t *testing.T) {
	queues, err := newQueues()
	if err != nil {
		t.Fatal(err)
	}
	for name, q := range queues {
		if err = testQueue(q); err != nil {
			t.Fatalf("queue: %s: %s", name, err)
		}
	}
}

func testQueue(q queue.Queue) error {
	const n byte = 5
	for i := byte(0); i < n; i++ {
		if err := q.Enqueue([]byte{i}); err != nil {
			return err
		}
	}
	for i := byte(0); i < n; i++ {
		data, err := q.Dequeue()
		if err != nil {
			return err
		}
		want := []byte{i}
		if !bytes.Equal(data, want) {
			return fmt.Errorf("want %s, have %s", data, want)
		}
	}
	if _, err := q.Dequeue(); err != queue.ErrEmpty {
		return fmt.Errorf("doesn't return ErrEmpty when empty")
	}
	return q.Close()
}

func TestQueueRace(t *testing.T) {
	queues, err := newQueues()
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range queues {
		testRace(q)
	}
}

func testRace(q queue.Queue) {
	defer q.Close()
	const (
		c = 2
		n = 4
	)
	var wg sync.WaitGroup
	wg.Add(c * n)
	quit := make(chan struct{}, c*n)
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
		go func() {
			defer wg.Done()
			for {
				select {
				case <-quit:
					return
				default:
				}
				_, _ = q.Dequeue()
			}
		}()
	}
	time.Sleep(500 * time.Millisecond)
	for i := 0; i < c*n; i++ {
		quit <- struct{}{}
	}
	wg.Wait()
}

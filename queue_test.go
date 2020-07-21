package queue_test

import (
	"bytes"
	"errors"
	"flag"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/esote/queue"
)

var files = []string{
	"test.db", // File from ExampleAsyncQueue
}

func TestMain(m *testing.M) {
	if !flag.Parsed() {
		flag.Parse()
	}
	ret := m.Run()
	for _, file := range files {
		_ = os.Remove(file)
	}
	os.Exit(ret)
}

func tempFile() (string, error) {
	f, err := ioutil.TempFile("", "queue-*.db")
	if err != nil {
		return "", err
	}
	files = append(files, f.Name())
	return f.Name(), f.Close()
}

func newQueues() ([]queue.Queue, error) {
	qs := []queue.Queue{
		queue.NewMemoryQueue(),
	}
	file, err := tempFile()
	if err != nil {
		return nil, err
	}
	q, err := queue.NewSqlite3Queue(file)
	if err != nil {
		return nil, err
	}
	qs = append(qs, q)
	return qs, nil
}

func closeQueues(closers []queue.Queue) error {
	var err error
	for _, c := range closers {
		if err2 := c.Close(); err == nil {
			err = err2
		}
	}
	return err
}

func TestQueues(t *testing.T) {
	qs, err := newQueues()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeQueues(qs); err != nil {
			t.Fatal(err)
		}
	}()
	const n = ^byte(0)
	for _, q := range qs {
		for i := byte(0); i < n; i++ {
			if err = q.Enqueue([]byte{i}); err != nil {
				t.Fatal(err)
			}
		}
		for i := byte(0); i < n; i++ {
			data, err := q.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, []byte{i}) {
				t.Fatalf("data != %d", i)
			}
		}
		if _, err = q.Dequeue(); err != queue.ErrEmpty {
			t.Fatal("err != ErrEmpty")
		}
	}
}

func TestQueueRace(t *testing.T) {
	qs, err := newQueues()
	if err != nil {
		t.Fatal(err)
	}
	defer closeQueues(qs)
	const (
		n = 100
		c = 3
	)
	for _, q := range qs {
		var wg sync.WaitGroup
		wg.Add(2 * c)
		ex := []byte{3, 2, 1}
		errs := make(chan error, 2*c)
		defer close(errs)
		for i := 0; i < c; i++ {
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					if err := q.Enqueue(ex); err != nil {
						errs <- err
						return
					}
				}
			}()
			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					data, err := q.Dequeue()
					if err != nil {
						if err != queue.ErrEmpty {
							errs <- err
						}
						return
					}
					if !bytes.Equal(data, ex) {
						errs <- errors.New("data != ex")
						return
					}
				}
			}()
		}
		wg.Wait()
		select {
		case err := <-errs:
			t.Fatal(err)
		default:
		}
	}
}

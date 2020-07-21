package queue_test

import (
	"fmt"
	"log"
	"time"

	"github.com/esote/queue"
)

// An example of using the async queue to process data.
func ExampleAsyncQueue() {
	sqlite3, err := queue.NewSqlite3Queue("test.db")
	if err != nil {
		log.Fatal(err)
	}
	defer sqlite3.Close()
	handler := func(data []byte, err error) {
		if err == queue.ErrEmpty {
			// If the queue will usually be empty, here you can
			// sleep or use exp-backoff to avoid busy-waiting. With
			// more knowledge of enqueues its possible to sleep
			// indefinitely and wake when data is added.
			time.Sleep(100 * time.Millisecond)
			return
		}
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Println(string(data))
	}
	q, err := queue.NewAsyncQueue(sqlite3, handler, 2)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	msgs := []string{"hi", "hello", "hey", "hiya"}
	for _, msg := range msgs {
		if err = q.Enqueue([]byte(msg)); err != nil {
			log.Fatal(err)
		}
	}

	// The messages will be dequeued "eventually". Closing the async queue
	// does not block until the queue is empty, it only blocks long enough
	// to ensure all data is accounted for (either stored in the queue or
	// done being used by a handler). However, for the purpose of this
	// example we wait to give the async queue time to process its data.
	time.Sleep(500 * time.Millisecond)
	// Output: hi
	// hello
	// hey
	// hiya
}

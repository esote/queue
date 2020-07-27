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
		if err != nil {
			log.Println(err)
			return
		}
		// Note printing via fmt is racey with multiple workers.
		fmt.Println(string(data))
	}
	// q is an async queue, backed by SQLite3, with one worker. It is
	// generally recommended to have multiple workers.
	q, err := queue.NewAsyncQueue(sqlite3, handler, 1)
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

	// In an async queue, data will be dequeued "eventually." For the
	// purposes of this example we wait for all of it to be processed.
	time.Sleep(10 * time.Millisecond)

	// Output: hi
	// hello
	// hey
	// hiya
}

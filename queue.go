package queue

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"

	// SQLite3 driver.
	_ "github.com/mattn/go-sqlite3"
)

// Queue contains data.
type Queue interface {
	// Add data to the queue. Safe for concurrent use.
	Enqueue(data []byte) error

	// Remove data from the queue. Safe for concurrent use. Returns ErrEmpty
	// if the queue contains no data.
	Dequeue() ([]byte, error)

	// Close the queue.
	io.Closer
}

// ErrEmpty is returned when dequeuing from an empty queue.
var ErrEmpty = errors.New("queue: queue is empty")

type sqlite3Queue struct {
	db *sql.DB
	st map[string]*sql.Stmt
}

// NewSqlite3Queue creates an SQLite3-backed queue with ACID properties.
func NewSqlite3Queue(file string) (Queue, error) {
	u := &url.URL{
		Scheme: "file",
		Opaque: file,
	}
	query := u.Query()
	query.Set("_secure_delete", "on")
	u.RawQuery = query.Encode()

	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return nil, err
	}

	// SQLite3 driver doesn't handle concurrency very well.
	db.SetMaxOpenConns(1)

	const qCreate = `
CREATE TABLE IF NOT EXISTS queue (
	id INTEGER PRIMARY KEY,
	data BLOB NOT NULL
)`

	q := &sqlite3Queue{
		db: db,
		st: make(map[string]*sql.Stmt),
	}

	if _, err := q.db.Exec(qCreate); err != nil {
		_ = q.Close()
		return nil, err
	}
	if err = q.statements(); err != nil {
		_ = q.Close()
		return nil, err
	}
	return q, nil
}

func (q *sqlite3Queue) Enqueue(data []byte) error {
	_, err := q.st["enqueue"].Exec(data)
	return err
}

func (q *sqlite3Queue) Dequeue() ([]byte, error) {
	var data []byte
	err := q.transact(func(tx *sql.Tx) error {
		var id int64
		err := tx.Stmt(q.st["peek"]).QueryRow().Scan(&id, &data)
		if err != nil {
			return err
		}
		_, err = tx.Stmt(q.st["delete"]).Exec(id)
		return err
	})
	switch {
	case err == sql.ErrNoRows:
		return nil, ErrEmpty
	case err != nil:
		return nil, err
	}
	return data, nil
}

func (q *sqlite3Queue) Close() error {
	var err error
	for _, stmt := range q.st {
		if err2 := stmt.Close(); err == nil {
			err = err2
		}
	}
	if err2 := q.db.Close(); err == nil {
		err = err2
	}
	return err
}

func (q *sqlite3Queue) statements() error {
	var err error

	// Enqueue data.
	q.st["enqueue"], err = q.db.Prepare(`
INSERT INTO queue(data)
VALUES (?)`)
	if err != nil {
		return err
	}

	// Peek oldest data.
	q.st["peek"], err = q.db.Prepare(`
SELECT id, data
FROM queue
ORDER BY id
LIMIT 1`)
	if err != nil {
		return err
	}

	// Delete data.
	q.st["delete"], err = q.db.Prepare(`
DELETE FROM queue
WHERE id = ?`)
	return err
}

// Run function within a transaction
func (q *sqlite3Queue) transact(f func(tx *sql.Tx) error) (err error) {
	var tx *sql.Tx
	tx, err = q.db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if r := recover(); r != nil && err == nil {
			err = fmt.Errorf("%v", r)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if err = f(tx); err != nil {
		return
	}
	err = tx.Commit()
	return
}

type memoryQueue struct {
	datas [][]byte
	mu    sync.Mutex
}

// NewMemoryQueue creates an in-memory queue.
func NewMemoryQueue() Queue {
	return &memoryQueue{}
}

func (q *memoryQueue) Enqueue(data []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.datas = append(q.datas, data)
	return nil
}

func (q *memoryQueue) Dequeue() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.datas) == 0 {
		return nil, ErrEmpty
	}
	data := q.datas[0]
	q.datas = q.datas[1:]
	return data, nil
}

func (q *memoryQueue) Close() error {
	return nil
}

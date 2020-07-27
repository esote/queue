package tmpdb

import (
	"io/ioutil"
	"os"
	"sync"
)

var (
	files []string
	mu    sync.Mutex
)

// Clean all existing temporary database files.
func Clean() {
	mu.Lock()
	defer mu.Unlock()
	for _, file := range files {
		_ = os.Remove(file)
	}
	files = nil
}

// AddFile registers a new temporary database file.
func AddFile(f string) {
	mu.Lock()
	defer mu.Unlock()
	files = append(files, f)
}

// New creates a new temporary database file.
func New() (string, error) {
	f, err := ioutil.TempFile("", "queue-*.db")
	if err != nil {
		return "", err
	}
	AddFile(f.Name())
	return f.Name(), f.Close()
}

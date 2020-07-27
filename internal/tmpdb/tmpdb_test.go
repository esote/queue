package tmpdb_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/esote/queue/internal/tmpdb"
)

func Test(t *testing.T) {
	f, err := tmpdb.New()
	if err != nil {
		t.Fatal(err)
	}
	if f != filepath.Join(os.TempDir(), filepath.Base(f)) {
		t.Fatalf("f exists outside of %s", os.TempDir())
	}
	if st, err := os.Stat(f); err != nil || !st.Mode().IsRegular() {
		t.Fatal(err)
	}
	file, err := os.Create("test.db")
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	tmpdb.AddFile("test.db")
	tmpdb.Clean()
	if _, err := os.Stat(f); !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT but got %s", err)
	}
	if _, err := os.Stat("test.db"); !os.IsNotExist(err) {
		t.Fatalf("expected ENOENT but got %s", err)
	}
}

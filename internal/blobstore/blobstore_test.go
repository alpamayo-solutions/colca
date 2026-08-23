package blobstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func open(t *testing.T, max uint64) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), max)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func TestPutThenGetReturnsTheSameBytes(t *testing.T) {
	s := open(t, 1<<20)
	content := []byte("operator manual")
	sha, size, err := s.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}
	if sha != digest(content) {
		t.Fatalf("sha = %s, want %s", sha, digest(content))
	}
	if size != int64(len(content)) {
		t.Fatalf("size = %d, want %d", size, len(content))
	}
	rc, got, err := s.Get(sha)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	if got != int64(len(content)) {
		t.Fatalf("Get size = %d, want %d", got, len(content))
	}
	read, _ := io.ReadAll(rc)
	if !bytes.Equal(read, content) {
		t.Fatalf("content round-trip mismatch")
	}
}

func TestPutIsIdempotent(t *testing.T) {
	s := open(t, 1<<20)
	content := []byte("same file twice")
	first, _, err := s.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := s.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("digests differ: %s vs %s", first, second)
	}
	list, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("List() = %d entries, want 1 — identical content must dedup", len(list))
	}
}

func TestPutRefusesAMismatchedDigest(t *testing.T) {
	s := open(t, 1<<20)
	wrong := digest([]byte("something else"))
	_, _, err := s.Put(bytes.NewReader([]byte("actual content")), wrong)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("err = %v, want ErrDigestMismatch", err)
	}
	if _, ok := s.Has(wrong); ok {
		t.Fatal("a rejected blob must not be stored")
	}
	// The denominator: the same Has() call finds content that WAS stored.
	good, _, err := s.Put(bytes.NewReader([]byte("actual content")), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Has(good); !ok {
		t.Fatal("Has() cannot see a stored blob — the absence assertion above proves nothing")
	}
}

func TestPutRefusesAnOversizeBlob(t *testing.T) {
	s := open(t, 64)
	if _, _, err := s.Put(bytes.NewReader(make([]byte, 32)), ""); err != nil {
		t.Fatalf("under-cap put failed: %v", err)
	}
	_, _, err := s.Put(bytes.NewReader(make([]byte, 128)), "")
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err = %v, want ErrTooLarge", err)
	}
	list, _ := s.List()
	if len(list) != 1 {
		t.Fatalf("List() = %d, want 1 — the oversize blob must not be stored", len(list))
	}
}

func TestPutLeavesNoTempFileBehind(t *testing.T) {
	s := open(t, 64)
	_, _, _ = s.Put(bytes.NewReader(make([]byte, 128)), "")
	var leftovers []string
	_ = filepathWalk(s.root(), func(path string, isDir bool) {
		if !isDir && strings.Contains(path, ".tmp") {
			leftovers = append(leftovers, path)
		}
	})
	if len(leftovers) != 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}
}

func TestGetAndDeleteOnAMissingBlob(t *testing.T) {
	s := open(t, 1<<20)
	if _, _, err := s.Get(digest([]byte("absent"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get err = %v, want ErrNotFound", err)
	}
	if err := s.Delete(digest([]byte("absent"))); err != nil {
		t.Fatalf("Delete of a missing blob must be a no-op, got %v", err)
	}
}

func TestRejectsANonDigestKey(t *testing.T) {
	s := open(t, 1<<20)
	for _, bad := range []string{"", "../../etc/passwd", strings.Repeat("z", 64), "abc"} {
		if _, ok := s.Has(bad); ok {
			t.Fatalf("Has(%q) reported present", bad)
		}
		if _, _, err := s.Get(bad); !errors.Is(err, ErrBadDigest) {
			t.Fatalf("Get(%q) err = %v, want ErrBadDigest", bad, err)
		}
	}
}

func TestSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	sha, _, err := s.Put(bytes.NewReader([]byte("persisted")), "")
	if err != nil {
		t.Fatal(err)
	}
	again, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := again.Has(sha); !ok {
		t.Fatal("a reopened store lost its blob")
	}
}

// filepathWalk keeps the test independent of the store's internal layout.
func filepathWalk(root string, fn func(path string, isDir bool)) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		p := root + "/" + e.Name()
		fn(p, e.IsDir())
		if e.IsDir() {
			_ = filepathWalk(p, fn)
		}
	}
	return nil
}

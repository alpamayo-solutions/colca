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
	"time"
)

func open(t *testing.T, limit uint64) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), limit)
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
	// For comparison, the same Has() call finds content that was stored.
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

	// First check that the walk can see a temp file that exists, so a walk at the
	// wrong root cannot pass by finding nothing.
	canary := s.root() + "/.canary-check.tmp"
	if err := os.WriteFile(canary, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	var seeded []string
	_ = filepathWalk(s.root(), func(path string, isDir bool) {
		if !isDir && strings.Contains(path, ".tmp") {
			seeded = append(seeded, path)
		}
	})
	if len(seeded) != 1 {
		t.Fatalf("walk found %d temp files with the canary present, want 1 — the walk cannot see a temp file that genuinely exists", len(seeded))
	}
	if err := os.Remove(canary); err != nil {
		t.Fatal(err)
	}

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

	// The same Get and Delete against a blob that is present, so the results above
	// are measured against calls that work.
	sha, _, err := s.Put(bytes.NewReader([]byte("present")), "")
	if err != nil {
		t.Fatal(err)
	}
	rc, _, err := s.Get(sha)
	if err != nil {
		t.Fatalf("Get of a present blob failed: %v — the ErrNotFound above proves nothing", err)
	}
	rc.Close()
	if err := s.Delete(sha); err != nil {
		t.Fatalf("Delete of a present blob failed: %v", err)
	}
	if _, ok := s.Has(sha); ok {
		t.Fatal("blob still present after Delete")
	}
}

// TestTouchMovesModifiedForwardAndErrorsOnAnAbsentDigest: a claim on a blob
// must be able to restart the sweeper's grace, and must not pretend to succeed
// for a digest this store never held.
func TestTouchMovesModifiedForwardAndErrorsOnAnAbsentDigest(t *testing.T) {
	s := open(t, 1<<20)
	sha, _, err := s.Put(bytes.NewReader([]byte("touch me")), "")
	if err != nil {
		t.Fatal(err)
	}

	// Push the stored mtime into the past so Touch moving it forward is
	// unambiguous regardless of the filesystem's timestamp resolution.
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(s.path(sha), past, past); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(s.path(sha))
	if err != nil {
		t.Fatal(err)
	}

	if err := s.Touch(sha); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(s.path(sha))
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().After(before.ModTime()) {
		t.Fatalf("Touch did not move Modified forward: before=%v after=%v", before.ModTime(), after.ModTime())
	}

	// An absent digest is an error, not a silent no-op, so the caller can tell a
	// real touch from nothing to touch.
	if err := s.Touch(digest([]byte("never stored"))); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Touch(absent) err = %v, want ErrNotFound", err)
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
	// The same Has and Get against a digest that is present, so the rejections
	// above are measured against a lookup that works.
	good, _, err := s.Put(bytes.NewReader([]byte("valid content")), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Has(good); !ok {
		t.Fatal("Has() cannot see a stored blob — the rejections above prove nothing")
	}
	rc, _, err := s.Get(good)
	if err != nil {
		t.Fatalf("Get() of a stored blob failed: %v", err)
	}
	rc.Close()
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

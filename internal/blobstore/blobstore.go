// Package blobstore is colcad's content-addressed file store: the bytes behind a
// resource, keyed by their SHA-256. Content addressing makes every operation
// safe to repeat: storing held content is a no-op, identical files dedupe, and
// every transfer can be verified. The package knows nothing about doors,
// replication or resources.
package blobstore

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrTooLarge       = errors.New("blob exceeds the configured limit")
	ErrDigestMismatch = errors.New("blob content does not match its digest")
	ErrNotFound       = errors.New("no such blob")
	ErrBadDigest      = errors.New("not a sha-256 hex digest")
)

// Store holds blobs under root as {root}/{sha[0:2]}/{sha}. The two-character
// fan-out keeps any one directory small enough for a filesystem to enumerate
// cheaply, which List does on every sync pass.
type Store struct {
	dir      string
	maxBytes uint64
}

type Info struct {
	SHA256   string
	Size     int64
	Modified time.Time
}

// Open prepares dir, creating it if absent. maxBytes of 0 means no cap.
func Open(dir string, maxBytes uint64) (*Store, error) {
	if dir == "" {
		return nil, errors.New("blobstore: empty directory")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("blobstore: %w", err)
	}
	return &Store{dir: dir, maxBytes: maxBytes}, nil
}

// root exposes the base directory to this package's tests.
func (s *Store) root() string { return s.dir }

// validDigest is the only gate between a caller-supplied string and the
// filesystem: 64 lowercase hex characters can contain no path separator and
// no traversal, so a valid digest is always a safe path element.
func validDigest(sha string) bool {
	if len(sha) != 64 {
		return false
	}
	for i := 0; i < len(sha); i++ {
		c := sha[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (s *Store) path(sha string) string {
	return filepath.Join(s.dir, sha[:2], sha)
}

// tempPrefix and tempSuffix frame the name Put writes to before the rename that
// publishes a blob. Only this package knows what an unfinished blob looks like
// on disk.
const (
	tempPrefix = ".incoming-"
	tempSuffix = ".tmp"
)

// abandonedTempAge is how long an unfinished blob must sit untouched before
// ReclaimAbandonedTemp treats it as debris. It is not the sweeper's grace, which
// an operator may set to 0; that would delete every upload in flight. A write
// bumps the file's mtime, so only a transfer whose process died or that stalled
// for an hour qualifies.
const abandonedTempAge = time.Hour

// Put streams r into the store, hashing as it goes. When expect is non-empty
// the computed digest must equal it. The content lands through a temp file
// and a rename, so a reader never observes a partial blob and a failed write
// leaves nothing behind.
func (s *Store) Put(r io.Reader, expect string) (string, int64, error) {
	if expect != "" && !validDigest(expect) {
		return "", 0, ErrBadDigest
	}
	tmp, err := os.CreateTemp(s.dir, tempPrefix+"*"+tempSuffix)
	if err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	tmpName := tmp.Name()
	// Every failure below must remove the temp file, since nothing else can see it:
	// it has no digest. A process that dies before this defer is what
	// ReclaimAbandonedTemp is for.
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	h := sha256.New()
	reader := io.TeeReader(r, h)
	if s.maxBytes > 0 && s.maxBytes < math.MaxInt64 {
		// One byte past the cap is enough to know it was exceeded.
		reader = io.LimitReader(reader, int64(s.maxBytes)+1)
	}
	size, err := io.Copy(tmp, reader)
	if err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	if s.maxBytes > 0 && size > 0 && uint64(size) > s.maxBytes {
		return "", 0, fmt.Errorf("blobstore: %d bytes > %d: %w", size, s.maxBytes, ErrTooLarge)
	}
	sha := hex.EncodeToString(h.Sum(nil))
	if expect != "" && sha != expect {
		return "", 0, fmt.Errorf("blobstore: computed %s, expected %s: %w", sha, expect, ErrDigestMismatch)
	}
	if err := tmp.Sync(); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	target := s.path(sha)
	shardDir := filepath.Dir(target)
	if err := os.MkdirAll(shardDir, 0o700); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	// The shard directory may be new, so fsync its parent too, or a crash could lose
	// that directory entry.
	if err := syncDir(s.dir); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	// Content addressing makes this rename idempotent: if the target already
	// exists it holds byte-identical content, so overwriting is harmless.
	if err := os.Rename(tmpName, target); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	// fsync of the temp file made the content durable; fsync of the shard directory
	// makes the rename durable. Without it a crash could lose the directory entry,
	// and since confirmations are not persisted, nothing would ever push the blob
	// again.
	if err := syncDir(shardDir); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	return sha, size, nil
}

// syncDir fsyncs a directory so creates, renames and deletes in it survive a
// crash. A file's own fsync does not cover its directory entry.
func syncDir(path string) error {
	d, err := os.Open(path) //nolint:gosec // a directory inside the blob store
	if err != nil {
		return fmt.Errorf("blobstore: %w", err)
	}
	if err := errors.Join(d.Sync(), d.Close()); err != nil {
		return fmt.Errorf("blobstore: %w", err)
	}
	return nil
}

func (s *Store) Get(sha string) (io.ReadCloser, int64, error) {
	if !validDigest(sha) {
		return nil, 0, ErrBadDigest
	}
	f, err := os.Open(s.path(sha))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, fmt.Errorf("blobstore: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("blobstore: %w", err)
	}
	return f, info.Size(), nil
}

func (s *Store) Has(sha string) (int64, bool) {
	if !validDigest(sha) {
		return 0, false
	}
	info, err := os.Stat(s.path(sha))
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

// Touch sets a blob's modification time to now without changing its content. A
// claim on a blob (a resource upsert calling Has) touches it, restarting the
// sweeper's grace for that blob. Callers treat a failed touch as advisory.
func (s *Store) Touch(sha string) error {
	if !validDigest(sha) {
		return ErrBadDigest
	}
	now := time.Now()
	if err := os.Chtimes(s.path(sha), now, now); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("blobstore: %w", err)
	}
	return nil
}

// Delete removes a blob. A blob that is already absent is not an error —
// the sweep and a concurrent delete must be able to race harmlessly.
func (s *Store) Delete(sha string) error {
	if !validDigest(sha) {
		return ErrBadDigest
	}
	if err := os.Remove(s.path(sha)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("blobstore: %w", err)
	}
	return nil
}

// ReclaimAbandonedTemp removes unfinished blobs left by a process that died
// mid-Put and reports how many. Put cleans up after every failure it survives,
// but not after a kill or power cut, and such a file has no digest, so nothing
// else would ever remove it. Only files older than abandonedTempAge are removed;
// now is passed in so callers and tests share one clock.
func (s *Store) ReclaimAbandonedTemp(now time.Time) (removed int, err error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, fmt.Errorf("blobstore: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, tempPrefix) || !strings.HasSuffix(name, tempSuffix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // vanished under the scan — someone else already reclaimed it
		}
		if now.Sub(info.ModTime()) < abandonedTempAge {
			continue // written to recently: a transfer, not debris
		}
		if err := os.Remove(filepath.Join(s.dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return removed, fmt.Errorf("blobstore: %w", err)
		}
		removed++
	}
	return removed, nil
}

// List enumerates every stored blob. Entries that are not valid digests are
// skipped rather than reported: a temp file mid-write is normal, not a fault.
func (s *Store) List() ([]Info, error) {
	shards, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("blobstore: %w", err)
	}
	var out []Info
	for _, shard := range shards {
		if !shard.IsDir() || len(shard.Name()) != 2 {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(s.dir, shard.Name()))
		if err != nil {
			return nil, fmt.Errorf("blobstore: %w", err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !validDigest(name) || !strings.HasPrefix(name, shard.Name()) {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			out = append(out, Info{SHA256: name, Size: info.Size(), Modified: info.ModTime()})
		}
	}
	return out, nil
}

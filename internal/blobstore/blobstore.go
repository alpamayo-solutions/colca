// Package blobstore is colcad's content-addressed file store: the bytes
// behind a resource, keyed by their SHA-256 (resources design §4).
//
// Content addressing is what makes every operation here safe to repeat: a
// write of content already held is a no-op, identical files from different
// nodes dedup, and every transfer is verifiable end to end. Nothing in this
// package knows about doors, replication, or what a resource is.
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

// tempPrefix and tempSuffix bracket the name Put writes through before the
// rename that publishes a blob. They live here because this package is the
// only one that may know what an unfinished blob looks like on disk — the
// sweeper asks for reclamation, it does not go looking for filenames.
const (
	tempPrefix = ".incoming-"
	tempSuffix = ".tmp"
)

// abandonedTempAge is how long an unfinished blob must have sat untouched
// before ReclaimAbandonedTemp treats it as debris rather than a transfer.
//
// It is a constant, not a knob, and deliberately NOT the sweeper's blob grace
// even though both are an hour today. The grace answers a different question —
// "might the record that references this blob still be on its way?" — and an
// operator may legitimately set it to 0 ("no grace, sweep immediately"), which
// would then delete the temp file of every upload in flight.
//
// An hour is safe with room to spare because a write bumps the file's mtime:
// a transfer that is still making any progress at all is never a candidate,
// however slow the link or however large the blob. Only a transfer whose
// process died, or that has been stalled for an hour, qualifies.
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
	// Every failure path below must remove the temp file: no other path here
	// can see it, since it has no digest and List only reports digests. A
	// process that dies before this defer runs is what ReclaimAbandonedTemp
	// exists for.
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
	// The shard directory may have just been created above (a new fan-out
	// bucket): fsync its parent so that dirent survives a crash too, or a
	// later List() on a fresh mount could walk right past it.
	if err := syncDir(s.dir); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	// Content addressing makes this rename idempotent: if the target already
	// exists it holds byte-identical content, so overwriting is harmless.
	if err := os.Rename(tmpName, target); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	// fsync the temp file (above) makes the CONTENT durable; fsync the shard
	// directory makes the RENAME durable. Without this, a crash between the
	// rename and the directory's next background flush can lose the dirent
	// while the data blocks it points at are already on disk — the blob
	// silently vanishes, and because the child's confirmation map is
	// in-memory and does not survive a PARENT restart either, nothing ever
	// re-offers it: eager push degrades to "blob absent at the parent"
	// forever, with no error anywhere to say so.
	if err := syncDir(shardDir); err != nil {
		return "", 0, fmt.Errorf("blobstore: %w", err)
	}
	return sha, size, nil
}

// syncDir fsyncs a directory so that changes to its entries — a create, a
// rename, a delete — survive a crash. A file's own fsync only makes its
// CONTENT durable; the directory entry that makes the file findable again is
// a separate write that needs its own fsync.
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

// Touch bumps a blob's Modified time to now, without touching its content.
// This is what lets a claim on a blob (an in-flight resource upsert reading
// Has) restart the sweeper's grace clock: the sweeper only deletes a blob
// that is both unreferenced AND past grace, so a fresh Modified means a sweep
// racing the claim sees the blob as newly touched and leaves it alone for
// another full grace period (resources design §8, §9.1 TOCTOU fix).
//
// A digest that is not present, or not a valid digest, is not an error here —
// the caller (BlobPort.Has) treats a touch failure as advisory, never as a
// reason to fail the check it is answering.
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

// ReclaimAbandonedTemp removes unfinished blobs left behind by a process that
// died mid-Put, and reports how many it removed.
//
// Put's own deferred cleanup covers every failure THAT process lives through;
// it cannot cover a SIGKILL, an OOM, or a power cut, and the file it leaves is
// invisible to every other path here: it has no digest, so List skips it and
// the sweeper can never see it. Without this the debris is permanent, bounded
// only by crash count times blob size — up to 32 MiB a time on an edge with
// small flash.
//
// A temp file is reclaimed only once it is older than abandonedTempAge; see
// there for why that is a constant and not the sweeper's grace. now is passed
// in rather than read here so a caller (and its tests) has one clock.
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

package door

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Ledger records the commands an Executor started and the answers it gave,
// under a correlation id or an operation key. An entry is written before the
// handler runs and before the answer is published, so a restart answers a
// command again instead of running it again.
type Ledger interface {
	// Entry reports whether key was started, and its answer; answer is nil
	// while the command was started and not answered.
	Entry(key string) (started bool, answer []byte, err error)
	// Start records key as started. An existing entry is kept.
	Start(key string) error
	// Answer records key's answer.
	Answer(key string, answer []byte) error
}

// ledgerLimit is how many entries a MemoryLedger keeps.
const ledgerLimit = 4096

// MemoryLedger is a Ledger that does not survive a restart. It keeps the
// newest 4096 entries.
type MemoryLedger struct {
	mu      sync.Mutex
	entries map[string][]byte
	order   []string
}

// NewMemoryLedger returns an empty MemoryLedger.
func NewMemoryLedger() *MemoryLedger { return &MemoryLedger{entries: map[string][]byte{}} }

// Entry implements Ledger.
func (l *MemoryLedger) Entry(key string) (bool, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	answer, ok := l.entries[key]
	return ok, answer, nil
}

// Start implements Ledger.
func (l *MemoryLedger) Start(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.entries[key]; !ok {
		l.add(key, nil)
	}
	return nil
}

// Answer implements Ledger.
func (l *MemoryLedger) Answer(key string, answer []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.entries[key]; ok {
		l.entries[key] = answer
		return nil
	}
	l.add(key, answer)
	return nil
}

func (l *MemoryLedger) add(key string, answer []byte) {
	l.entries[key] = answer
	l.order = append(l.order, key)
	for len(l.order) > ledgerLimit {
		delete(l.entries, l.order[0])
		l.order = l.order[1:]
	}
}

// FileLedger is a Ledger in one JSON file, for a service with a volume. Every
// change rewrites the file and syncs it before returning. Entries are dropped
// keep after their command started.
type FileLedger struct {
	mu      sync.Mutex
	path    string
	keep    time.Duration
	now     func() time.Time
	entries map[string]fileEntry
}

type fileEntry struct {
	Started int64           `json:"started"` // unix milliseconds
	Answer  json.RawMessage `json:"answer,omitempty"`
}

// OpenFileLedger opens or creates the ledger at path. keep 0 means 7 days.
func OpenFileLedger(path string, keep time.Duration) (*FileLedger, error) {
	if keep <= 0 {
		keep = 7 * 24 * time.Hour
	}
	l := &FileLedger{path: path, keep: keep, now: time.Now, entries: map[string]fileEntry{}}
	raw, err := os.ReadFile(path) //nolint:gosec // the ledger file the service names
	switch {
	case os.IsNotExist(err):
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, fmt.Errorf("ledger %s: %w", path, err)
		}
	case err != nil:
		return nil, fmt.Errorf("ledger %s: %w", path, err)
	default:
		if err := json.Unmarshal(raw, &l.entries); err != nil {
			return nil, fmt.Errorf("ledger %s: %w", path, err)
		}
	}
	return l, nil
}

// Entry implements Ledger.
func (l *FileLedger) Entry(key string) (bool, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[key]
	if !ok {
		return false, nil, nil
	}
	return true, []byte(e.Answer), nil
}

// Start implements Ledger. It also drops entries older than keep.
func (l *FileLedger) Start(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.entries[key]; ok {
		return nil
	}
	now := l.now()
	for k, e := range l.entries {
		if now.Sub(time.UnixMilli(e.Started)) > l.keep {
			delete(l.entries, k)
		}
	}
	l.entries[key] = fileEntry{Started: now.UnixMilli()}
	return l.save()
}

// Answer implements Ledger.
func (l *FileLedger) Answer(key string, answer []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[key]
	if !ok {
		e.Started = l.now().UnixMilli()
	}
	e.Answer = append(json.RawMessage(nil), answer...)
	l.entries[key] = e
	return l.save()
}

// save writes the entries to a temporary file, syncs it and renames it over
// the ledger, so a crash leaves the old file or the new one.
func (l *FileLedger) save() error {
	raw, err := json.Marshal(l.entries)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(l.path), filepath.Base(l.path)+".*")
	if err != nil {
		return fmt.Errorf("ledger %s: %w", l.path, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("ledger %s: %w", l.path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("ledger %s: %w", l.path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("ledger %s: %w", l.path, err)
	}
	if err := os.Rename(tmp.Name(), l.path); err != nil {
		return fmt.Errorf("ledger %s: %w", l.path, err)
	}
	if dir, err := os.Open(filepath.Dir(l.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

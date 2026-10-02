// Package secretstore persists service-sealed ciphertext in a node-local
// Pebble database. Nothing in this package has a stream, cursor, uplink, or
// replication representation.
package secretstore

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/pebblelog"
	"github.com/alpamayo-solutions/colca/secrets"
	"github.com/cockroachdb/pebble/v2"
)

var (
	ErrNotFound         = errors.New("secret not found")
	ErrConflict         = errors.New("secret revision conflict")
	ErrInvalid          = errors.New("invalid secret record")
	ErrInvalidPageToken = errors.New("invalid secret page token")
)

// Record is the complete durable value. Envelope contains ciphertext only.
type Record struct {
	Owner     string           `json:"owner"`
	Name      string           `json:"name"`
	Envelope  secrets.Envelope `json:"envelope"`
	Revision  uint64           `json:"revision"`
	ExpiresAt *time.Time       `json:"expires_at,omitempty"`
	UpdatedAt time.Time        `json:"updated_at"`
}

// Metadata is safe for administration views and intentionally omits ciphertext.
type Metadata struct {
	Owner     string     `json:"owner"`
	Name      string     `json:"name"`
	Revision  uint64     `json:"revision"`
	KeyID     string     `json:"key_id"`
	Algorithm string     `json:"algorithm"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
}

func (r Record) Metadata() Metadata {
	return Metadata{Owner: r.Owner, Name: r.Name, Revision: r.Revision, KeyID: r.Envelope.KeyID,
		Algorithm: r.Envelope.Algorithm, ExpiresAt: r.ExpiresAt, UpdatedAt: r.UpdatedAt}
}

type Store struct {
	db *pebble.DB
	mu sync.Mutex
}

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("secretstore: empty directory")
	}
	db, err := pebble.Open(dir, pebblelog.New("secrets").Options())
	if err != nil {
		return nil, fmt.Errorf("secretstore: open: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Put creates or replaces a secret. expectedRevision=nil is an unconditional
// upsert; 0 means create-only; any other value is compare-and-swap.
func (s *Store) Put(owner, name string, envelope secrets.Envelope, expiresAt *time.Time, expectedRevision *uint64, now time.Time) (Record, error) {
	if err := validateIdentity(owner, name); err != nil {
		return Record{}, err
	}
	if err := envelope.Validate(); err != nil {
		return Record{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if expiresAt != nil {
		utc := expiresAt.UTC()
		expiresAt = &utc
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current, found, err := s.get(owner, name)
	if err != nil {
		return Record{}, err
	}
	if expectedRevision != nil {
		if (*expectedRevision == 0 && found) || (*expectedRevision > 0 && (!found || current.Revision != *expectedRevision)) {
			return Record{}, ErrConflict
		}
	}
	revision := uint64(1)
	if found {
		revision = current.Revision + 1
	}
	record := Record{Owner: owner, Name: name, Envelope: envelope, Revision: revision, ExpiresAt: expiresAt, UpdatedAt: now.UTC()}
	encoded, err := json.Marshal(record)
	if err != nil {
		return Record{}, fmt.Errorf("secretstore: encode: %w", err)
	}
	if err := s.db.Set(secretKey(owner, name), encoded, pebble.Sync); err != nil {
		return Record{}, fmt.Errorf("secretstore: put: %w", err)
	}
	return record, nil
}

func (s *Store) Get(owner, name string) (Record, error) {
	if err := validateIdentity(owner, name); err != nil {
		return Record{}, err
	}
	record, found, err := s.get(owner, name)
	if err != nil {
		return Record{}, err
	}
	if !found {
		return Record{}, ErrNotFound
	}
	return record, nil
}

func (s *Store) get(owner, name string) (Record, bool, error) {
	value, closer, err := s.db.Get(secretKey(owner, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, fmt.Errorf("secretstore: get: %w", err)
	}
	defer closer.Close()
	var record Record
	if err := json.Unmarshal(value, &record); err != nil {
		return Record{}, false, fmt.Errorf("secretstore: corrupt record %s/%s: %w", owner, name, err)
	}
	if record.Owner != owner || record.Name != name || record.Revision == 0 {
		return Record{}, false, fmt.Errorf("secretstore: corrupt record identity %s/%s", owner, name)
	}
	return record, true, nil
}

func (s *Store) List(owner string) ([]Metadata, error) {
	if err := validateOwner(owner); err != nil {
		return nil, err
	}
	prefix := secretPrefix(owner)
	upper := append(append([]byte{}, prefix...), 0xff)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("secretstore: list: %w", err)
	}
	defer iter.Close()
	var out []Metadata
	for iter.First(); iter.Valid(); iter.Next() {
		var record Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return nil, fmt.Errorf("secretstore: corrupt record in %s: %w", owner, err)
		}
		out = append(out, record.Metadata())
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("secretstore: list: %w", err)
	}
	return out, nil
}

// ListPage returns a bounded, stable page of one owner's secret metadata.
// The continuation token is opaque and scoped to that owner.
func (s *Store) ListPage(owner, after string, limit int) ([]Metadata, string, error) {
	if err := validateOwner(owner); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		return nil, "", fmt.Errorf("secretstore: page size must be positive")
	}
	prefix := secretPrefix(owner)
	upper := append(append([]byte{}, prefix...), 0xff)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return nil, "", fmt.Errorf("secretstore: list page: %w", err)
	}
	defer iter.Close()

	valid := iter.First()
	if after != "" {
		raw, decodeErr := base64.RawURLEncoding.DecodeString(after)
		if decodeErr != nil || bytes.Compare(raw, prefix) < 0 || bytes.Compare(raw, upper) >= 0 {
			return nil, "", ErrInvalidPageToken
		}
		valid = iter.SeekGE(raw)
		if valid && bytes.Equal(iter.Key(), raw) {
			valid = iter.Next()
		}
	}

	// A large limit must not preallocate a large slice; append grows it as needed.
	const maxPrealloc = 1000
	capacity := limit
	if capacity > maxPrealloc {
		capacity = maxPrealloc
	}
	out := make([]Metadata, 0, capacity)
	var lastKey []byte
	for ; valid && len(out) < limit; valid = iter.Next() {
		lastKey = append(lastKey[:0], iter.Key()...)
		var record Record
		if err := json.Unmarshal(iter.Value(), &record); err != nil {
			return nil, "", fmt.Errorf("secretstore: corrupt record in %s: %w", owner, err)
		}
		out = append(out, record.Metadata())
	}
	if err := iter.Error(); err != nil {
		return nil, "", fmt.Errorf("secretstore: list page: %w", err)
	}
	if valid && len(lastKey) > 0 {
		return out, base64.RawURLEncoding.EncodeToString(lastKey), nil
	}
	return out, "", nil
}

func (s *Store) Delete(owner, name string, expectedRevision *uint64) error {
	if err := validateIdentity(owner, name); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found, err := s.get(owner, name)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if expectedRevision != nil && current.Revision != *expectedRevision {
		return ErrConflict
	}
	if err := s.db.Delete(secretKey(owner, name), pebble.Sync); err != nil {
		return fmt.Errorf("secretstore: delete: %w", err)
	}
	return nil
}

func secretKey(owner, name string) []byte { return []byte("s\x00" + owner + "\x00" + name) }
func secretPrefix(owner string) []byte    { return []byte("s\x00" + owner + "\x00") }

func validateIdentity(owner, name string) error {
	if err := validateOwner(owner); err != nil {
		return err
	}
	if name == "" || len(name) > 256 || strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") {
		return fmt.Errorf("%w: secret name must be 1-256 characters", ErrInvalid)
	}
	for _, segment := range strings.Split(name, "/") {
		if err := validateSegment(segment); err != nil {
			return fmt.Errorf("%w: invalid secret name: %w", ErrInvalid, err)
		}
	}
	return nil
}

func validateOwner(owner string) error {
	if len(owner) > 128 {
		return fmt.Errorf("%w: owner exceeds 128 characters", ErrInvalid)
	}
	if err := validateSegment(owner); err != nil {
		return fmt.Errorf("%w: invalid owner: %w", ErrInvalid, err)
	}
	return nil
}

func validateSegment(segment string) error {
	if segment == "" || segment == "." || segment == ".." {
		return fmt.Errorf("segment is empty or reserved")
	}
	for _, r := range segment {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("segment contains %q", r)
	}
	return nil
}

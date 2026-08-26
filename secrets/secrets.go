// Package secrets owns the service-side cryptography for Colca's node-local
// ciphertext store. Colca stores Envelope values but never receives plaintext
// or a service's private key.
package secrets

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/nacl/box"
)

const (
	// Algorithm is libsodium's sealed-box construction for X25519 recipients.
	Algorithm      = "nacl-box-seal-x25519-xsalsa20-poly1305"
	keyringVersion = 1
	maxCiphertext  = 64 << 10
)

// Envelope is the only secret-shaped value Colca accepts. It is safe to
// persist and back up: only the service key named by KeyID can open it.
type Envelope struct {
	Version    int    `json:"version"`
	Algorithm  string `json:"algorithm"`
	KeyID      string `json:"key_id"`
	Ciphertext string `json:"ciphertext"`
}

// Validate rejects malformed or disguised cleartext before it reaches disk.
func (e Envelope) Validate() error {
	if e.Version != 1 || e.Algorithm != Algorithm || e.KeyID == "" || e.Ciphertext == "" {
		return fmt.Errorf("unsupported or incomplete sealed secret envelope")
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(e.Ciphertext)
	if err != nil {
		return fmt.Errorf("sealed secret ciphertext is not valid base64")
	}
	if len(decoded) < 32+box.Overhead {
		return fmt.Errorf("sealed secret ciphertext is truncated")
	}
	if len(decoded) > maxCiphertext {
		return fmt.Errorf("sealed secret ciphertext exceeds %d bytes", maxCiphertext)
	}
	return nil
}

type storedKey struct {
	Public    string     `json:"public"`
	Private   string     `json:"private"`
	CreatedAt time.Time  `json:"created_at"`
	RetiredAt *time.Time `json:"retired_at,omitempty"`
}

type diskKeyring struct {
	Version     int                  `json:"version"`
	ActiveKeyID string               `json:"active_key_id"`
	RotatedAt   time.Time            `json:"rotated_at"`
	Keys        map[string]storedKey `json:"keys"`
}

// PublicKey is safe service metadata. KeyID is the SHA-256 fingerprint of
// PublicKey, so an administrator can pin what they approved.
type PublicKey struct {
	KeyID     string `json:"key_id"`
	PublicKey string `json:"public_key"`
	Active    bool   `json:"active"`
}

// PublicMetadata is advertised by a service for secret provisioning.
type PublicMetadata struct {
	Algorithm   string      `json:"algorithm"`
	ActiveKeyID string      `json:"active_key_id"`
	Keys        []PublicKey `json:"keys"`
	RotatedAt   time.Time   `json:"rotated_at"`
}

// Keyring is a service-owned X25519 keyring. Its file never belongs in
// Colca's data or deployment bundle.
type Keyring struct {
	mu   sync.RWMutex
	path string
	data diskKeyring
}

// OpenKeyring loads a keyring or mints its first key. Directory and file modes
// are forced to owner-only on every open.
func OpenKeyring(directory string) (*Keyring, error) {
	if directory == "" {
		return nil, fmt.Errorf("secret key directory is required")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create secret key directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("secure secret key directory: %w", err)
	}
	path := filepath.Join(directory, "keyring.json")
	encoded, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		ring := &Keyring{path: path, data: diskKeyring{Version: keyringVersion, Keys: map[string]storedKey{}}}
		if _, err := ring.rotateLocked(time.Now().UTC()); err != nil {
			return nil, err
		}
		return ring, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read secret keyring: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat secret keyring: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("secret keyring must not be group/world accessible")
	}
	var data diskKeyring
	if err := json.Unmarshal(encoded, &data); err != nil {
		return nil, fmt.Errorf("decode secret keyring: %w", err)
	}
	if data.Version != keyringVersion || data.ActiveKeyID == "" || len(data.Keys) == 0 {
		return nil, fmt.Errorf("secret keyring is invalid")
	}
	if _, ok := data.Keys[data.ActiveKeyID]; !ok {
		return nil, fmt.Errorf("secret keyring active key is missing")
	}
	return &Keyring{path: path, data: data}, nil
}

func (ring *Keyring) ActiveKeyID() string {
	ring.mu.RLock()
	defer ring.mu.RUnlock()
	return ring.data.ActiveKeyID
}

// Rotate retires the active key and mints a replacement. Retired keys remain
// available until Prune removes them, so existing envelopes keep working.
func (ring *Keyring) Rotate(now time.Time) (string, error) {
	ring.mu.Lock()
	defer ring.mu.Unlock()
	return ring.rotateLocked(now.UTC())
}

func (ring *Keyring) rotateLocked(now time.Time) (string, error) {
	public, private, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("generate X25519 key: %w", err)
	}
	id := keyID(*public)
	next := cloneKeyring(ring.data)
	if current, ok := next.Keys[next.ActiveKeyID]; ok {
		current.RetiredAt = &now
		next.Keys[next.ActiveKeyID] = current
	}
	next.Version = keyringVersion
	next.ActiveKeyID = id
	next.RotatedAt = now
	if next.Keys == nil {
		next.Keys = map[string]storedKey{}
	}
	next.Keys[id] = storedKey{
		Public: base64.StdEncoding.EncodeToString(public[:]), Private: base64.StdEncoding.EncodeToString(private[:]), CreatedAt: now,
	}
	if err := ring.persist(next); err != nil {
		return "", err
	}
	ring.data = next
	return id, nil
}

// Open decrypts one envelope inside the service process.
func (ring *Keyring) Open(envelope Envelope) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	ring.mu.RLock()
	key, ok := ring.data.Keys[envelope.KeyID]
	ring.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("sealed secret names unknown key_id")
	}
	public, err := decodeKey(key.Public)
	if err != nil {
		return nil, err
	}
	private, err := decodeKey(key.Private)
	if err != nil {
		return nil, err
	}
	return openSealed(envelope.Ciphertext, public, private)
}

// SealForActive encrypts plaintext for this service's active key.
func (ring *Keyring) SealForActive(plaintext []byte) (Envelope, error) {
	ring.mu.RLock()
	id := ring.data.ActiveKeyID
	key := ring.data.Keys[id]
	ring.mu.RUnlock()
	public, err := decodeKey(key.Public)
	if err != nil {
		return Envelope{}, err
	}
	ciphertext, err := seal(plaintext, public)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Version: 1, Algorithm: Algorithm, KeyID: id, Ciphertext: ciphertext}, nil
}

// SealForPublic encrypts plaintext from an admin/UI process using advertised
// service metadata. The private key is not needed or exposed.
func SealForPublic(plaintext []byte, key PublicKey) (Envelope, error) {
	public, err := decodeKey(key.PublicKey)
	if err != nil {
		return Envelope{}, err
	}
	if key.KeyID == "" || key.KeyID != keyID(public) {
		return Envelope{}, fmt.Errorf("public key fingerprint does not match key_id")
	}
	ciphertext, err := seal(plaintext, public)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{Version: 1, Algorithm: Algorithm, KeyID: key.KeyID, Ciphertext: ciphertext}, nil
}

// Metadata returns the active key plus unexpired grace keys.
func (ring *Keyring) Metadata(now time.Time, grace time.Duration) PublicMetadata {
	ring.mu.RLock()
	defer ring.mu.RUnlock()
	keys := make([]PublicKey, 0, len(ring.data.Keys))
	for id, key := range ring.data.Keys {
		if id != ring.data.ActiveKeyID && (key.RetiredAt == nil || now.Sub(*key.RetiredAt) > grace) {
			continue
		}
		keys = append(keys, PublicKey{KeyID: id, PublicKey: key.Public, Active: id == ring.data.ActiveKeyID})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].KeyID < keys[j].KeyID })
	return PublicMetadata{Algorithm: Algorithm, ActiveKeyID: ring.data.ActiveKeyID, Keys: keys, RotatedAt: ring.data.RotatedAt}
}

// Prune removes grace-expired retired keys unless a caller says they are used.
func (ring *Keyring) Prune(now time.Time, grace time.Duration, inUse map[string]struct{}) error {
	ring.mu.Lock()
	defer ring.mu.Unlock()
	next := cloneKeyring(ring.data)
	changed := false
	for id, key := range next.Keys {
		if id == next.ActiveKeyID || key.RetiredAt == nil || now.Sub(*key.RetiredAt) <= grace {
			continue
		}
		if _, used := inUse[id]; used {
			return fmt.Errorf("cannot remove secret key %s: stored secret still references it", id)
		}
		delete(next.Keys, id)
		changed = true
	}
	if changed {
		if err := ring.persist(next); err != nil {
			return err
		}
		ring.data = next
	}
	return nil
}

func cloneKeyring(data diskKeyring) diskKeyring {
	keys := make(map[string]storedKey, len(data.Keys))
	for id, key := range data.Keys {
		keys[id] = key
	}
	data.Keys = keys
	return data
}

func (ring *Keyring) persist(data diskKeyring) error {
	encoded, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	directory := filepath.Dir(ring.path)
	temp, err := os.CreateTemp(directory, ".keyring-*")
	if err != nil {
		return err
	}
	name := temp.Name()
	defer os.Remove(name)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return err
	}
	if _, err := temp.Write(encoded); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(name, ring.path)
}

func seal(plaintext []byte, recipientPublic [32]byte) (string, error) {
	ephemeralPublic, ephemeralPrivate, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	nonce, err := sealedNonce(ephemeralPublic, &recipientPublic)
	if err != nil {
		return "", err
	}
	sealed := make([]byte, 32)
	copy(sealed, ephemeralPublic[:])
	sealed = box.Seal(sealed, plaintext, &nonce, &recipientPublic, ephemeralPrivate)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

func openSealed(ciphertext string, recipientPublic, recipientPrivate [32]byte) ([]byte, error) {
	sealed, err := base64.StdEncoding.Strict().DecodeString(ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode sealed ciphertext: invalid base64")
	}
	if len(sealed) < 32+box.Overhead {
		return nil, fmt.Errorf("sealed ciphertext is truncated")
	}
	var ephemeralPublic [32]byte
	copy(ephemeralPublic[:], sealed[:32])
	nonce, err := sealedNonce(&ephemeralPublic, &recipientPublic)
	if err != nil {
		return nil, err
	}
	plaintext, ok := box.Open(nil, sealed[32:], &nonce, &ephemeralPublic, &recipientPrivate)
	if !ok {
		return nil, fmt.Errorf("sealed ciphertext authentication failed")
	}
	return plaintext, nil
}

func sealedNonce(ephemeralPublic, recipientPublic *[32]byte) ([24]byte, error) {
	hash, err := blake2b.New(24, nil)
	if err != nil {
		return [24]byte{}, err
	}
	_, _ = hash.Write(ephemeralPublic[:])
	_, _ = hash.Write(recipientPublic[:])
	var nonce [24]byte
	copy(nonce[:], hash.Sum(nil))
	return nonce, nil
}

func keyID(public [32]byte) string {
	digest := sha256.Sum256(public[:])
	return "v1:" + hex.EncodeToString(digest[:])
}

func decodeKey(value string) ([32]byte, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return [32]byte{}, fmt.Errorf("X25519 public key material is invalid")
	}
	var key [32]byte
	copy(key[:], decoded)
	return key, nil
}

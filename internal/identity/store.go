package identity

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
)

// DefaultTPMDevice is the TPM opened when identity.tpm_device is not set: the
// kernel resource manager, which lets several processes share the chip.
const DefaultTPMDevice = "/dev/tpmrm0"

// Options select and locate the node key (config block `identity`).
type Options struct {
	// KeyStore is StoreAuto (also when empty), StoreTPM or StoreFile.
	KeyStore string
	// KeyFile holds the file key, or the TPM key blob for a TPM key.
	KeyFile string
	// TPMDevice is the TPM character device or simulator socket;
	// DefaultTPMDevice when empty.
	TPMDevice string
	// Log receives the reason when auto falls back to a file key; slog.Default
	// when nil.
	Log *slog.Logger
}

func (o Options) device() string {
	if o.TPMDevice == "" {
		return DefaultTPMDevice
	}
	return o.TPMDevice
}

func (o Options) log() *slog.Logger {
	if o.Log == nil {
		return slog.Default()
	}
	return o.Log
}

// ValidKeyStore reports whether s is a key_store value config accepts.
func ValidKeyStore(s string) bool {
	return s == "" || s == StoreAuto || s == StoreTPM || s == StoreFile
}

// Open returns the node key, minting it on first start; the bool reports
// whether it minted. What already exists at KeyFile always wins over KeyStore,
// because replacing it would change the node's identity:
//
//   - a TPM key blob is loaded from the TPM; if the TPM cannot be opened the
//     node does not start (whatever KeyStore says);
//   - a file key is loaded as a file key. With KeyStore tpm the TPM must still
//     open, so a missing device mapping is loud; the file key stays in use
//     until a key change moves the node into the TPM;
//   - anything unparseable is an error, never a reason to mint.
//
// Only when KeyFile does not exist is KeyStore consulted: tpm creates the key
// in the TPM or fails, file mints an ed25519 file key, auto tries the TPM and
// falls back to a file key.
func Open(o Options) (*Identity, bool, error) {
	if !ValidKeyStore(o.KeyStore) {
		return nil, false, fmt.Errorf("identity: key_store %q, want auto, tpm or file", o.KeyStore)
	}
	raw, err := os.ReadFile(o.KeyFile)
	switch {
	case err == nil:
		id, err := openExisting(o, raw)
		if err != nil {
			return nil, false, unusable(o.KeyFile, err)
		}
		if o.KeyStore == StoreTPM && id.Store == StoreFile {
			dev, err := tpmkey.Open(o.device())
			if err != nil {
				return nil, false, fmt.Errorf("identity: key_store tpm, but %w", err)
			}
			_ = dev.Close()
			o.log().Warn("key_store is tpm but the node key is a file key; it stays in use until the parent accepts a key change into the TPM",
				"key_file", o.KeyFile, "fingerprint", id.Fingerprint())
		}
		return id, false, nil
	case !errors.Is(err, fs.ErrNotExist):
		return nil, false, unusable(o.KeyFile, err)
	}

	if err := os.MkdirAll(filepath.Dir(o.KeyFile), 0o700); err != nil {
		return nil, false, fmt.Errorf("identity %s: %w", o.KeyFile, err)
	}
	switch o.KeyStore {
	case StoreFile:
		id, err := Generate(o.KeyFile)
		if err != nil {
			return nil, false, fmt.Errorf("identity %s: %w", o.KeyFile, err)
		}
		return id, true, nil
	case StoreTPM:
		id, err := mintTPM(o)
		if err != nil {
			return nil, false, fmt.Errorf("identity: key_store tpm: %w", err)
		}
		return id, true, nil
	}
	id, err := mintTPM(o)
	if err == nil {
		return id, true, nil
	}
	o.log().Info("no usable TPM, the node key is a file key", "tpm_device", o.device(), "reason", err.Error())
	id, err = Generate(o.KeyFile)
	if err != nil {
		return nil, false, fmt.Errorf("identity %s: %w", o.KeyFile, err)
	}
	return id, true, nil
}

// OpenExisting loads the node key at KeyFile without ever minting one: a
// missing file is an fs.ErrNotExist error. A TPM key blob is loaded through
// TPMDevice.
func OpenExisting(o Options) (*Identity, error) {
	raw, err := os.ReadFile(o.KeyFile)
	if err != nil {
		return nil, err
	}
	return openExisting(o, raw)
}

func openExisting(o Options, raw []byte) (*Identity, error) {
	if !tpmkey.IsBlob(raw) {
		return parseFileKey(o.KeyFile, raw)
	}
	dev, err := tpmkey.Open(o.device())
	if err != nil {
		return nil, fmt.Errorf("the key is held by a TPM: %w", err)
	}
	k, err := dev.LoadKey(raw)
	if err != nil {
		_ = dev.Close()
		return nil, err
	}
	return tpmIdentity(dev, k)
}

func mintTPM(o Options) (*Identity, error) {
	dev, err := tpmkey.Open(o.device())
	if err != nil {
		return nil, err
	}
	k, err := dev.CreateKey()
	if err != nil {
		_ = dev.Close()
		return nil, err
	}
	if err := os.WriteFile(o.KeyFile, k.Blob(), 0o600); err != nil {
		_ = k.Close()
		_ = dev.Close()
		return nil, fmt.Errorf("write key blob %s: %w", o.KeyFile, err)
	}
	return tpmIdentity(dev, k)
}

func tpmIdentity(dev *tpmkey.Device, k *tpmkey.Key) (*Identity, error) {
	c := tpmCloser{dev: dev, key: k}
	id, err := New(k, StoreTPM, c)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	return id, nil
}

type tpmCloser struct {
	dev *tpmkey.Device
	key *tpmkey.Key
}

func (c tpmCloser) Close() error {
	kerr := c.key.Close()
	return errors.Join(kerr, c.dev.Close())
}

// TPMKey returns the TPM key behind a TPM identity, for attestation.
func (i *Identity) TPMKey() (*tpmkey.Key, bool) {
	k, ok := i.Signer.(*tpmkey.Key)
	return k, ok
}

// PendingKeyFile is where a key change keeps the new key until the parent
// accepts it: next to KeyFile, so a restart in the middle resumes the change
// with the same key instead of locking the node out.
func (o Options) PendingKeyFile() string { return o.KeyFile + ".next" }

// CertFile is where the certificate the parent issued is kept, next to the key.
func (o Options) CertFile() string { return o.KeyFile + ".crt" }

// MintPending creates the new key of a key change at PendingKeyFile, in the
// TPM for StoreTPM and as an ed25519 file key otherwise.
func MintPending(o Options, store string) (*Identity, error) {
	path := o.PendingKeyFile()
	if store == StoreTPM {
		p := o
		p.KeyFile = path
		return mintTPM(p)
	}
	return Generate(path)
}

// OpenPending loads the new key of a key change in progress; an
// fs.ErrNotExist error when there is none.
func OpenPending(o Options) (*Identity, error) {
	p := o
	p.KeyFile = o.PendingKeyFile()
	return OpenExisting(p)
}

// PromotePending makes the pending key the node key: it replaces KeyFile in
// one rename, so the old key is gone (it is no longer valid anywhere) and a
// crash leaves either the old key with the pending one, or the new key.
func PromotePending(o Options) error {
	if err := os.Rename(o.PendingKeyFile(), o.KeyFile); err != nil {
		return fmt.Errorf("identity: promote the new key: %w", err)
	}
	return nil
}

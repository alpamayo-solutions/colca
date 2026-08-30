package secretstore

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/secrets"
)

func testEnvelope(t *testing.T, plaintext string) secrets.Envelope {
	t.Helper()
	ring, err := secrets.OpenKeyring(filepath.Join(t.TempDir(), "keys"))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := ring.SealForActive([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestCiphertextPersistsAcrossRestart(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "secrets")
	first, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	envelope := testEnvelope(t, "provider-api-key")
	expires := time.Now().Add(24 * time.Hour).UTC()
	record, err := first.Put("assistant", "model-providers/alpha", envelope, &expires, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	got, err := second.Get("assistant", "model-providers/alpha")
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 1 || got.Envelope != record.Envelope || got.ExpiresAt == nil || !got.ExpiresAt.Equal(expires) {
		t.Fatalf("persisted record = %+v, want %+v", got, record)
	}
}

func TestNamespacesAndListingsAreOwnerScoped(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	envelope := testEnvelope(t, "same ciphertext shape")
	for _, owner := range []string{"assistant", "notifications"} {
		if _, err := store.Put(owner, "primary", envelope, nil, nil, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.List("assistant")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Owner != "assistant" {
		t.Fatalf("assistant list = %+v", items)
	}
}

func TestListPageIsBoundedAndTokensAreOwnerScoped(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	envelope := testEnvelope(t, "ciphertext")
	for _, name := range []string{"a", "b", "c"} {
		if _, err := store.Put("assistant", name, envelope, nil, nil, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	first, next, err := store.ListPage("assistant", "", 2)
	if err != nil || len(first) != 2 || next == "" {
		t.Fatalf("first page = %+v next=%q err=%v", first, next, err)
	}
	second, final, err := store.ListPage("assistant", next, 2)
	if err != nil || len(second) != 1 || second[0].Name != "c" || final != "" {
		t.Fatalf("second page = %+v next=%q err=%v", second, final, err)
	}
	if _, _, err := store.ListPage("notifications", next, 2); !errors.Is(err, ErrInvalidPageToken) {
		t.Fatalf("cross-owner token error = %v, want ErrInvalidPageToken", err)
	}
}

func TestCompareAndSwapProtectsConcurrentUpdatesAndDeletes(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	envelope := testEnvelope(t, "one")
	createOnly := uint64(0)
	created, err := store.Put("assistant", "primary", envelope, nil, &createOnly, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put("assistant", "primary", envelope, nil, &createOnly, time.Now()); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate create error = %v, want conflict", err)
	}
	stale := created.Revision + 1
	if _, err := store.Put("assistant", "primary", envelope, nil, &stale, time.Now()); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error = %v, want conflict", err)
	}
	if err := store.Delete("assistant", "primary", &stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete error = %v, want conflict", err)
	}
	if err := store.Delete("assistant", "primary", &created.Revision); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRejectsPlaintextShapedAndTraversalRecords(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	badEnvelope := secrets.Envelope{Version: 1, Algorithm: secrets.Algorithm, KeyID: "key", Ciphertext: "plaintext"}
	for _, tc := range []struct {
		owner string
		name  string
	}{
		{"assistant", "primary"},
		{"assistant", "../other"},
		{"../assistant", "primary"},
		{"assistant", ""},
	} {
		if _, err := store.Put(tc.owner, tc.name, badEnvelope, nil, nil, time.Now()); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Put(%q, %q) error = %v, want invalid", tc.owner, tc.name, err)
		}
	}
}

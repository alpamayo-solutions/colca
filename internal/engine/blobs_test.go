package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
)

type fakeFetcher struct {
	content map[string][]byte
	calls   int
}

func (f *fakeFetcher) BlobGet(sha string, hops int) (io.ReadCloser, int64, error) {
	f.calls++
	body, ok := f.content[sha]
	if !ok {
		return nil, 0, errors.New("no ancestor holds it")
	}
	return io.NopCloser(bytes.NewReader(body)), int64(len(body)), nil
}

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func newPort(t *testing.T) (*BlobPort, *blobstore.Store) {
	t.Helper()
	store, err := blobstore.Open(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return NewBlobPort(store), store
}

func TestHasReportsWhatTheStoreHolds(t *testing.T) {
	port, store := newPort(t)
	content := []byte("press 3 manual")
	sha, _, err := store.Put(bytes.NewReader(content), "")
	if err != nil {
		t.Fatal(err)
	}
	if !port.Has(sha) {
		t.Fatal("Has must see a blob the store holds")
	}
	if port.Has(digestOf([]byte("never stored"))) {
		t.Fatal("Has must not report a blob the store lacks")
	}
}

func TestPullFetchesAndStores(t *testing.T) {
	port, _ := newPort(t)
	content := []byte("staged at the root")
	sha := digestOf(content)
	port.SetFetcher(&fakeFetcher{content: map[string][]byte{sha: content}})

	if port.Has(sha) {
		t.Fatal("precondition: the store must not hold it yet")
	}
	if err := port.Pull(sha); err != nil {
		t.Fatal(err)
	}
	if !port.Has(sha) {
		t.Fatal("the blob must be held after a successful pull")
	}
}

func TestPullVerifiesTheDigest(t *testing.T) {
	port, _ := newPort(t)
	claimed := digestOf([]byte("claimed"))
	port.SetFetcher(&fakeFetcher{content: map[string][]byte{claimed: []byte("something else")}})

	if err := port.Pull(claimed); err == nil {
		t.Fatal("a fetch whose content does not match the digest must fail")
	}
	if port.Has(claimed) {
		t.Fatal("mismatched content must not be stored")
	}
	// Denominator: an honest fetch through the same path IS stored.
	good := []byte("honest")
	sha := digestOf(good)
	port.SetFetcher(&fakeFetcher{content: map[string][]byte{sha: good}})
	if err := port.Pull(sha); err != nil {
		t.Fatal(err)
	}
	if !port.Has(sha) {
		t.Fatal("Has cannot see a stored blob — the assertion above proves nothing")
	}
}

func TestPullWithoutAFetcherFails(t *testing.T) {
	port, _ := newPort(t)
	err := port.Pull(digestOf([]byte("anything")))
	if err == nil {
		t.Fatal("a node with no parent must report an error, not succeed silently")
	}
}

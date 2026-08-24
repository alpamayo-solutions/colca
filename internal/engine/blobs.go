package engine

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
)

// BlobFetcher is the parent link as this adapter needs it (resources design
// §7.1). It is an interface rather than *repl.Client because repl imports
// engine — taking the concrete type would be an import cycle. The real client
// is injected after it exists, exactly as repl.Server's upstream is.
type BlobFetcher interface {
	BlobGet(sha string, hops int) (io.ReadCloser, int64, error)
}

// defaultPullHops bounds the rootward walk a pull may trigger. The tree has no
// cycles; this stops a misconfigured parent chain hanging a command instead of
// failing it.
const defaultPullHops = 8

// BlobPort satisfies uns.Blobs over this node's blob store and its parent
// link. The domain declares that interface; this is the core half.
type BlobPort struct {
	store *blobstore.Store

	// mu guards fetcher only: it is written once at startup, after the
	// listeners are already serving, and read by every command thereafter.
	mu      sync.Mutex
	fetcher BlobFetcher
}

func NewBlobPort(store *blobstore.Store) *BlobPort {
	return &BlobPort{store: store}
}

// SetFetcher installs the parent link. Called once at startup, after the repl
// client exists; a root node never calls it and its Pull fails honestly.
func (p *BlobPort) SetFetcher(f BlobFetcher) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fetcher = f
}

func (p *BlobPort) upstream() BlobFetcher {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fetcher
}

func (p *BlobPort) Has(sha string) bool {
	if p == nil || p.store == nil {
		return false
	}
	_, ok := p.store.Has(sha)
	return ok
}

// Pull fetches one blob from the parent and stores it. Put re-verifies the
// digest, so a lying or corrupt ancestor is refused here rather than becoming
// the bytes behind a resource.
func (p *BlobPort) Pull(sha string) error {
	if p == nil || p.store == nil {
		return errors.New("this node has no blob store")
	}
	up := p.upstream()
	if up == nil {
		return errors.New("this node has no parent to fetch from")
	}
	rc, _, err := up.BlobGet(sha, defaultPullHops)
	if err != nil {
		return fmt.Errorf("fetch from parent: %w", err)
	}
	defer rc.Close()
	if _, _, err := p.store.Put(rc, sha); err != nil {
		return fmt.Errorf("store the fetched blob: %w", err)
	}
	return nil
}

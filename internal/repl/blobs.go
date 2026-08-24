package repl

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// handleBlobHead answers whether this node holds a blob. It is what lets a
// child ask "do you need this?" before spending bandwidth on a push.
func (s *Server) handleBlobHead(w http.ResponseWriter, r *http.Request) {
	if _, _, err := s.childFromReq(r); err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	sha := r.PathValue("sha")
	size, ok := s.blobs.Has(sha)
	if !ok {
		http.Error(w, "no such blob", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
}

// handleBlobPut accepts a blob from an enrolled child. The digest in the path
// is verified against the content, so a push cannot poison the store even if
// the child is buggy.
func (s *Server) handleBlobPut(w http.ResponseWriter, r *http.Request) {
	child, _, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	sha := r.PathValue("sha")
	max := int64(s.cfg.Limits.EffectiveMaxBlobBytes())
	r.Body = http.MaxBytesReader(w, r.Body, max)

	if _, _, putErr := s.blobs.Put(r.Body, sha); putErr != nil {
		// One classification, one counting site, one response write: a new
		// error case added later cannot land counted on one branch and silent
		// on another, which is exactly how the 413 path went uncounted before.
		status, reason, message := blobPutOutcome(putErr, max)
		if reason != "" {
			s.metrics.BlobRejected(reason)
		}
		s.metrics.BlobTransfer("receive", "error")
		http.Error(w, message, status)
		return
	}
	s.log.Debug("stored a blob from a child", "child", child.ULID, "sha", sha[:12])
	s.metrics.BlobTransfer("receive", "ok")
	w.WriteHeader(http.StatusNoContent)
}

// blobPutOutcome maps a blobstore.Put error to the HTTP status, the
// BlobRejected reason to record (resources design §5), and the response
// body. reason is "" for an error that is not one of the three known
// ingress-rejection reasons (a genuine internal failure) — BlobRejected only
// ever records those three, never a fourth label.
//
// blobstore.ErrTooLarge is deliberately not one of the cases here. r.Body is
// wrapped in http.MaxBytesReader with the SAME cap
// (cfg.Limits.EffectiveMaxBlobBytes(), the same config the store itself was
// opened with in node.Start) before Put ever sees the stream, so
// MaxBytesReader always trips first and Put's own size check can never fire
// on this door — a case for it here would be dead code. blobstore.Put keeps
// its own check regardless: that is the store's unconditional guarantee, not
// this door's, and it still holds for any other caller of Put that does not
// wrap its reader the same way.
func blobPutOutcome(err error, max int64) (status int, reason, message string) {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("blob exceeds %d bytes", max)
	case errors.Is(err, blobstore.ErrDigestMismatch):
		return http.StatusBadRequest, "digest_mismatch", err.Error()
	case errors.Is(err, blobstore.ErrBadDigest):
		return http.StatusBadRequest, "bad_digest", err.Error()
	default:
		return http.StatusInternalServerError, "", err.Error()
	}
}

// blobHopsHeader bounds pull-through recursion. Depth, not cycles, is the
// real risk — the tree has no cycles — but an unbounded rootward walk on a
// misconfigured parent chain would hang a request instead of failing it.
const blobHopsHeader = "X-Colca-Blob-Hops"

const defaultBlobHops = 8

// handleBlobGet serves a blob to a child, fetching it from this node's own
// parent on a miss and caching what it relays (resources design §7.1).
//
// This is the direction provisioning needs: a file staged at the root reaches
// a headless leaf. Every request in the chain is still a child dialing its
// parent — no parent ever dials down.
func (s *Server) handleBlobGet(w http.ResponseWriter, r *http.Request) {
	if _, _, err := s.childFromReq(r); err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	sha := r.PathValue("sha")

	if rc, size, err := s.blobs.Get(sha); err == nil {
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)
		return
	} else if errors.Is(err, blobstore.ErrBadDigest) {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	hops := defaultBlobHops
	if raw := r.Header.Get(blobHopsHeader); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil {
			hops = n
		}
	}
	// The hop that just delivered this request already spent one unit of the
	// budget — TTL-style: decrement on arrival, not only when forwarding.
	// Without this, a local hit at the node we ask next answers before it
	// ever reads the header, so the budget would never actually bound
	// anything: only the LAST node's local store matters, not how many of
	// them a request may cross to get there.
	hops--
	up := s.upstream()
	if up == nil || hops <= 0 {
		http.Error(w, "no such blob", http.StatusNotFound)
		return
	}

	rc, size, err := up.BlobGet(sha, hops)
	if err != nil {
		s.metrics.BlobTransfer("pull", "error")
		http.Error(w, "no such blob", http.StatusNotFound)
		return
	}
	defer rc.Close()

	// Cache what we relay: a blob provisioned to many siblings then crosses
	// each upper link once. Put verifies the digest, so a corrupt upstream
	// answer is refused here rather than passed on.
	if _, _, err := s.blobs.Put(rc, sha); err != nil {
		s.metrics.BlobTransfer("pull", "error")
		http.Error(w, "no such blob", http.StatusNotFound)
		return
	}
	s.metrics.BlobTransfer("pull", "ok")

	cached, cachedSize, err := s.blobs.Get(sha)
	if err != nil {
		http.Error(w, "no such blob", http.StatusNotFound)
		return
	}
	defer cached.Close()
	_ = size
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(cachedSize, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, cached)
}

// BlobGet asks the parent for a blob, letting it recurse rootward on a miss.
func (c *Client) BlobGet(sha string, hops int) (io.ReadCloser, int64, error) {
	req, err := http.NewRequest(http.MethodGet, c.base+"/blobs/"+sha, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set(blobHopsHeader, strconv.Itoa(hops))
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("blob get %s: %s", sha[:12], resp.Status)
	}
	return resp.Body, resp.ContentLength, nil
}

// BlobHas asks the parent whether it already holds sha. A cheap question that
// keeps a push from re-sending what the parent has — including everything it
// received from a sibling, since content addressing makes those the same blob.
func (c *Client) BlobHas(sha string) (bool, error) {
	req, err := http.NewRequest(http.MethodHead, c.base+"/blobs/"+sha, nil)
	if err != nil {
		return false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("blob head %s: %s", sha[:12], resp.Status)
	}
}

// BlobPutError reports a PUT the parent actually answered, as opposed to a
// transport failure (dial error, timeout, connection reset), which never
// becomes one of these. Carrying the status code as a typed field — rather
// than folding it into a formatted string — lets a caller decide what the
// failure means without parsing prose: a 4xx says the parent will never
// accept this exact blob (bad digest, over its cap), a 5xx says the parent
// itself is unhealthy right now.
type BlobPutError struct {
	SHA    string
	Status int
	Body   string
}

func (e *BlobPutError) Error() string {
	return fmt.Sprintf("blob put %s: %d: %s", e.SHA[:12], e.Status, e.Body)
}

// blobRejected reports whether err is a BlobPutError with a 4xx status — the
// parent answered, and it will never accept this specific blob as it stands.
// Any other failure (a transport error, or a BlobPutError with a 5xx) says
// nothing about this particular blob: it means the parent is not currently
// accepting pushes at all.
func blobRejected(err error) bool {
	var pe *BlobPutError
	return errors.As(err, &pe) && pe.Status >= 400 && pe.Status < 500
}

// BlobPut sends one blob to the parent whole. There is no chunking or
// resumption: the configured cap is what makes whole-file transfer with retry
// sufficient, which is why no such protocol exists here.
func (c *Client) BlobPut(sha string, r io.Reader, size int64) error {
	req, err := http.NewRequest(http.MethodPut, c.base+"/blobs/"+sha, r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &BlobPutError{SHA: sha, Status: resp.StatusCode, Body: string(body)}
	}
	return nil
}

// syncBlobs pushes every local blob the parent does not hold, one whole file
// at a time (resources design §7). It runs AFTER the record lanes drain, so a
// large file can never queue ahead of an alarm or an entity batch — blobs do
// not ride the streams at all.
//
// confirmed and rejected are caller-owned and scoped to one pinned parent
// key: a reparent builds a new Client and new maps, which is what re-offers
// everything to the new parent — the new parent may hold what the old one
// didn't, and may accept what the old one capped out on. Nothing is
// persisted; a restart re-verifies with a HEAD per blob, which is cheap and
// idempotent.
func syncBlobs(c *Client, blobs *blobstore.Store, m *metrics.Metrics, confirmed, rejected map[string]bool) int {
	if c == nil || blobs == nil {
		return 0
	}
	list, err := blobs.List()
	if err != nil {
		c.log.Warn("cannot enumerate blobs", "err", err)
		return 0
	}
	pushed := 0
	for _, info := range list {
		if confirmed[info.SHA256] || rejected[info.SHA256] {
			continue
		}
		has, err := c.BlobHas(info.SHA256)
		if err != nil {
			// The parent is unreachable or unhappy; the next pass retries.
			// Nothing is marked confirmed, so no blob is lost by giving up here.
			c.log.Debug("blob head failed", "sha", info.SHA256[:12], "err", err)
			return pushed
		}
		if has {
			confirmed[info.SHA256] = true
			continue
		}
		rc, size, err := blobs.Get(info.SHA256)
		if err != nil {
			// Swept between List and Get — normal, not a fault.
			continue
		}
		err = c.BlobPut(info.SHA256, rc, size)
		rc.Close()
		if err != nil {
			m.BlobTransfer("push", "error")
			if blobRejected(err) {
				// The parent answered and refused this exact blob (bad
				// digest, over its cap) — re-offering it unchanged can
				// never succeed. Skip it, but keep going: one
				// permanently-rejected blob must not starve every blob
				// behind it in List() order, forever.
				c.log.Warn("blob rejected by parent, skipping", "sha", info.SHA256[:12], "err", err)
				rejected[info.SHA256] = true
				continue
			}
			// Transport failure or a 5xx: the parent is not currently
			// accepting pushes at all. Nothing later in this pass will
			// do better; the next pass retries from the top.
			c.log.Warn("blob push failed", "sha", info.SHA256[:12], "err", err)
			return pushed
		}
		confirmed[info.SHA256] = true
		pushed++
		m.BlobTransfer("push", "ok")
	}
	return pushed
}

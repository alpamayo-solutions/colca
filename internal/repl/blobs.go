package repl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// handleBlobHead answers whether this node holds a blob. It is what lets a
// child ask "do you need this?" before spending bandwidth on a push.
func (s *Server) handleBlobHead(w http.ResponseWriter, r *http.Request) {
	child, _, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	release, ok := s.acquireRequest(w, limitClassReplTransfer, child.ULID, replTransferPolicy)
	if !ok {
		return
	}
	defer release()
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
	release, ok := s.acquireRequest(w, limitClassReplTransfer, child.ULID, replTransferPolicy)
	if !ok {
		return
	}
	defer release()
	sha := r.PathValue("sha")
	limit := int64(s.cfg.Limits.EffectiveMaxBlobBytes()) //nolint:gosec // config caps max_blob_bytes
	r.Body = http.MaxBytesReader(w, r.Body, limit)

	if _, _, putErr := s.blobs.Put(r.Body, sha); putErr != nil {
		// One classification, one place that counts and one response write, so a new
		// error case cannot end up counted on one branch and silent on another.
		status, reason, message := blobPutOutcome(putErr, limit)
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
// BlobRejected reason and the response body. reason is "" for an internal
// failure; BlobRejected only records the three known reasons. ErrTooLarge
// cannot occur here: the body is wrapped in http.MaxBytesReader with the same
// cap, which trips first.
func blobPutOutcome(err error, limit int64) (status int, reason, message string) {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("blob exceeds %d bytes", limit)
	case errors.Is(err, blobstore.ErrDigestMismatch):
		return http.StatusBadRequest, "digest_mismatch", err.Error()
	case errors.Is(err, blobstore.ErrBadDigest):
		return http.StatusBadRequest, "bad_digest", err.Error()
	default:
		return http.StatusInternalServerError, "", err.Error()
	}
}

// blobHopsHeader bounds pull-through recursion. The tree has no cycles, but a
// misconfigured parent chain must fail the request instead of hanging it.
const blobHopsHeader = "X-Colca-Blob-Hops"

const defaultBlobHops = 8

// handleBlobGet serves a blob to a child, fetching it from this node's parent
// on a miss and caching what it relays. That is how a file staged at the root
// reaches a headless leaf, with every request still a child dialing its parent.
func (s *Server) handleBlobGet(w http.ResponseWriter, r *http.Request) {
	child, _, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	release, ok := s.acquireRequest(w, limitClassReplTransfer, child.ULID, replTransferPolicy)
	if !ok {
		return
	}
	defer release()
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
	// The hop that delivered this request already spent one unit of the budget.
	// Decrement on arrival: otherwise a local hit would answer before the header is
	// read and the budget would bound nothing.
	hops--
	up := s.upstream()
	if up == nil || hops <= 0 {
		http.Error(w, "no such blob", http.StatusNotFound)
		return
	}

	rc, size, err := up.BlobGet(sha, hops)
	if err != nil {
		s.metrics.BlobTransfer("pull", "error")
		s.log.Warn("blob pull-through miss: no ancestor holds it", "sha", sha[:12], "err", err)
		http.Error(w, "no such blob", http.StatusNotFound)
		return
	}
	defer rc.Close()

	// Cache what we relay: a blob provisioned to many siblings then crosses
	// each upper link once. Put verifies the digest, so a corrupt upstream
	// answer is refused here rather than passed on.
	if _, _, err := s.blobs.Put(rc, sha); err != nil {
		s.metrics.BlobTransfer("pull", "error")
		s.log.Warn("blob pull-through relay refused: upstream answer failed local verification", "sha", sha[:12], "err", err)
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
	// Relay the size of our own cached copy rather than the upstream
	// Content-Length: it is what the bytes we copy actually measure.
	_ = size
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(cachedSize, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, cached)
}

// deadlineBody bounds a streamed response body that the caller, not this
// function, reads. The deadline cannot be set on the request: it depends on
// how many bytes the answer carries, which is only known once the parent's
// headers arrive. Closing the body stops the clock and releases the request.
type deadlineBody struct {
	io.ReadCloser
	timer  *time.Timer
	cancel context.CancelFunc
}

func (b *deadlineBody) Close() error {
	b.timer.Stop()
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

// BlobGet asks the parent for a blob, letting it recurse rootward on a miss.
// The body carries a deadline derived from the size the parent announced (see
// transferDeadline); the caller must Close it.
func (c *Client) BlobGet(sha string, hops int) (io.ReadCloser, int64, error) {
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/blobs/"+sha, nil)
	if err != nil {
		cancel()
		return nil, 0, err
	}
	req.Header.Set(blobHopsHeader, strconv.Itoa(hops))
	// Until the headers are in, the exchange is bounded by time alone: a
	// parent that never answers must not hold this open.
	headers := time.AfterFunc(transferGrace, cancel)
	resp, err := c.http.Do(req)
	if err != nil {
		headers.Stop()
		cancel()
		return nil, 0, err
	}
	headers.Stop()
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, 0, fmt.Errorf("blob get %s: %s", sha[:12], resp.Status)
	}
	return &deadlineBody{
		ReadCloser: resp.Body,
		timer:      time.AfterFunc(transferDeadline(resp.ContentLength), cancel),
		cancel:     cancel,
	}, resp.ContentLength, nil
}

// BlobHas asks the parent whether it already holds sha. A cheap question that
// keeps a push from re-sending what the parent has — including everything it
// received from a sibling, since content addressing makes those the same blob.
func (c *Client) BlobHas(sha string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), transferGrace)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.base+"/blobs/"+sha, nil)
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

// BlobPutError is a PUT the parent answered, as opposed to a transport failure.
// The status code tells the caller what it means: a 4xx says the parent will
// never accept this blob, a 5xx that the parent is unhealthy right now.
type BlobPutError struct {
	SHA    string
	Status int
	Body   string
}

func (e *BlobPutError) Error() string {
	return fmt.Sprintf("blob put %s: %d: %s", e.SHA[:12], e.Status, e.Body)
}

// blobRejected reports whether err is a BlobPutError with a 4xx status, meaning
// the parent will never accept this blob. Any other failure says the parent is
// not taking pushes at all right now. A 403 cannot reach here: the HEAD in
// syncBlobs fails first for an unenrolled child.
func blobRejected(err error) bool {
	var pe *BlobPutError
	return errors.As(err, &pe) && pe.Status >= 400 && pe.Status < 500
}

// BlobPut sends one blob to the parent whole. There is no chunking or
// resumption: the configured cap is what makes whole-file transfer with retry
// sufficient, which is why no such protocol exists here.
func (c *Client) BlobPut(sha string, r io.Reader, size int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), transferDeadline(size))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+"/blobs/"+sha, r)
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

// syncBlobs pushes every local blob the parent lacks, one whole file at a time,
// after the record lanes have drained. confirmed and rejected belong to the
// caller and one parent key: a reparent starts with new maps and offers
// everything again. confirmed holds the local blob's modification time when it
// was confirmed, so a blob swept by blobgc and staged again later, which Put
// always writes fresh, is offered again instead of being skipped forever.
func syncBlobs(c *Client, blobs *blobstore.Store, m *metrics.Metrics, confirmed map[string]time.Time, rejected map[string]bool) int {
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
		if rejected[info.SHA256] {
			continue
		}
		if at, ok := confirmed[info.SHA256]; ok && !info.Modified.After(at) {
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
			confirmed[info.SHA256] = info.Modified
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
				// The parent refused this exact blob, so offering it again cannot succeed. Skip
				// it but keep going, so one rejected blob does not block the rest.
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
		confirmed[info.SHA256] = info.Modified
		pushed++
		m.BlobTransfer("push", "ok")
	}
	return pushed
}

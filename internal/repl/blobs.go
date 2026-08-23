package repl

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
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
		return fmt.Errorf("blob put %s: %s: %s", sha[:12], resp.Status, body)
	}
	return nil
}

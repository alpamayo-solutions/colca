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

	if _, _, err := s.blobs.Put(r.Body, sha); err != nil {
		switch {
		case errors.Is(err, blobstore.ErrTooLarge):
			http.Error(w, fmt.Sprintf("blob exceeds %d bytes", max), http.StatusRequestEntityTooLarge)
		case errors.Is(err, blobstore.ErrDigestMismatch), errors.Is(err, blobstore.ErrBadDigest):
			http.Error(w, err.Error(), http.StatusBadRequest)
		default:
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, fmt.Sprintf("blob exceeds %d bytes", max), http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		s.metrics.BlobTransfer("receive", "error")
		return
	}
	s.log.Debug("stored a blob from a child", "child", child.ULID, "sha", sha[:12])
	s.metrics.BlobTransfer("receive", "ok")
	w.WriteHeader(http.StatusNoContent)
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

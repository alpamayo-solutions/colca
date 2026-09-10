package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

// mountBlobRoutes adds the local door's blob endpoints. They exist only on the
// local door, where reaching it is the credential. On an authenticated door a
// file is read through its resource id, so the element grant check always runs: a
// digest is a pointer, not a capability.
func mountBlobRoutes(
	mux *http.ServeMux,
	blobs *blobstore.Store,
	m *metrics.Metrics,
	maxBytes uint64,
	writeJSON func(http.ResponseWriter, int, any),
	auth endpointAuth,
) {
	if blobs == nil {
		return
	}

	mux.HandleFunc("POST /blobs", auth(limitClassTransfer, transferPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytes)) //nolint:gosec // config caps max_blob_bytes
		sha, size, err := blobs.Put(r.Body, r.Header.Get("X-Colca-Blob-SHA256"))
		// Local uploads are not counted as BlobTransfer, which is for node-to-node
		// transfers. blobstore.ErrTooLarge cannot occur here: MaxBytesReader with the same
		// cap always trips first.
		var tooLarge *http.MaxBytesError
		switch {
		case err == nil:
			writeJSON(w, http.StatusCreated, map[string]any{"sha256": sha, "size": size})
		case errors.As(err, &tooLarge):
			m.BlobRejected("too_large")
			writeJSON(w, http.StatusRequestEntityTooLarge,
				map[string]any{"error": fmt.Sprintf("blob exceeds %d bytes", maxBytes)})
		case errors.Is(err, blobstore.ErrDigestMismatch):
			m.BlobRejected("digest_mismatch")
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		case errors.Is(err, blobstore.ErrBadDigest):
			m.BlobRejected("bad_digest")
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		default:
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		}
	}))

	serve := func(w http.ResponseWriter, r *http.Request, body bool) {
		sha := r.PathValue("sha")
		if body {
			rc, size, err := blobs.Get(sha)
			if err != nil {
				blobReadError(w, err, writeJSON)
				return
			}
			defer rc.Close()
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.WriteHeader(http.StatusOK)
			_, _ = io.Copy(w, rc)
			return
		}
		size, ok := blobs.Has(sha)
		if !ok {
			// Has cannot tell a malformed digest from an absent one, so ask Get, keeping HEAD
			// consistent with GET. If the blob landed in between, close the reader and report
			// its real size.
			rc, gotSize, err := blobs.Get(sha)
			if err != nil {
				blobReadError(w, err, writeJSON)
				return
			}
			rc.Close()
			size = gotSize
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
	}

	mux.HandleFunc("GET /blobs/{sha}", auth(limitClassTransfer, transferPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		serve(w, r, true)
	}))
	mux.HandleFunc("HEAD /blobs/{sha}", auth(limitClassTransfer, transferPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		serve(w, r, false)
	}))
}

// blobReadError maps a blobstore read error to its HTTP status. Reads never count
// BlobRejected, which is for blobs refused on upload.
func blobReadError(w http.ResponseWriter, err error, writeJSON func(http.ResponseWriter, int, any)) {
	switch {
	case errors.Is(err, blobstore.ErrBadDigest):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	case errors.Is(err, blobstore.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "no such blob"})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
}

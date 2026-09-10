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

// mountBlobRoutes adds the local door's blob endpoints (resources design §4).
//
// These exist ONLY on the local door. Reachability from inside the
// deployment's own network is the credential there, exactly as it is for
// every other local read. On an authenticated door a file is reached through
// its resource id so the element-scoped grant check always runs — a digest is
// a pointer, never a capability.
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
		// BlobTransfer is deliberately NOT counted on this door, unlike the
		// repl door's handleBlobPut/handleBlobGet. That metric family means
		// node-to-node transfers (push/pull/receive); a local staging upload
		// is not one, and counting it here would poison the counter.
		//
		// blobstore.ErrTooLarge is deliberately not one of the cases below.
		// r.Body is wrapped in http.MaxBytesReader with the SAME cap
		// (maxBytes, the same config value the store itself was opened with)
		// before Put ever sees the stream, so MaxBytesReader always trips
		// first and Put's own size check can never fire on this door — a
		// case for it here would be dead code. blobstore.Put keeps its own
		// check regardless: that is the store's unconditional guarantee, not
		// this door's, and it still holds for any other caller of Put that
		// does not wrap its reader the same way.
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
			// Has() cannot tell a malformed digest from an absent one, so ask
			// Get() which error it would have been — HEAD must agree with GET.
			// The blob may have landed between the Has() miss above and this
			// Get() call: on that success path the read must still be closed
			// (nothing else will) and its real size used, not the 0 Has()
			// reported for a blob it never found.
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

// blobReadError maps a blobstore read error to its HTTP status. Unlike
// mountBlobRoutes' POST arm, this never touches m.BlobRejected: that metric
// counts blobs refused AT INGRESS (its own doc comment, resources design
// §5) — a malformed or absent digest on a read is a different failure mode,
// not a write the store turned away.
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

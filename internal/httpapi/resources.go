package httpapi

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// mountResourceRoutes adds the resource-file read, on BOTH the local and the
// published door (resources design §6, revised: a digest is a pointer, never
// a capability, so raw digest access must never be how a person's or a
// forwarded service's read is authorized — not even on the local door). Raw
// /blobs/{sha} remains local- and replication-door-only, where the door's own
// trust model (deployment-network reachability, parent pinning) IS the
// authorization for whichever local service or child node reads it. This
// route is the ONLY way a file is read anywhere else: every read passes
// through a resource id so the element-scoped grant check always runs,
// whichever door the caller reached — a forwarded human Bearer on the local
// door is authorized exactly as an Edit command is (node-side command
// authorization design §3B), and a plain local-service caller is authorized
// by its own placement, the same as any other local read.
func mountResourceRoutes(
	mux *http.ServeMux,
	e *engine.Engine,
	blobs *blobstore.Store,
	m *metrics.Metrics,
	writeJSON func(http.ResponseWriter, int, any),
	auth endpointAuth,
) {
	if blobs == nil {
		return
	}
	var es uns.EntityStore = e.EntityStore()

	mux.HandleFunc("GET /resources/{id}/file", auth(limitClassTransfer, transferPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		id := r.PathValue("id")

		// No by-id index exists for entities, so the record is found by scan.
		// Resources are few relative to metrics; if that stops being true this
		// is the place an index goes.
		var topic string
		var payload []byte
		for _, rec := range es.KVScanAll(uns.ResourceContract) {
			if got, ok := uns.ResourceID(rec.Payload); ok && got == id {
				topic, payload = rec.Topic, rec.Payload
				break
			}
		}
		if topic == "" {
			m.ResourceRead("not_found")
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "no such resource"})
			return
		}

		if !c.admin && !uns.Authorize(e.Scope(), c.entry, uns.ActReadRecord, topic) {
			m.ResourceRead("denied")
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "not granted on this element"})
			return
		}

		sha, ok := uns.ResourceBlob(payload)
		if !ok {
			m.ResourceRead("not_found")
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "the resource names no readable digest"})
			return
		}

		rc, size, err := blobs.Get(sha)
		if err != nil {
			resourceBlobError(w, m, err, sha, writeJSON)
			return
		}
		defer rc.Close()
		m.ResourceRead("ok")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)
	}))
}

// resourceBlobError maps a blobstore read error to this route's response.
// Same discriminator shape as blobReadError (blobs.go), but a different
// mapping — this route's caller has already been told the resource EXISTS
// (the 404 above owns "no such resource"), so a blob-store error here can
// only mean one of two things, and only one of them is retryable:
//
//   - ErrNotFound: the bytes genuinely have not arrived. This is the only
//     case that answers 409 blob_pending — a promise that the file is in
//     flight and an ancestor can only wait for the child's push, never pull
//     it rootward.
//   - anything else (ErrBadDigest included): an internal fault, not a
//     "not yet" — validateResourcePayload enforces isSHA256Hex before a
//     _Resource is ever written, so a stored record can never legitimately
//     carry a malformed digest; reaching ErrBadDigest here means this node's
//     own state is inconsistent. A generic I/O error means the same thing
//     one level down (disk or permission fault on this node). Neither is
//     the caller's problem to retry, so both are 500 — collapsing them into
//     blob_pending would have an operator or the editor's retry loop
//     polling forever on something that will never clear.
func resourceBlobError(w http.ResponseWriter, m *metrics.Metrics, err error, sha string, writeJSON func(http.ResponseWriter, int, any)) {
	if errors.Is(err, blobstore.ErrNotFound) {
		m.ResourceRead("pending")
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":  "blob_pending",
			"sha256": sha,
			"detail": "the resource's file has not replicated to this node yet",
		})
		return
	}
	m.ResourceRead("error")
	// The raw error can carry a filesystem path (blobstore wraps os errors
	// with %w); a static body keeps that off the wire, and the real error
	// still reaches operators through the log.
	slog.Default().Error("resource file read failed", "sha256", sha, "err", err)
	writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "blob read failed"})
}

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

// mountResourceRoutes adds the resource-file read on both doors. A digest is a
// pointer, never a capability, so a person's or forwarded service's read always
// goes through a resource id and its element grant check. Raw /blobs/{sha} stays
// on the local and replication doors, whose own trust model is the authorization.
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
	es := e.EntityStore()

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

// resourceBlobError maps a blobstore read error for a resource that exists. Only
// ErrNotFound, bytes not arrived yet, is 409 blob_pending. Anything else,
// ErrBadDigest included, means this node's state or disk is broken and is 500, so
// no client polls forever for something that will not clear.
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

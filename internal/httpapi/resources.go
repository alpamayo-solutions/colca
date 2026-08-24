package httpapi

import (
	"io"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// mountResourceRoutes adds the published door's resource read (resources
// design §6).
//
// This is the ONLY way a file is read on an authenticated door. Raw
// /blobs/{sha} lives on the local and replication doors, whose own trust model
// is the authorization; here every read passes through a resource id so the
// element-scoped grant check always runs. A digest is a pointer, not a
// capability.
func mountResourceRoutes(
	mux *http.ServeMux,
	e *engine.Engine,
	blobs *blobstore.Store,
	m *metrics.Metrics,
	writeJSON func(http.ResponseWriter, int, any),
	auth func(func(http.ResponseWriter, *http.Request, caller)) http.HandlerFunc,
) {
	if blobs == nil {
		return
	}
	var es uns.EntityStore = e.EntityStore()

	mux.HandleFunc("GET /resources/{id}/file", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
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
			// The resource exists; its bytes have not arrived yet. An ancestor
			// cannot fetch them rootward — only the child's push brings them —
			// so this is a retry-later, not a dead end.
			m.ResourceRead("pending")
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":  "blob_pending",
				"sha256": sha,
				"detail": "the resource's file has not replicated to this node yet",
			})
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

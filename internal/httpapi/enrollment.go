package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/alpamayo-solutions/colca/internal/enroll"
	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// enrollmentStatus maps a decision error to its HTTP status.
func enrollmentStatus(err error) int {
	switch {
	case errors.Is(err, enroll.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, enroll.ErrConflict), errors.Is(err, enroll.ErrBelowPolicy):
		return http.StatusConflict
	case errors.Is(err, enroll.ErrInvalid):
		return http.StatusUnprocessableEntity
	}
	return http.StatusInternalServerError
}

func actorOf(c caller) enroll.Actor {
	label := c.human.Username
	if label == "" {
		label = c.human.Sub
	}
	return enroll.Actor{ID: c.human.Sub, Label: label, Kind: "human"}
}

// mountEnrollmentRoutes serves the decisions on enrollment requests and
// pre-approvals (node enrollment spec §5, §5.1). Only a person holding
// admin:# decides; the static admin token is refused, so every decision names
// somebody.
func mountEnrollmentRoutes(mux *http.ServeMux, enr *enroll.Manager, writeJSON secretJSONWriter, auth endpointAuth) {
	human := func(class string, policy httplimit.Policy, next func(w http.ResponseWriter, r *http.Request, a enroll.Actor)) http.HandlerFunc {
		return auth(class, policy, func(w http.ResponseWriter, r *http.Request, c caller) {
			if c.human == nil || !c.human.Entry.IsAdmin() {
				writeJSON(w, http.StatusForbidden, map[string]any{
					"error": "deciding enrollment requests needs a person's token with admin:# — the admin token is not accepted here",
				})
				return
			}
			next(w, r, actorOf(c))
		})
	}
	decodeBody := func(w http.ResponseWriter, r *http.Request, v any) bool {
		r.Body = http.MaxBytesReader(w, r.Body, maxEnrollBodyBytes)
		if err := json.NewDecoder(r.Body).Decode(v); err != nil && !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "request body: " + err.Error()})
			return false
		}
		return true
	}
	fingerprint := func(w http.ResponseWriter, r *http.Request) (string, bool) {
		fp, err := pubkey.ParseFingerprint(r.PathValue("fp"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return "", false
		}
		return fp, true
	}
	decided := func(w http.ResponseWriter, err error, ok map[string]any) {
		if err != nil {
			writeJSON(w, enrollmentStatus(err), map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, ok)
	}

	mux.HandleFunc("GET /enroll/requests", human(limitClassScan, scanPolicy, func(w http.ResponseWriter, r *http.Request, _ enroll.Actor) {
		pageSize, after, err := pageRequest(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "max must be between 1 and 10000"})
			return
		}
		state := r.URL.Query().Get("state")
		switch state {
		case "", uns.RequestPending, uns.RequestRejected, uns.RequestBlocked:
		default:
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "state must be pending, rejected or blocked"})
			return
		}
		if after != "" {
			if after, err = pubkey.ParseFingerprint(after); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "after: " + err.Error()})
				return
			}
		}
		items, next := enr.Requests(state, after, pageSize)
		if items == nil {
			items = []uns.EnrollmentRequest{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"requests": items, "next": next})
	}))
	mux.HandleFunc("GET /enroll/requests/{fp}", human(limitClassCheap, cheapPolicy, func(w http.ResponseWriter, r *http.Request, _ enroll.Actor) {
		fp, ok := fingerprint(w, r)
		if !ok {
			return
		}
		req, err := enr.Request(fp)
		if err != nil {
			writeJSON(w, enrollmentStatus(err), map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, req)
	}))
	mux.HandleFunc("POST /enroll/requests/{fp}/approve", human(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request, a enroll.Actor) {
		fp, ok := fingerprint(w, r)
		if !ok {
			return
		}
		var body struct {
			Element string `json:"element"`
			Mount   string `json:"mount"`
			Name    string `json:"name"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		decided(w, enr.Approve(fp, body.Element, body.Mount, body.Name, a), map[string]any{"status": "approved"})
	}))
	mux.HandleFunc("POST /enroll/requests/{fp}/reject", human(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request, a enroll.Actor) {
		fp, ok := fingerprint(w, r)
		if !ok {
			return
		}
		var body struct {
			Reason string `json:"reason"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		decided(w, enr.Reject(fp, body.Reason, a), map[string]any{"status": uns.RequestRejected})
	}))
	mux.HandleFunc("POST /enroll/requests/{fp}/block", human(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request, a enroll.Actor) {
		fp, ok := fingerprint(w, r)
		if !ok {
			return
		}
		var body struct {
			Reason string `json:"reason"`
			ULID   string `json:"ulid"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		decided(w, enr.Block(fp, body.Reason, body.ULID, a), map[string]any{"status": uns.RequestBlocked})
	}))
	mux.HandleFunc("POST /enroll/requests/{fp}/unblock", human(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request, a enroll.Actor) {
		fp, ok := fingerprint(w, r)
		if !ok {
			return
		}
		decided(w, enr.Unblock(fp, a), map[string]any{"status": uns.RequestPending})
	}))
	mux.HandleFunc("POST /enroll/{ulid}/request-key-change", human(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request, a enroll.Actor) {
		decided(w, enr.RequestKeyChange(r.PathValue("ulid"), a), map[string]any{"ulid": r.PathValue("ulid"), "key_change_requested": true})
	}))

	mux.HandleFunc("GET /enroll/preapprovals", human(limitClassScan, scanPolicy, func(w http.ResponseWriter, r *http.Request, _ enroll.Actor) {
		pageSize, after, err := pageRequest(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "max must be between 1 and 10000"})
			return
		}
		items, next := enr.Preapprovals(after, pageSize)
		if items == nil {
			items = []uns.EnrollmentPreapproval{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"preapprovals": items, "next": next})
	}))
	mux.HandleFunc("POST /enroll/preapprovals", human(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request, a enroll.Actor) {
		var body struct {
			ID        string              `json:"id"`
			Match     uns.EnrollmentMatch `json:"match"`
			Element   string              `json:"element"`
			Mount     string              `json:"mount"`
			Name      string              `json:"name"`
			ExpiresAt string              `json:"expires_at"`
			Uses      int                 `json:"uses"`
			Note      string              `json:"note"`
		}
		if !decodeBody(w, r, &body) {
			return
		}
		in := enroll.PreapprovalInput{ID: body.ID, Match: body.Match, Element: body.Element, Mount: body.Mount,
			Name: body.Name, Uses: body.Uses, Note: body.Note}
		if body.ExpiresAt != "" {
			t, err := time.Parse(time.RFC3339, body.ExpiresAt)
			if err != nil {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": "expires_at must be RFC 3339"})
				return
			}
			in.ExpiresAt = t
		}
		p, err := enr.Preapprove(in, a)
		if err != nil {
			writeJSON(w, enrollmentStatus(err), map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, p)
	}))
	mux.HandleFunc("DELETE /enroll/preapprovals/{id}", human(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request, a enroll.Actor) {
		decided(w, enr.Unpreapprove(r.PathValue("id"), a), map[string]any{"id": r.PathValue("id"), "deleted": true})
	}))
}

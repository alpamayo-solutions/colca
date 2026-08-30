package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/alpamayo-solutions/colca/internal/secretstore"
	"github.com/alpamayo-solutions/colca/secrets"
)

const maxSecretRequestBody = 96 << 10

type secretWrite struct {
	Envelope         secrets.Envelope `json:"envelope"`
	ExpiresAt        *time.Time       `json:"expires_at,omitempty"`
	ExpectedRevision *uint64          `json:"expected_revision,omitempty"`
}

type secretJSONWriter func(http.ResponseWriter, int, any)

func mountLocalSecretRoutes(mux *http.ServeMux, store *secretstore.Store, writeJSON secretJSONWriter, auth endpointAuth) {
	mux.HandleFunc("GET /secrets", auth(limitClassScan, scanPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		pageSize, after, err := pageRequest(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "max must be between 1 and 10000"})
			return
		}
		items, next, err := store.ListPage(c.entry.Name, after, pageSize)
		if err != nil {
			if errors.Is(err, secretstore.ErrInvalidPageToken) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid page token"})
				return
			}
			secretStoreError(w, writeJSON, "list", c.entry.Name, "", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"secrets": metadataWithExpiry(items, time.Now()), "next": next})
	}))

	mux.HandleFunc("GET /secrets/{name...}", auth(limitClassCheap, cheapPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		name := r.PathValue("name")
		record, err := store.Get(c.entry.Name, name)
		if err != nil {
			secretStoreError(w, writeJSON, "get", c.entry.Name, name, err)
			return
		}
		if expired(record.ExpiresAt, time.Now()) {
			writeJSON(w, http.StatusGone, map[string]any{"error": "secret expired", "secret": metadataMap(record.Metadata(), time.Now())})
			return
		}
		writeJSON(w, http.StatusOK, record)
	}))

	mux.HandleFunc("PUT /secrets/{name...}", auth(limitClassWrite, writePolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		in, ok := decodeSecretWrite(w, r, writeJSON)
		if !ok {
			return
		}
		name := r.PathValue("name")
		record, err := store.Put(c.entry.Name, name, in.Envelope, in.ExpiresAt, in.ExpectedRevision, time.Now())
		if err != nil {
			secretStoreError(w, writeJSON, "put", c.entry.Name, name, err)
			return
		}
		slog.Info("local secret stored", "service", c.entry.Name, "service_id", c.entry.ULID,
			"name", name, "revision", record.Revision, "key_id", record.Envelope.KeyID)
		writeJSON(w, http.StatusOK, map[string]any{"secret": metadataMap(record.Metadata(), time.Now())})
	}))

	mux.HandleFunc("DELETE /secrets/{name...}", auth(limitClassWrite, writePolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		name := r.PathValue("name")
		expected, ok := parseExpectedRevision(w, r, writeJSON)
		if !ok {
			return
		}
		if err := store.Delete(c.entry.Name, name, expected); err != nil {
			secretStoreError(w, writeJSON, "delete", c.entry.Name, name, err)
			return
		}
		slog.Info("local secret deleted", "service", c.entry.Name, "service_id", c.entry.ULID, "name", name)
		writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
	}))
}

// Admin routes are intentionally metadata-only on read. The admin can supply
// ciphertext for a service but cannot use this API to retrieve that ciphertext.
func mountAdminSecretRoutes(mux *http.ServeMux, store *secretstore.Store, writeJSON secretJSONWriter, adminOnly endpointAdminAuth) {
	mux.HandleFunc("GET /secrets/{owner}", adminOnly(limitClassScan, scanPolicy, func(w http.ResponseWriter, r *http.Request) {
		owner := r.PathValue("owner")
		pageSize, after, err := pageRequest(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "max must be between 1 and 10000"})
			return
		}
		items, next, err := store.ListPage(owner, after, pageSize)
		if err != nil {
			if errors.Is(err, secretstore.ErrInvalidPageToken) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid page token"})
				return
			}
			secretStoreError(w, writeJSON, "admin_list", owner, "", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"secrets": metadataWithExpiry(items, time.Now()), "next": next})
	}))

	mux.HandleFunc("GET /secrets/{owner}/{name...}", adminOnly(limitClassCheap, cheapPolicy, func(w http.ResponseWriter, r *http.Request) {
		owner, name := r.PathValue("owner"), r.PathValue("name")
		record, err := store.Get(owner, name)
		if err != nil {
			secretStoreError(w, writeJSON, "admin_get", owner, name, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"secret": metadataMap(record.Metadata(), time.Now())})
	}))

	mux.HandleFunc("PUT /secrets/{owner}/{name...}", adminOnly(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request) {
		in, ok := decodeSecretWrite(w, r, writeJSON)
		if !ok {
			return
		}
		owner, name := r.PathValue("owner"), r.PathValue("name")
		record, err := store.Put(owner, name, in.Envelope, in.ExpiresAt, in.ExpectedRevision, time.Now())
		if err != nil {
			secretStoreError(w, writeJSON, "admin_put", owner, name, err)
			return
		}
		slog.Info("secret provisioned", "owner", owner, "name", name, "revision", record.Revision, "key_id", record.Envelope.KeyID)
		writeJSON(w, http.StatusOK, map[string]any{"secret": metadataMap(record.Metadata(), time.Now())})
	}))

	mux.HandleFunc("DELETE /secrets/{owner}/{name...}", adminOnly(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request) {
		owner, name := r.PathValue("owner"), r.PathValue("name")
		expected, ok := parseExpectedRevision(w, r, writeJSON)
		if !ok {
			return
		}
		if err := store.Delete(owner, name, expected); err != nil {
			secretStoreError(w, writeJSON, "admin_delete", owner, name, err)
			return
		}
		slog.Info("secret deleted", "owner", owner, "name", name)
		writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
	}))
}

func decodeSecretWrite(w http.ResponseWriter, r *http.Request, writeJSON secretJSONWriter) (secretWrite, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSecretRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var in secretWrite
	if err := decoder.Decode(&in); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "secret envelope exceeds the request limit"})
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid secret envelope"})
		}
		return secretWrite{}, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "request must contain one JSON object"})
		return secretWrite{}, false
	}
	return in, true
}

func parseExpectedRevision(w http.ResponseWriter, r *http.Request, writeJSON secretJSONWriter) (*uint64, bool) {
	raw, present := r.URL.Query()["expected_revision"]
	if !present {
		return nil, true
	}
	if len(raw) != 1 || raw[0] == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "expected_revision must be one unsigned integer"})
		return nil, false
	}
	value, err := strconv.ParseUint(raw[0], 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "expected_revision must be one unsigned integer"})
		return nil, false
	}
	return &value, true
}

func secretStoreError(w http.ResponseWriter, writeJSON secretJSONWriter, operation, owner, name string, err error) {
	switch {
	case errors.Is(err, secretstore.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "secret not found"})
	case errors.Is(err, secretstore.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "secret revision conflict"})
	case errors.Is(err, secretstore.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
	default:
		slog.Error("secret store operation failed", "operation", operation, "owner", owner, "name", name, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "secret store operation failed"})
	}
}

func expired(at *time.Time, now time.Time) bool { return at != nil && !at.After(now) }

func metadataWithExpiry(items []secretstore.Metadata, now time.Time) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, metadataMap(item, now))
	}
	return out
}

func metadataMap(item secretstore.Metadata, now time.Time) map[string]any {
	out := map[string]any{
		"owner": item.Owner, "name": item.Name, "revision": item.Revision, "key_id": item.KeyID,
		"algorithm": item.Algorithm, "updated_at": item.UpdatedAt, "expired": expired(item.ExpiresAt, now),
	}
	if item.ExpiresAt != nil {
		out["expires_at"] = item.ExpiresAt
	}
	return out
}

// Package httpapi exposes the node's local control surface: publish, cursor
// fetch/ack, the KV projection, Prometheus metrics and a debug view. Every
// route except /healthz and /metrics requires the admin token from the config
// (header X-Colca-Token).
//
// Payloads travel as json.RawMessage in both directions: they are never decoded
// into map[string]any and re-encoded, so a number keeps the exact form the
// publisher sent it in.
package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// defaultMax / maxMax bound how many records one /fetch may return.
const (
	defaultMax = 100
	maxMax     = 1000
)

func Handler(e *engine.Engine, cfg *config.Config, m *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	// auth rejects everything when no token is configured: an empty token is a
	// missing secret, not a permission to skip authentication.
	auth := func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Colca-Token") != cfg.API.Token || cfg.API.Token == "" {
				writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid or missing X-Colca-Token"})
				return
			}
			next(w, r)
		}
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "ulid": cfg.ULID})
	})

	// /metrics is tokenless like /healthz: the API listener is loopback/
	// in-cluster, and Prometheus scrape targets do not carry admin tokens.
	if m != nil {
		mux.Handle("GET /metrics", m.Handler())
	}

	mux.HandleFunc("POST /publish", auth(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Topic   string          `json:"topic"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		res, err := e.IngestAdmin(in.Topic, in.Payload)
		if err != nil {
			// Grammar, unknown contract and payload validation are all
			// "well-formed request, unacceptable content" → 422.
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"stream": res.Stream, "offset": res.Offset, "topic": res.Topic})
	}))

	// GET /fetch reads from the cursor's current position and never advances it:
	// reading is side-effect free, /ack is the only thing that moves a cursor.
	//
	// Gap contract (spec §6.1): when the cursor's position is below the
	// stream's LWM the response gains a "gap" object and records begin at the
	// LWM (the pruned prefix is gone from disk, so Read starts there by
	// construction). The gap does NOT move the cursor — the consumer sees the
	// same gap on every fetch until it acks a record at or past the LWM. A
	// brand-new cursor (position 1) on a long-pruned stream gets the gap too:
	// a new consumer genuinely cannot see history. Unlike GET /downlink, next
	// is never bumped past the hole here — /fetch is side-effect free and
	// ack-driven, so a consumer with nothing readable past the LWM clears the
	// gap by acking gap.to_offset (which puts its cursor exactly at the LWM).
	mux.HandleFunc("GET /fetch", auth(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		stream, cursor := q.Get("stream"), q.Get("cursor")
		// An unknown stream is a malformed request, not an empty result — and
		// it has no LWM, so the gap machinery must never see it.
		if e.Store().NextOffset(stream) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unknown stream " + strconv.Quote(stream)})
			return
		}
		limit, _ := strconv.Atoi(q.Get("max"))
		if limit <= 0 || limit > maxMax {
			limit = defaultMax
		}
		prefix := q.Get("prefix")
		var filter func(string) bool
		if prefix != "" {
			// prefix filters on the uns hierarchy path, not on the raw topic.
			filter = func(topic string) bool {
				p, err := uns.Parse(topic)
				return err == nil && strings.HasPrefix(p.Path, prefix)
			}
		}
		from := e.Store().CursorGet(cursor, stream)
		recs, next, err := e.Store().Read(stream, from, limit, filter)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		out := make([]map[string]any, 0, len(recs))
		for _, rec := range recs {
			out = append(out, map[string]any{
				"offset":  rec.Offset,
				"topic":   rec.Topic,
				"payload": json.RawMessage(rec.Payload),
				"ts":      rec.TS,
			})
		}
		resp := map[string]any{"records": out, "next": next}
		if gap, ok := e.Store().Gap(stream, from); ok {
			resp["gap"] = gap
			// TODO: increment colca_gap_served_total{stream,surface="fetch"}.
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	// POST /ack: the client acks the last PROCESSED offset, the store holds the
	// next offset to read — hence offset+1.
	mux.HandleFunc("POST /ack", auth(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Cursor string `json:"cursor"`
			Stream string `json:"stream"`
			Offset uint64 `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		moved := e.Store().CursorAck(in.Cursor, in.Stream, in.Offset+1)
		writeJSON(w, http.StatusOK, map[string]any{"moved": moved})
	}))

	mux.HandleFunc("GET /kv", auth(func(w http.ResponseWriter, r *http.Request) {
		entries := e.Store().KVScan(r.URL.Query().Get("prefix"))
		out := make([]map[string]any, 0, len(entries))
		for _, en := range entries {
			out = append(out, map[string]any{
				"path":    en.Path,
				"node_id": en.NodeID,
				"topic":   en.Topic,
				"payload": json.RawMessage(en.Payload),
				"ts":      en.TS,
				"offset":  en.Offset,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{"entries": out})
	}))

	mux.HandleFunc("GET /debug/state", auth(func(w http.ResponseWriter, r *http.Request) {
		streams := map[string]any{}
		for _, st := range []string{"metrics", "entities", "commands"} {
			streams[st] = map[string]any{"next_offset": e.Store().NextOffset(st)}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ulid": cfg.ULID, "streams": streams})
	}))

	return mux
}

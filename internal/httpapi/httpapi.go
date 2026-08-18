// Package httpapi exposes the node's local control surface: publish, cursor
// fetch/ack, the KV projection, the enrollment door, Prometheus metrics and a
// debug view (auth design §4, §6.3).
//
// The listener speaks TLS with the node's own key (cert = key container,
// trust = pinning; ClientAuth requests but does not require a client cert).
// A caller is either a MACHINE (TLS client key resolved against the local
// registry — reads are grant-scoped, cursors are namespaced) or the ADMIN
// (X-Colca-Token — unscoped, and the only identity that may enroll/revoke).
// /healthz and /metrics stay open: scrapers and probes carry neither certs
// nor admin tokens.
//
// Payloads travel as json.RawMessage in both directions: they are never decoded
// into map[string]any and re-encoded, so a number keeps the exact form the
// publisher sent it in.
package httpapi

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// defaultMax / maxMax bound how many records one /fetch may return.
const (
	defaultMax = 100
	maxMax     = 1000
)

// TLSConfig builds the API listener's TLS config: the node's own key as
// server identity, client certs REQUESTED but not required — machine callers
// present their pinned key, admin tooling and scrapers stay certless.
func TLSConfig(id *identity.Identity, ulid string) (*tls.Config, error) {
	cert, err := id.SelfSignedCert(ulid)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequestClientCert,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// caller is the resolved identity of one request: exactly one of admin,
// machine (entry, human == nil) or human (entry + human) is set. For humans
// the entry IS human.Entry (KindHuman) — the read-scope and cursor-ownership
// code paths work identically for machines and humans through it.
type caller struct {
	admin bool
	entry *uns.Entry
	human *tokenauth.Verified
}

func Handler(e *engine.Engine, cfg *config.Config, reg *registry.Manager, ver *tokenauth.Verifier, m *metrics.Metrics) http.Handler {
	mux := http.NewServeMux()

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}

	// resolve identifies the caller (auth §6.3). A presented client cert MUST
	// resolve to an enrolled machine — an unknown cert never falls through to
	// token auth. The admin token rejects everything when unconfigured: an
	// empty token is a missing secret, not a permission to skip authentication.
	resolve := func(r *http.Request) (caller, bool) {
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			pub, err := identity.PeerPubHex(r.TLS.PeerCertificates[0].Raw)
			if err != nil {
				m.AuthReject(metrics.DoorHTTP, metrics.AuthUnknownKey)
				return caller{}, false
			}
			entry, ok := reg.ByPubkey(pub)
			if !ok {
				m.AuthReject(metrics.DoorHTTP, metrics.AuthUnknownKey)
				return caller{}, false
			}
			if entry.Kind != uns.KindMachine {
				m.AuthReject(metrics.DoorHTTP, metrics.AuthKind)
				return caller{}, false
			}
			return caller{entry: entry}, true
		}
		// Bearer = human (human-authz §5.3). A PRESENTED token that fails is
		// 401 — it never falls through to the admin token check.
		if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
			if ver == nil {
				m.AuthReject(metrics.DoorHTTP, tokenauth.ReasonBadToken)
				return caller{}, false // no auth: block → the human world does not exist here
			}
			v, reason, err := ver.Verify(strings.TrimPrefix(h, "Bearer "))
			if err != nil {
				m.AuthReject(metrics.DoorHTTP, reason)
				return caller{}, false
			}
			return caller{entry: v.Entry, human: v}, true
		}
		if cfg.API.Token != "" && r.Header.Get("X-Colca-Token") == cfg.API.Token {
			return caller{admin: true}, true
		}
		m.AuthReject(metrics.DoorHTTP, metrics.AuthToken)
		return caller{}, false
	}

	// auth admits machines and the admin; adminOnly admits only the admin.
	auth := func(next func(w http.ResponseWriter, r *http.Request, c caller)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			c, ok := resolve(r)
			if !ok {
				writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "no enrolled client key and no valid X-Colca-Token"})
				return
			}
			next(w, r, c)
		}
	}
	// adminOnly admits the static token AND humans carrying admin:# (§3):
	// same routes, two credentials — human admin actions are attributable
	// (sub in the log), token actions are not.
	adminOnly := func(next http.HandlerFunc) http.HandlerFunc {
		return auth(func(w http.ResponseWriter, r *http.Request, c caller) {
			if !c.admin && (c.human == nil || !c.human.Entry.IsAdmin()) {
				writeJSON(w, http.StatusForbidden, map[string]any{"error": "admin only (token or admin:# grant)"})
				return
			}
			if c.human != nil {
				slog.Default().Info("admin action by human", "sub", c.human.Sub,
					"username", c.human.Username, "path", r.URL.Path, "method", r.Method)
			}
			next(w, r)
		})
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "ulid": cfg.ULID})
	})

	// /metrics is certless/tokenless like /healthz: Prometheus scrape targets
	// carry no admin tokens (they must accept the self-signed server cert).
	if m != nil {
		mux.Handle("GET /metrics", m.Handler())
	}

	mux.HandleFunc("POST /publish", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
		var in struct {
			Topic   string          `json:"topic"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		var res engine.Result
		var err error
		switch {
		case c.admin:
			res, err = e.IngestAdmin(in.Topic, in.Payload)
		case c.human != nil:
			// Humans command and nothing else (§5.2) — IngestHuman enforces it.
			res, err = e.IngestHuman(c.entry, in.Topic, in.Payload)
		default:
			// A machine publishing over HTTP is judged exactly like its MQTT
			// publish: own zone, identity rule, cmd grants.
			res, err = e.IngestClient(c.entry.ULID, in.Topic, in.Payload)
		}
		if err != nil {
			// Grammar, unknown contract and payload validation are all
			// "well-formed request, unacceptable content" → 422.
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
			return
		}
		if !res.Persisted {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": "topic outside colca/# is not persisted"})
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
	//
	// A machine caller sees only records inside its read grants — filtered, not
	// erred: scope is a view, not a denial (the request itself is legitimate).
	// The gap object rides OUTSIDE that filter on purpose: pruning is
	// offset-based and stream-wide, so a cursor below the LWM has lost records
	// regardless of which topics its grants cover — a consumer must learn its
	// position is inside a hole even when every surviving record is filtered
	// out of its view. The gap carries only stream offsets, which the same
	// door already exposes to every authenticated caller via "next"; content
	// stays grant-gated. Unauthenticated callers never reach the gap logic.
	mux.HandleFunc("GET /fetch", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
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
		filter := func(topic string) bool {
			if prefix != "" {
				// prefix filters on the uns hierarchy path, not on the raw topic.
				p, err := uns.Parse(topic)
				if err != nil || !strings.HasPrefix(p.Path, prefix) {
					return false
				}
			}
			return c.admin || uns.Authorize(e.Scope(), c.entry, uns.ActReadRecord, topic)
		}
		if c.entry != nil && !ownsCursor(c.entry, cursor) {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "cursor not owned: machine cursors are named {ulid}/..."})
			return
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
			m.GapServed(stream, "fetch")
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	// POST /ack: the client acks the last PROCESSED offset, the store holds the
	// next offset to read — hence offset+1. Machine cursors are namespaced
	// {ulid}/... so one machine can never move another's cursor.
	mux.HandleFunc("POST /ack", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
		var in struct {
			Cursor string `json:"cursor"`
			Stream string `json:"stream"`
			Offset uint64 `json:"offset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if c.entry != nil && !ownsCursor(c.entry, in.Cursor) {
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "cursor not owned: machine cursors are named {ulid}/..."})
			return
		}
		moved := e.Store().CursorAck(in.Cursor, in.Stream, in.Offset+1)
		writeJSON(w, http.StatusOK, map[string]any{"moved": moved})
	}))

	mux.HandleFunc("GET /kv", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
		entries := e.Store().KVScan(r.URL.Query().Get("prefix"))
		out := make([]map[string]any, 0, len(entries))
		denied := 0
		for _, en := range entries {
			if !c.admin && !uns.Authorize(e.Scope(), c.entry, uns.ActReadRecord, en.Topic) {
				denied++
				continue
			}
			out = append(out, map[string]any{
				"path":    en.Path,
				"node_id": en.NodeID,
				"topic":   en.Topic,
				"payload": json.RawMessage(en.Payload),
				"ts":      en.TS,
				"offset":  en.Offset,
			})
		}
		if denied > 0 {
			m.ACLDeny(metrics.ACLRead)
		}
		writeJSON(w, http.StatusOK, map[string]any{"entries": out})
	}))

	// The enrollment door (auth §4): the ONLY write path for registry entries,
	// admin-guarded. Local-only — downward provisioning via a _CmdAdmin
	// flow is the intended direction.
	mux.HandleFunc("POST /enroll", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		ulid, off, err := reg.Enroll(body)
		if err != nil {
			code := http.StatusUnprocessableEntity
			if errors.Is(err, registry.ErrConflict) {
				code = http.StatusConflict
			}
			writeJSON(w, code, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ulid": ulid, "offset": off})
	}))

	mux.HandleFunc("DELETE /enroll/{ulid}", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		ulid := r.PathValue("ulid")
		// Move-drain design §3.1/§3.4: DELETE stays the immediate kill-switch
		// — no drain precondition ever creeps into registry.Revoke itself
		// (unchanged below). wasDraining is read by Revoke under its OWN
		// lock, atomically with the removal — no separate pre-check call, no
		// window for a concurrent POST .../drain to start and finish
		// unaccounted between a check and this revoke.
		off, wasDraining, err := reg.Revoke(ulid)
		if err != nil {
			if errors.Is(err, registry.ErrNotEnrolled) {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		if wasDraining {
			m.DrainCompleted(ulid, metrics.DrainOutcomeForced)
		}
		writeJSON(w, http.StatusOK, map[string]any{"revoked": true, "offset": off})
	}))

	// Move-drain design §3.1: a deliberate, admin-initiated decommission of a
	// kind=node child, distinct from the DELETE kill-switch above — the
	// child stays fully functional (connects, fetches /downlink, acks) while
	// its queue drains, and new commands addressed under its mount are
	// rejected at admission (engine draining check, reason "draining"). The
	// node auto-revokes through the same Revoke path once the completion
	// predicate holds (repl.Server.evaluateDrain), or immediately via DELETE
	// above (outcome "forced").
	mux.HandleFunc("POST /enroll/{ulid}/drain", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		ulid := r.PathValue("ulid")
		off, err := reg.Drain(ulid)
		if err != nil {
			code := http.StatusUnprocessableEntity
			switch {
			case errors.Is(err, registry.ErrNotEnrolled):
				code = http.StatusNotFound
			case errors.Is(err, registry.ErrNotNode), errors.Is(err, registry.ErrAlreadyDraining):
				code = http.StatusConflict
			}
			writeJSON(w, code, map[string]any{"error": err.Error()})
			return
		}
		// colca_drains_active is incremented by reg.Drain itself (registry
		// design: a state transition it fully understands owns its own
		// metric, the same way Enroll owns firing kick/deliver) — nothing to
		// do here.
		writeJSON(w, http.StatusOK, map[string]any{"ulid": ulid, "offset": off, "status": uns.StatusDraining})
	}))

	mux.HandleFunc("GET /enroll", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"entries": reg.List()})
	}))

	mux.HandleFunc("GET /debug/state", adminOnly(func(w http.ResponseWriter, r *http.Request) {
		streams := map[string]any{}
		for _, st := range []string{"metrics", "entities", "commands"} {
			streams[st] = map[string]any{"next_offset": e.Store().NextOffset(st)}
		}
		writeJSON(w, http.StatusOK, map[string]any{"ulid": cfg.ULID, "streams": streams})
	}))

	return mux
}

// ownsCursor: a machine's cursors live under its own ULID prefix.
func ownsCursor(e *uns.Entry, cursor string) bool {
	return strings.HasPrefix(cursor, e.ULID+"/")
}

func readBody(r *http.Request) ([]byte, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

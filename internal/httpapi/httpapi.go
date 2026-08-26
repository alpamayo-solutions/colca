// Package httpapi exposes the node's HTTP control surface: publish, cursor
// fetch/ack, the KV projection, the enrollment door, Prometheus metrics and a
// debug view (auth design §4, §6.3). Handler builds this over TWO listeners
// (local-service-trust design §4):
//
//   - The main door speaks TLS with the node's own key (cert = key container,
//     trust = pinning; ClientAuth requests but does not require a client
//     cert). A caller is either a MACHINE (TLS client key resolved against
//     the local registry — reads are grant-scoped, cursors are namespaced) or
//     the ADMIN (X-Colca-Token — unscoped, and the only identity that may
//     enroll/revoke).
//   - The local door (Handler's local=true) is plaintext and unpublished:
//     reachability from inside the deployment's own network IS the
//     credential, the caller names itself via X-Colca-Service, and the
//     admin routes are never mounted on it at all.
//
// /healthz and /metrics stay open on both: scrapers and probes carry neither
// certs nor admin tokens, and Prometheus is itself a local service.
//
// Payloads travel as json.RawMessage in both directions: they are never decoded
// into map[string]any and re-encoded, so a number keeps the exact form the
// publisher sent it in.
package httpapi

import (
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// defaultMax / maxMax bound how many records one /fetch may return.
const (
	defaultMax = 100
	maxMax     = 1000
)

// TLSConfig builds the API listener's TLS config: client certs REQUESTED but
// not required — machine callers present their pinned key, admin tooling and
// scrapers stay certless.
//
// The SERVER certificate is the supplied pair when the node configures one, and
// its own key container otherwise. Nothing pins this door's server certificate
// (clients here authenticate themselves, not the node), so it is one of the
// doors where a browser-trusted certificate is both possible and useful.
func TLSConfig(id *identity.Identity, ulid, certFile, keyFile string) (*tls.Config, error) {
	cert, err := identity.ServerCert(id, ulid, certFile, keyFile)
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

// Handler builds the node's HTTP surface. pubkey is this node's public key in
// hex — served on /healthz so a parent can enroll this node before trusting
// it, which is the only order enrollment can happen in.
//
// local selects the local API door (local-service-trust design §4): the
// listener is unreachable from outside the deployment, so reaching it IS the
// credential — there is nothing to authenticate, only a caller to name and
// register. The local door omits the admin routes entirely (reading and
// writing the node's own data is a local service's job; provisioning
// identities is not), so it never registers them at all — an admin route
// that 404s because it was never mounted, rather than 403s because it
// refused, is what keeps a scanner from learning the route exists.
func Handler(e *engine.Engine, cfg *config.Config, reg *registry.Manager, ver *tokenauth.Verifier, m *metrics.Metrics, blobs *blobstore.Store, pubkey string, local bool) http.Handler {
	mux := http.NewServeMux()

	// The body carries the payload base64-encoded inside a JSON envelope, so
	// it is legitimately larger than the record itself: 2x covers base64's
	// 4/3 inflation with room for the envelope's fields. Store.Append remains
	// the authority on the record; this only stops a huge body being read
	// into memory before that check can run.
	maxPublishBody := int64(cfg.Limits.EffectiveMaxRecordBytes())*2 + 4096

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	auditDenied := func(r *http.Request, door, reason string, entry *uns.Entry) {
		d := engine.AuditDenial{
			Operation: "authenticate", ReasonCode: reason,
			Metadata: map[string]any{"door": door, "route": r.URL.Path, "method": r.Method},
		}
		if entry != nil {
			d.ActorID, d.ActorLabel, d.ActorKind = entry.ULID, entry.Name, entry.ActorKind()
		}
		_ = e.RecordDenial(d)
	}

	var resolve func(r *http.Request) (caller, bool)
	if local {
		// The local door: the request arrived on a listener that cannot be
		// reached from outside the deployment, so there is nothing to
		// authenticate. The service names itself (X-Colca-Service) and is
		// registered on first sight, exactly as on the local MQTT door
		// (mqttsrv/local.go's authenticateLocal — kept in sync deliberately;
		// the two doors differing here would be exactly the second path
		// architecture principle 1 forbids).
		//
		// Two checks refuse a name that resolves to a KEYED identity (a
		// machine or child node), and both are needed — they catch it
		// through different paths:
		//
		//   - reg.Get(name): a name that collides with some OTHER entry's
		//     ULID. Register alone would not catch this — its own
		//     uniqueness check is scoped to the byName index, a different
		//     key space than byID — so presenting a machine's ULID as a
		//     "name" here would otherwise fall straight through to
		//     self-registration and silently mint an unrelated kind=local
		//     entry.
		//   - entry.MayUseDoor(uns.DoorLocal), checked AFTER Register
		//     returns: uns.Entry.Validate permits a `name` field on
		//     KindMachine and KindNode too, and Manager.Enroll indexes ANY
		//     non-empty Name into byName regardless of kind. So an operator
		//     who gives a machine or child node a friendly `name` makes it
		//     resolvable by ByName — and Register's own idempotent-reconnect
		//     path ("entry exists? return it, no kind check") would hand
		//     that keyed identity's ULID to a credential-free local request.
		//     This is the check that actually closes that hole: it asks the
		//     domain a question rather than enumerating the ways a name
		//     might resolve to something keyed, so it also subsumes the
		//     Get(name) case above and any future kind.
		resolve = func(r *http.Request) (caller, bool) {
			name := r.Header.Get("X-Colca-Service")
			if name == "" {
				m.AuthReject(metrics.DoorLocal, metrics.AuthNoName)
				auditDenied(r, metrics.DoorLocal, metrics.AuthNoName, nil)
				return caller{}, false
			}
			if known, ok := reg.Get(name); ok && !known.MayUseDoor(uns.DoorLocal) {
				m.AuthReject(metrics.DoorLocal, metrics.AuthKind)
				auditDenied(r, metrics.DoorLocal, metrics.AuthKind, known)
				return caller{}, false
			}
			entry, err := reg.Register(name, r.Header.Get("X-Colca-Mount"))
			if err != nil {
				m.AuthReject(metrics.DoorLocal, metrics.AuthRegister)
				auditDenied(r, metrics.DoorLocal, metrics.AuthRegister, nil)
				return caller{}, false
			}
			if !entry.MayUseDoor(uns.DoorLocal) {
				m.AuthReject(metrics.DoorLocal, metrics.AuthKind)
				auditDenied(r, metrics.DoorLocal, metrics.AuthKind, entry)
				return caller{}, false
			}
			return caller{entry: entry}, true
		}
	} else {
		// resolve identifies the caller (auth §6.3). A presented client cert
		// MUST resolve to an enrolled machine — an unknown cert never falls
		// through to token auth. The admin token rejects everything when
		// unconfigured: an empty token is a missing secret, not a permission
		// to skip authentication.
		resolve = func(r *http.Request) (caller, bool) {
			if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
				pub, err := identity.PeerPubHex(r.TLS.PeerCertificates[0].Raw)
				if err != nil {
					m.AuthReject(metrics.DoorHTTP, metrics.AuthUnknownKey)
					auditDenied(r, metrics.DoorHTTP, metrics.AuthUnknownKey, nil)
					return caller{}, false
				}
				entry, ok := reg.ByPubkey(pub)
				if !ok {
					m.AuthReject(metrics.DoorHTTP, metrics.AuthUnknownKey)
					auditDenied(r, metrics.DoorHTTP, metrics.AuthUnknownKey, nil)
					return caller{}, false
				}
				if !entry.MayUseDoor(uns.DoorHTTP) {
					m.AuthReject(metrics.DoorHTTP, metrics.AuthKind)
					auditDenied(r, metrics.DoorHTTP, metrics.AuthKind, entry)
					return caller{}, false
				}
				return caller{entry: entry}, true
			}
			// Bearer = human (human-authz §5.3). A PRESENTED token that fails
			// is 401 — it never falls through to the admin token check.
			if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
				if ver == nil {
					m.AuthReject(metrics.DoorHTTP, tokenauth.ReasonBadToken)
					auditDenied(r, metrics.DoorHTTP, tokenauth.ReasonBadToken, nil)
					return caller{}, false // no auth: block → the human world does not exist here
				}
				v, reason, err := ver.VerifyForScope(strings.TrimPrefix(h, "Bearer "), "broker-http")
				if err != nil {
					m.AuthReject(metrics.DoorHTTP, reason)
					auditDenied(r, metrics.DoorHTTP, reason, nil)
					return caller{}, false
				}
				return caller{entry: v.Entry, human: v}, true
			}
			// Constant-time compare: this is the unscoped admin credential, so a
			// byte-at-a-time short-circuit on == would let a network attacker
			// recover it one byte at a time via timing. ConstantTimeCompare
			// still returns 0 immediately on a length mismatch — that leaks
			// length, not content, and is accepted (design note above the
			// guard: an empty configured token must never authorize anyone,
			// which the != "" check ahead of the compare still guarantees).
			if cfg.API.Token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Colca-Token")), []byte(cfg.API.Token)) == 1 {
				return caller{admin: true}, true
			}
			m.AuthReject(metrics.DoorHTTP, metrics.AuthToken)
			auditDenied(r, metrics.DoorHTTP, metrics.AuthToken, nil)
			return caller{}, false
		}
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
				auditDenied(r, metrics.DoorHTTP, "admin_denied", c.entry)
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
		// The ULID and pubkey are what `colca node enroll` reads. Enrollment
		// happens BEFORE this node is trusted by anything, so both have to be
		// readable at the one unauthenticated door.
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "ulid": cfg.ULID, "pubkey": pubkey})
	})

	// /metrics is certless/tokenless like /healthz: Prometheus scrape targets
	// carry no admin tokens (they must accept the self-signed server cert).
	if m != nil {
		mux.Handle("GET /metrics", m.Handler())
	}

	mux.HandleFunc("POST /publish", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
		r.Body = http.MaxBytesReader(w, r.Body, maxPublishBody)
		var in struct {
			Topic      string          `json:"topic"`
			Payload    json.RawMessage `json:"payload"`
			WrittenBy  string          `json:"written_by"`
			ActorID    string          `json:"actor_id"`
			ActorLabel string          `json:"actor_label"`
			ActorKind  string          `json:"actor_kind"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				m.RecordRejected("too_large")
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
					"error": fmt.Sprintf("request body exceeds %d bytes", maxPublishBody)})
				return
			}
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		var res engine.Result
		var err error
		switch {
		case c.admin:
			writtenBy := in.WrittenBy
			if writtenBy == "" {
				writtenBy = r.Header.Get("X-Colca-Service")
			}
			if writtenBy == "" {
				writtenBy = "admin"
			}
			actorID := in.ActorID
			actorLabel := in.ActorLabel
			actorKind := in.ActorKind
			if actorID == "" {
				actorID = writtenBy
			}
			if actorLabel == "" {
				actorLabel = actorID
			}
			if actorKind == "" {
				actorKind = "system"
			}
			res, err = e.IngestAdminAttributed(in.Topic, in.Payload, engine.Attribution{
				WrittenBy: writtenBy, ActorID: actorID,
				ActorLabel: actorLabel, ActorKind: actorKind,
			})
		case c.human != nil:
			// Humans command and nothing else (§5.2) — IngestHuman enforces it.
			actor := c.human.Username
			if actor == "" {
				actor = c.human.Sub
			}
			res, err = e.IngestHumanAttributed(c.entry, actor, in.Topic, in.Payload)
		default:
			// A machine publishing over HTTP is judged exactly like its MQTT
			// publish: own zone, identity rule, cmd grants.
			if local && in.ActorID != "" {
				res, err = e.IngestLocalAttributed(c.entry.ULID, in.Topic, in.Payload, engine.Attribution{
					ActorID: in.ActorID, ActorLabel: in.ActorLabel, ActorKind: in.ActorKind,
				})
			} else {
				res, err = e.IngestClient(c.entry.ULID, in.Topic, in.Payload)
			}
		}
		if err != nil {
			if errors.Is(err, store.ErrRecordTooLarge) {
				// A DIFFERENT case from the MaxBytesReader arm above: that one
				// trips on the raw HTTP body before JSON decode ever
				// completes (a `return` inside the decode-error branch, so
				// this code is unreachable for it — no double count), and
				// counts m.RecordRejected itself since only this door sees
				// that failure. This one is a record whose payload still fit
				// inside the JSON envelope (§5 caps the record, not just the
				// wire request) but is too large once decoded — Store.Append
				// is what discovers that, so persistTSAttributed/
				// ingestAdminStateBatch (engine.go) are where it is counted:
				// the same call sites MQTT ingest goes through, so counting
				// there covers both doors from one place.
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": err.Error()})
				return
			}
			// Grammar, unknown contract and payload validation are all
			// "well-formed request, unacceptable content" → 422.
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
			return
		}
		if !res.Persisted {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": "topic outside colca/# is not persisted"})
			return
		}
		body := map[string]any{"stream": res.Stream, "offset": res.Offset, "topic": res.Topic}
		if res.Command != nil {
			body["command"] = res.Command
		}
		writeJSON(w, http.StatusOK, body)
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
		signalIDs, hasSignalFilter := q["signal_id"]
		signalSet := make(map[string]struct{}, len(signalIDs))
		if hasSignalFilter {
			if stream != "metrics" {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "signal_id is valid only for the metrics stream"})
				return
			}
			for _, signalID := range signalIDs {
				if signalID == "" {
					writeJSON(w, http.StatusBadRequest, map[string]any{"error": "signal_id must not be empty"})
					return
				}
				signalSet[signalID] = struct{}{}
			}
			if len(signalSet) > 1000 {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "at most 1000 unique signal_id filters are allowed"})
				return
			}
		}
		filter := func(record store.StoredRecord) bool {
			topic := record.Topic
			var parsed uns.Parsed
			var parseErr error
			if prefix != "" {
				// prefix filters on the uns hierarchy path, not on the raw topic.
				parsed, parseErr = uns.Parse(topic)
				if parseErr != nil || !strings.HasPrefix(parsed.Path, prefix) {
					return false
				}
			}
			if !c.admin && !uns.Authorize(e.Scope(), c.entry, uns.ActReadRecord, topic) {
				return false
			}
			if !hasSignalFilter {
				return true
			}
			if parseErr != nil || parsed.Contract == "" {
				parsed, parseErr = uns.Parse(topic)
			}
			if parseErr != nil || parsed.Contract != "_Metric" {
				return false
			}
			var metric struct {
				SignalID string `json:"signal_id"`
			}
			if json.Unmarshal(record.Payload, &metric) != nil {
				return false
			}
			_, wanted := signalSet[metric.SignalID]
			return wanted
		}
		if c.entry != nil && !ownsCursor(c.entry, cursor) {
			_ = e.RecordDenial(engine.AuditDenial{Operation: "read", ReasonCode: "cursor_denied",
				ActorID: c.entry.ULID, ActorLabel: c.entry.Name, ActorKind: c.entry.ActorKind(),
				Metadata: map[string]any{"door": metrics.DoorHTTP, "route": r.URL.Path, "stream": stream, "cursor": cursor}})
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "cursor not owned: this identity's cursors are named " + c.entry.CursorPrefix() + "..."})
			return
		}
		from := e.Store().CursorGet(cursor, stream)
		recs, next, err := e.Store().ReadRecords(stream, from, limit, filter)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		out := make([]map[string]any, 0, len(recs))
		for _, rec := range recs {
			out = append(out, map[string]any{
				"offset":        rec.Offset,
				"origin_offset": rec.OriginOffset,
				"topic":         rec.Topic,
				"payload":       json.RawMessage(rec.Payload),
				"ts":            rec.TS,
				"written_by":    rec.WrittenBy,
				"actor_id":      rec.ActorID,
				"actor_label":   rec.ActorLabel,
				"actor_kind":    rec.ActorKind,
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
	// next offset to read — hence offset+1. Cursors are namespaced by
	// uns.Entry.CursorPrefix ({ulid}/... for a machine or human, c/{name}/...
	// for a local service) so one identity can never move another's cursor.
	//
	// "delete": true retires the cursor instead of moving it (mutually
	// exclusive with "offset" — when set, offset is ignored and no ack
	// happens). This is what lets a consumer that mints a fresh cursor name
	// on every rebuild (the dataops evaluator's "generation" cursors: colca
	// cursors only move forward, so re-reading a stream needs a new name)
	// retire the one it is replacing, instead of leaving it to linger forever
	// and hold back retention pruning. Same ownership guard as the ack path:
	// an identity may only delete cursors under its own prefix.
	mux.HandleFunc("POST /ack", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
		var in struct {
			Cursor string `json:"cursor"`
			Stream string `json:"stream"`
			Offset uint64 `json:"offset"`
			Delete bool   `json:"delete"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		op := "ack"
		if in.Delete {
			op = "cursor_delete"
		}
		if c.entry != nil && !ownsCursor(c.entry, in.Cursor) {
			_ = e.RecordDenial(engine.AuditDenial{Operation: op, ReasonCode: "cursor_denied",
				ActorID: c.entry.ULID, ActorLabel: c.entry.Name, ActorKind: c.entry.ActorKind(),
				Metadata: map[string]any{"door": metrics.DoorHTTP, "route": r.URL.Path, "stream": in.Stream, "cursor": in.Cursor}})
			writeJSON(w, http.StatusForbidden, map[string]any{"error": "cursor not owned: this identity's cursors are named " + c.entry.CursorPrefix() + "..."})
			return
		}
		if in.Delete {
			if err := e.Store().CursorDelete(in.Cursor, in.Stream); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
			return
		}
		moved := e.Store().CursorAck(in.Cursor, in.Stream, in.Offset+1)
		writeJSON(w, http.StatusOK, map[string]any{"moved": moved})
	}))

	mux.HandleFunc("GET /kv", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
		entries, err := e.Store().KVScan(r.URL.Query().Get("prefix"))
		if err != nil {
			// A storage-layer failure, not "no entries" — answering 200 with
			// an empty list here would tell a caller a prefix holds nothing
			// when the truth is the scan itself never completed. The response
			// body stays static (no err.Error()) so a storage-layer detail
			// such as a filesystem path never reaches the caller; the real
			// error still reaches operators through the log.
			slog.Default().Error("kv scan failed", "prefix", r.URL.Query().Get("prefix"), "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "scan failed"})
			return
		}
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

	// GET /self is the local service's bootstrap view of the registry entry
	// the door resolved for this request. It is deliberately local-only: a
	// service needs the ULID minted during self-registration and its CURRENT
	// mount in order to publish an identity-bearing catalogue at the right
	// topic. Neither fact is metadata the service may guess from visible KV
	// records. The entry remains the single source of truth, so an operator's
	// reparent is reflected immediately and a stale X-Colca-Mount declaration
	// never moves it back.
	if local {
		mountBlobRoutes(mux, blobs, m, cfg.Limits.EffectiveMaxBlobBytes(), writeJSON, auth)

		mux.HandleFunc("GET /self", auth(func(w http.ResponseWriter, r *http.Request, c caller) {
			mount := ""
			if c.entry.Element != "" {
				var ok bool
				mount, ok = e.Elements().PathOf(c.entry.Element)
				if !ok {
					// A bound entry whose element is absent from this node's
					// namespace is inconsistent state. Never collapse it into
					// the unplaced position: that would silently widen scope.
					writeJSON(w, http.StatusConflict, map[string]any{
						"error": "the local service's bound element is not present in this node's namespace",
					})
					return
				}
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"ulid":    c.entry.ULID,
				"name":    c.entry.Name,
				"node":    e.NodeID(),
				"element": c.entry.Element,
				"mount":   mount,
				// The upload caps a local service must respect. One owner
				// for the number: a caller reads it here instead of holding
				// its own copy of cfg.Limits (resources design §5).
				"limits": map[string]any{
					"max_record_bytes": cfg.Limits.EffectiveMaxRecordBytes(),
					"max_blob_bytes":   cfg.Limits.EffectiveMaxBlobBytes(),
				},
			})
		}))
	}

	// Admin routes: never mounted on the local door at all (design §4) — a
	// route that was never registered 404s, which tells a caller nothing;
	// a route that exists and refuses would 403, which confirms it exists.
	// Provisioning identities is not a local service's job; reading and
	// writing the node's own data is.
	if !local {
		// The published door's resource read (resources design §6): the ONLY
		// way a file is read on an authenticated door. Every read passes
		// through a resource id so the element-scoped grant check always
		// runs — raw digest access stays on the local and replication doors,
		// where the door itself is the authorization.
		mountResourceRoutes(mux, e, blobs, m, writeJSON, auth)

		// The enrollment door (auth §4): the ONLY write path for registry
		// entries, admin-guarded. Local-only — downward provisioning
		// via a _CmdAdmin flow is the intended direction.
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
			// Derived, not listed: this route is what the test harnesses read
			// to learn a node's stream set, so a copy here would let their
			// coverage drift from what the store actually holds.
			for _, st := range store.Streams() {
				streams[st] = map[string]any{"next_offset": e.Store().NextOffset(st)}
			}
			writeJSON(w, http.StatusOK, map[string]any{"ulid": cfg.ULID, "streams": streams})
		}))
	}

	return mux
}

// ownsCursor: whether the caller may move this cursor itself. The whole rule
// is a domain question, so it is answered in one place (uns.Entry.OwnsCursor):
// the caller's own prefix — ULID-prefixed for every keyed identity,
// name-prefixed for a local service, which never learns the ULID Register
// minted for it (local-service-trust design §4) — minus the cursors the node
// alone may write, which today means the command delivery floor.
//
// The predicate is nil-safe, so a future caller that forgets today's
// `c.entry != nil && !ownsCursor(...)` guard gets "owns nothing" rather than a
// panic, which is the correct answer anyway.
func ownsCursor(e *uns.Entry, cursor string) bool {
	return e.OwnsCursor(cursor)
}

func readBody(r *http.Request) ([]byte, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

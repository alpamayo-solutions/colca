// Package httpapi is the node's HTTP surface: publish, cursor fetch and ack, the
// KV projection, enrollment, Prometheus metrics and a debug view. Handler serves
// two listeners:
//
//   - The main door speaks TLS with the node's own key and requests, but does not
//     require, a client certificate. A caller is a machine (its key resolved
//     against the registry, reads scoped by grants), a human with a bearer token,
//     or the admin (X-Colca-Token, unscoped, the only one who may enroll).
//   - The local door is plaintext and never published: reaching it is the
//     credential, the caller names itself with X-Colca-Service, and the admin
//     routes do not exist there.
//
// /healthz and /metrics are open on both. Payloads travel as json.RawMessage both
// ways, so numbers keep the exact form the publisher sent.
package httpapi

import (
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/secretstore"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// rawPayload wraps a stored payload for the wire. An empty payload is a tombstone
// and must go out as JSON null: a zero-length json.RawMessage fails to encode,
// which would break the whole response.
func rawPayload(payload []byte) json.RawMessage {
	if len(payload) == 0 {
		return json.RawMessage("null")
	}
	return json.RawMessage(payload)
}

// maxLogoutBody bounds a back-channel logout request: one signed JWT in a form.
const maxLogoutBody = 64 << 10

// defaultMax / maxMax bound how many records one /fetch may return.
const (
	defaultMax = 100
	maxMax     = 1000
)

// TLSConfig builds the API listener's TLS config. Client certificates are
// requested but not required, so machines present their pinned key while admin
// tooling and scrapers need none. The server certificate is the configured pair
// if there is one, else the node's key container; nothing pins this door's
// certificate, so a browser-trusted one works.
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

// caller is the resolved identity of one request: exactly one of admin, machine
// (entry) or human (entry and human) is set. For humans the entry is
// human.Entry, so read scopes and cursor ownership work the same for both.
type caller struct {
	admin bool
	entry *uns.Entry
	human *tokenauth.Verified
}

// Handler builds the node's HTTP surface. pubkey is the node's public key in hex,
// served on /healthz so a parent can enroll the node before trusting it.
//
// local selects the local door. Reaching it is the credential, so there is
// nothing to authenticate, only a caller to name and register. The admin routes
// are not mounted there at all, so a scanner gets 404 rather than learning they
// exist.
//
// uplink is the node's replication client toward its parent, nil without one.
// /healthz reports its status, so chaski.Node or an operator can tell "not
// enrolled yet" from "connected".
func Handler(e *engine.Engine, cfg *config.Config, reg *registry.Manager, ver *tokenauth.Verifier, m *metrics.Metrics, blobs *blobstore.Store, pubkey string, local bool, uplink *repl.Client, secretStores ...*secretstore.Store) http.Handler {
	mux := http.NewServeMux()
	var secretDB *secretstore.Store
	if len(secretStores) > 0 {
		secretDB = secretStores[0]
	}

	// The payload is base64 inside a JSON envelope, so the body may be about twice the
	// record cap. Store.Append still enforces the record cap; this only keeps a huge
	// body out of memory.
	maxPublishBody := int64(cfg.Limits.EffectiveMaxRecordBytes())*2 + 4096 //nolint:gosec // config caps max_record_bytes at 1 GiB

	// writeJSON encodes into a buffer before touching the ResponseWriter, so an
	// encoding failure becomes a 500 instead of a 200 with an empty body.
	writeJSON := func(w http.ResponseWriter, code int, v any) {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(v); err != nil {
			slog.Default().Error("response failed to encode as JSON — refusing to send a 200 with no body", "err", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"response encoding failed"}` + "\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_, _ = w.Write(buf.Bytes())
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
		// On the local door the listener is only reachable inside the deployment, so
		// there is nothing to authenticate. The service names itself with
		// X-Colca-Service and is registered on first sight, as on the local MQTT door
		// (authenticateLocal); the two must stay in sync.
		//
		// A name must never hand out a keyed identity (a machine or child node).
		// reg.Get(name) catches a name that is another entry's ULID. MayUseDoor(DoorLocal)
		// after Register catches a machine that was given a friendly name, which Register
		// would otherwise return as is.
		resolve = func(r *http.Request) (caller, bool) {
			// A bearer token is a person, forwarded by a local service acting for them, so
			// the executor authorizes the person. A presented token that fails is 401 and
			// never falls back to the service identity.
			if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
				if ver == nil {
					m.AuthReject(metrics.DoorLocal, tokenauth.ReasonBadToken)
					auditDenied(r, metrics.DoorLocal, tokenauth.ReasonBadToken, nil)
					return caller{}, false // no verifier: the human world does not exist here
				}
				v, reason, err := ver.VerifyForScope(strings.TrimPrefix(h, "Bearer "), uns.ScopeAPI)
				if err != nil {
					m.AuthReject(metrics.DoorLocal, reason)
					auditDenied(r, metrics.DoorLocal, reason, nil)
					return caller{}, false
				}
				return caller{entry: v.Entry, human: v}, true
			}
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
		// resolve identifies the caller. A presented client certificate must resolve to
		// an enrolled machine and never falls through to token auth. An unconfigured admin
		// token rejects everything.
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
			// Bearer means a human. A presented token that fails is 401 and never falls
			// through to the admin token.
			if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
				if ver == nil {
					m.AuthReject(metrics.DoorHTTP, tokenauth.ReasonBadToken)
					auditDenied(r, metrics.DoorHTTP, tokenauth.ReasonBadToken, nil)
					return caller{}, false // no auth: block → the human world does not exist here
				}
				v, reason, err := ver.VerifyForScope(strings.TrimPrefix(h, "Bearer "), uns.ScopeBrokerHTTP)
				if err != nil {
					m.AuthReject(metrics.DoorHTTP, reason)
					auditDenied(r, metrics.DoorHTTP, reason, nil)
					return caller{}, false
				}
				return caller{entry: v.Entry, human: v}, true
			}
			// The admin token is compared in constant time so it cannot be recovered byte by
			// byte through timing. A length mismatch still returns early, which leaks only
			// the length. An empty configured token never authorizes; the != "" check
			// guarantees that.
			if cfg.API.Token != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Colca-Token")), []byte(cfg.API.Token)) == 1 {
				return caller{admin: true}, true
			}
			m.AuthReject(metrics.DoorHTTP, metrics.AuthToken)
			auditDenied(r, metrics.DoorHTTP, metrics.AuthToken, nil)
			return caller{}, false
		}
	}

	requestLimiter := httplimit.New()
	door := metrics.DoorHTTP
	if local {
		door = metrics.DoorLocal
	}

	// authFor first bounds unauthenticated work by source address, then applies
	// the endpoint policy to the resolved identity. On the deliberately trusted
	// local door the source container address remains the enforcement key, so a
	// caller cannot escape a bucket merely by changing X-Colca-Service.
	authFor := func(class string, policy httplimit.Policy, next func(w http.ResponseWriter, r *http.Request, c caller)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			preauthRelease, ok := acquireRequest(w, r, requestLimiter, m, door, limitClassAuth, sourceLimitKey(r), authPolicy)
			if !ok {
				return
			}
			defer preauthRelease()
			c, ok := resolve(r)
			if !ok {
				writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "no enrolled client key and no valid X-Colca-Token"})
				return
			}
			key := callerLimitKey(c)
			if local {
				key = sourceLimitKey(r)
			}
			release, ok := acquireRequest(w, r, requestLimiter, m, door, class, key, policy)
			if !ok {
				m.HTTPLimitedCaller(r.Pattern, callerLabel(c))
				return
			}
			m.HTTPCallerSeen(r.Pattern, callerLabel(c))
			defer release()
			next(w, r, c)
		}
	}
	// adminFor admits the static token and humans holding admin:#. Human admin actions
	// are attributable, token actions are not.
	adminFor := func(class string, policy httplimit.Policy, next http.HandlerFunc) http.HandlerFunc {
		return authFor(class, policy, func(w http.ResponseWriter, r *http.Request, c caller) {
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
		release, ok := acquireRequest(w, r, requestLimiter, m, door, limitClassHealth, sourceLimitKey(r), healthPolicy)
		if !ok {
			return
		}
		defer release()
		// Enrolling this node at a parent needs its ULID and pubkey before anything trusts
		// it, so both are readable at this unauthenticated door.
		payload := map[string]any{"ok": true, "ulid": cfg.ULID, "pubkey": pubkey}
		// uplink lets chaski.Node or an operator tell "not enrolled yet" from "connected"
		// without reading logs. "none" is an answer: a root has no uplink.
		switch {
		case cfg.Parent == nil:
			payload["uplink"] = map[string]any{"state": "none"}
		case uplink != nil:
			st := uplink.Status()
			payload["uplink"] = map[string]any{
				"state": string(st.State),
				"since": st.Since.Format(time.RFC3339),
			}
		}
		// storage stays 200 like uplink: restarting does not free a full disk, the
		// operator has to, and the state tells them to look.
		payload["storage"] = e.Store().Health()
		writeJSON(w, http.StatusOK, payload)
	})

	// Back-channel logout (OpenID Connect Back-Channel Logout 1.0). The identity
	// provider calls it server to server when a login ends; the signed logout token is
	// the credential, so it is open on both doors. Every session and token of the
	// named login stops working here at once.
	if ver != nil {
		mux.HandleFunc("POST /auth/backchannel-logout", func(w http.ResponseWriter, r *http.Request) {
			release, ok := acquireRequest(w, r, requestLimiter, m, door, limitClassAuth, sourceLimitKey(r), authPolicy)
			if !ok {
				return
			}
			defer release()
			// The specification forbids caching either answer.
			w.Header().Set("Cache-Control", "no-store")
			refuse := func(reason string) {
				m.AuthReject(door, tokenauth.ReasonBadToken)
				auditDenied(r, door, "logout_token_invalid", nil)
				slog.Default().Warn("back-channel logout refused", "reason", reason)
				writeJSON(w, http.StatusBadRequest, map[string]any{
					"error": "invalid_request", "error_description": reason,
				})
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxLogoutBody)
			if err := r.ParseForm(); err != nil {
				refuse("the body is not a form: " + err.Error())
				return
			}
			token := r.PostForm.Get("logout_token")
			if token == "" {
				refuse("no logout_token")
				return
			}
			if _, err := ver.BackchannelLogout(token); err != nil {
				refuse(err.Error())
				return
			}
			w.WriteHeader(http.StatusOK)
		})
	}

	// /metrics is certless/tokenless like /healthz: Prometheus scrape targets
	// carry no admin tokens (they must accept the self-signed server cert).
	if m != nil {
		metricsHandler := m.Handler()
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
			release, ok := acquireRequest(w, r, requestLimiter, m, door, limitClassMetrics, "global", metricsPolicy)
			if !ok {
				return
			}
			defer release()
			metricsHandler.ServeHTTP(w, r)
		})
	}

	mux.HandleFunc("POST /publish", authFor(limitClassWrite, writePolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		r.Body = http.MaxBytesReader(w, r.Body, maxPublishBody)
		var in struct {
			Topic      string          `json:"topic"`
			Payload    json.RawMessage `json:"payload"`
			WrittenBy  string          `json:"written_by"`
			ActorID    string          `json:"actor_id"`
			ActorLabel string          `json:"actor_label"`
			ActorKind  string          `json:"actor_kind"`
			// ActorGroups and FallbackReason: a local service that cannot forward a person's
			// token, such as a background job, attests their group ids instead. The node
			// resolves them against its own _Group definitions, which narrows the service to
			// the person's grants, and logs every use with the stated reason.
			ActorGroups    []string `json:"actor_groups"`
			FallbackReason string   `json:"fallback_reason"`
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
			// Humans only send commands; IngestHuman enforces it.
			actor := c.human.Username
			if actor == "" {
				actor = c.human.Sub
			}
			res, err = e.IngestHumanAttributed(c.entry, actor, in.Topic, in.Payload)
		default:
			// A machine publishing over HTTP is judged exactly like its MQTT
			// publish: own zone, identity rule, cmd grants.
			if local && in.ActorID != "" {
				if len(in.ActorGroups) > 0 {
					if in.FallbackReason == "" {
						writeJSON(w, http.StatusBadRequest, map[string]any{
							"error": "actor_groups attests a person without their token: state fallback_reason, or forward their Bearer instead"})
						return
					}
					slog.Default().Warn("publish: local service attests a person's groups instead of forwarding their token",
						"service", c.entry.Name, "actor", in.ActorID, "groups", len(in.ActorGroups),
						"fallback_reason", in.FallbackReason, "topic", in.Topic)
				}
				res, err = e.IngestLocalAttributed(c.entry.ULID, in.Topic, in.Payload, engine.Attribution{
					ActorID: in.ActorID, ActorLabel: in.ActorLabel, ActorKind: in.ActorKind,
					ActorGroups: in.ActorGroups,
				})
			} else {
				res, err = e.IngestClient(c.entry.ULID, in.Topic, in.Payload)
			}
		}
		if err != nil {
			if errors.Is(err, store.ErrRecordTooLarge) {
				// A record that fit the JSON envelope but exceeds the record cap once decoded.
				// Store.Append finds it and the engine counts it, for MQTT and HTTP alike; the
				// raw-body limit above returns early, so nothing is counted twice.
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": err.Error()})
				return
			}
			// An authorization denial is a well-formed request refused, 403, like the other
			// ownership checks on this door. Grammar, unknown contracts and validation
			// failures are unacceptable content, 422.
			var re *engine.RejectError
			if errors.Is(err, engine.ErrDenied) {
				errors.As(err, &re)
				writeJSON(w, http.StatusForbidden, map[string]any{"error": err.Error(), "reason": re.Reason})
				return
			}
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": err.Error()})
			return
		}
		if res.Duplicate {
			// A command this sender already sent: not stored or run again.
			writeJSON(w, http.StatusOK, map[string]any{"duplicate": true})
			return
		}
		if res.Answered {
			// No service executes this command; the _Ack says so too.
			writeJSON(w, http.StatusNotFound, map[string]any{"error": res.Command.Message, "command": res.Command})
			return
		}
		if !res.Persisted {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": "topic outside " + uns.Root() + "/# is not persisted"})
			return
		}
		body := map[string]any{"stream": res.Stream, "offset": res.Offset, "topic": res.Topic}
		if res.Command != nil {
			body["command"] = res.Command
		}
		writeJSON(w, http.StatusOK, body)
	}))

	// GET /fetch reads from the cursor's position and never moves it; only /ack does.
	//
	// If the cursor is below the stream's LWM the response carries a gap object and
	// records start at the LWM. The consumer sees the same gap until it acks at or
	// past the LWM (gap.to_offset). A new cursor on a pruned stream gets the gap too.
	//
	// Records are filtered by the caller's read grants, but the gap is not: pruning
	// is stream-wide, so a consumer must learn it is in a hole even if every surviving
	// record is outside its view. The gap only carries offsets, which "next" already
	// exposes.
	// GET /watch replaces polling /fetch on an idle stream: one held connection
	// that names the selected streams whenever they grow (see serveWatch).
	mux.HandleFunc("GET /watch", authFor(limitClassWatch, watchPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		serveWatch(w, r, e.Store(), writeJSON)
	}))

	mux.HandleFunc("GET /fetch", authFor(limitClassFetch, fetchPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		q := r.URL.Query()
		stream, cursor := q.Get("stream"), q.Get("cursor")
		// An unknown stream is a bad request, not an empty result, and has no LWM for the
		// gap logic.
		if e.Store().NextOffset(stream) == 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "unknown stream " + strconv.Quote(stream)})
			return
		}
		limit, _ := strconv.Atoi(q.Get("max"))
		if limit <= 0 || limit > maxMax {
			limit = defaultMax
		}
		prefix := q.Get("prefix")
		// contract narrows the page to the given contracts (repeatable), like /kv's.
		// Records of other contracts are skipped in the scan and next moves past them.
		var contractSet map[string]bool
		if contracts := q["contract"]; len(contracts) > 0 {
			contractSet = make(map[string]bool, len(contracts))
			for _, ct := range contracts {
				if !uns.IsKnown(e.ClassOf(ct)) {
					writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("unknown contract: %q", ct)})
					return
				}
				contractSet[ct] = true
			}
		}
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
			if stream == uns.StreamFor(uns.ClassCmd) && e.CommandRetired(record) {
				return false
			}
			topic := record.Topic
			var parsed uns.Parsed
			var parseErr error
			if contractSet != nil {
				parsed, parseErr = uns.Parse(topic)
				if parseErr != nil || !contractSet[parsed.Contract] {
					return false
				}
			}
			if prefix != "" && parsed.Contract == "" {
				// prefix filters on the uns hierarchy path, not on the raw topic.
				parsed, parseErr = uns.Parse(topic)
			}
			if prefix != "" && (parseErr != nil || !strings.HasPrefix(parsed.Path, prefix)) {
				return false
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
		// from=N reads ahead of the cursor: a consumer that processed up to N-1 but
		// has not acked yet fetches its next page without waiting for the ack. It
		// never reads behind the cursor, and the cursor still moves only on /ack.
		if v := q.Get("from"); v != "" {
			ahead, err := strconv.ParseUint(v, 10, 64)
			if err != nil || ahead == 0 {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "from must be a positive offset"})
				return
			}
			from = max(from, ahead)
		}
		// tail=1 reads the end of the stream instead of from the cursor. A viewer asks what
		// happened most recently, which a forward read from an unacked cursor cannot
		// answer. The cursor does not move, so a viewer and a consumer can share a cursor
		// name.
		if q.Get("tail") != "" {
			head := e.Store().NextOffset(stream)
			if head > uint64(limit) {
				from = head - uint64(limit)
			} else {
				from = 1
			}
		}
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
				"payload":       rawPayload(rec.Payload),
				"ts":            rec.TS,
				"written_by":    rec.WrittenBy,
				"actor_id":      rec.ActorID,
				"actor_label":   rec.ActorLabel,
				"actor_kind":    rec.ActorKind,
			})
		}
		m.HTTPFetch(callerLabel(c), stream)
		// "from" is where this page started, so a client can tell a node that read
		// ahead from one that ignored the parameter.
		resp := map[string]any{"records": out, "next": next, "from": from, "now_ms": e.AuthoritativeNow().UnixMilli()}
		if gap, ok := e.Store().Gap(stream, from); ok {
			resp["gap"] = gap
			m.GapServed(stream, "fetch")
		}
		writeJSON(w, http.StatusOK, resp)
	}))

	// POST /ack acks the last processed offset; the store keeps the next offset to
	// read, hence offset+1. Cursors are namespaced by uns.Entry.CursorPrefix, so no
	// identity can move another's cursor.
	//
	// "delete": true retires the cursor instead and ignores offset. A consumer that
	// mints a new cursor name per rebuild uses it to drop the old one, which would
	// otherwise hold back retention. The same ownership rule applies.
	mux.HandleFunc("POST /ack", authFor(limitClassWrite, writePolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		r.Body = http.MaxBytesReader(w, r.Body, maxAckBodyBytes)
		var in struct {
			Cursor string `json:"cursor"`
			Stream string `json:"stream"`
			Offset uint64 `json:"offset"`
			Delete bool   `json:"delete"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "ack request exceeds 16 KiB"})
				return
			}
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

	mux.HandleFunc("GET /kv", authFor(limitClassScan, scanPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
		pageSize, after, err := pageRequest(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "max must be between 1 and 10000"})
			return
		}
		prefix := r.URL.Query().Get("prefix")
		// contract narrows the page to the given contracts (repeatable). Unknown names are
		// rejected rather than silently matching nothing.
		contracts := r.URL.Query()["contract"]
		for _, ct := range contracts {
			// Check through the engine's authority, not uns's builtin table: bundle-declared
			// contracts such as _DataTags are stored too, and connectors read their catalogue
			// back through this filter.
			if !uns.IsKnown(e.ClassOf(ct)) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("unknown contract: %q", ct)})
				return
			}
		}
		// depth=N keeps entries at most N path segments below prefix and skips deeper
		// subtrees without walking them: a tree view reads one level at a time.
		depth := 0
		if raw := r.URL.Query().Get("depth"); raw != "" {
			depth, err = strconv.Atoi(raw)
			if err != nil || depth < 1 {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "depth must be a positive number of path segments"})
				return
			}
		}
		// folders=true (with depth) also names the paths at the cut that have deeper
		// entries, record or not, so a tree view learns which rows expand.
		withFolders := false
		if raw := r.URL.Query().Get("folders"); raw != "" {
			withFolders, err = strconv.ParseBool(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "folders must be true or false"})
				return
			}
			if withFolders && depth == 0 {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "folders needs depth"})
				return
			}
		}
		var entries []store.KVEntry
		var folders []string
		var next string
		if withFolders {
			entries, folders, next, err = e.Store().KVScanLevel(prefix, after, pageSize, contracts, depth)
		} else {
			entries, next, err = e.Store().KVScanPageDepth(prefix, after, pageSize, contracts, depth)
		}
		if err != nil {
			if errors.Is(err, store.ErrInvalidPageToken) {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid page token"})
				return
			}
			// A failed scan is not an empty prefix. The body stays generic so storage details
			// such as paths stay off the wire; the log has the error.
			slog.Default().Error("kv scan failed", "prefix", prefix, "err", err)
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
			entry := map[string]any{
				"path":    en.Path,
				"node_id": en.NodeID,
				"topic":   en.Topic,
				"payload": rawPayload(en.Payload),
				"ts":      en.TS,
				"offset":  en.Offset,
			}
			// Attribution mirrors /fetch's Record: omitted when this entry's
			// write carried none, rather than sent as empty strings.
			if en.WrittenBy != "" {
				entry["written_by"] = en.WrittenBy
			}
			if en.ActorID != "" {
				entry["actor_id"] = en.ActorID
			}
			if en.ActorLabel != "" {
				entry["actor_label"] = en.ActorLabel
			}
			if en.ActorKind != "" {
				entry["actor_kind"] = en.ActorKind
			}
			out = append(out, entry)
		}
		if denied > 0 {
			m.ACLDeny(metrics.ACLRead)
		}
		m.HTTPKVRead(callerLabel(c), contractLabel(contracts), prefixDepthLabel(prefix), len(out))
		if !withFolders {
			writeJSON(w, http.StatusOK, map[string]any{"entries": out, "next": next})
			return
		}
		visible := make([]string, 0, len(folders))
		for _, f := range folders {
			if c.admin || uns.AuthorizeBrowse(e.Scope(), c.entry, f) {
				visible = append(visible, f)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"entries": out, "folders": visible, "next": next})
	}))

	// GET /self is a local service's view of its own registry entry: the ULID minted
	// at self-registration and its current mount, which it needs to publish its
	// catalogue at the right topic. The entry decides, so an operator's move shows at
	// once.
	//
	// The resource-file route is mounted on both doors and is the only way to read a
	// file outside the raw local and replication digest routes. Every read goes
	// through a resource id, so the element grant check always runs. On the local door
	// a forwarded bearer is authorized as that person and a plain local service by its
	// placement; no credential is 401.
	mountResourceRoutes(mux, e, blobs, m, writeJSON, authFor)

	if local {
		mountBlobRoutes(mux, blobs, m, cfg.Limits.EffectiveMaxBlobBytes(), writeJSON, authFor)
		if secretDB != nil {
			mountLocalSecretRoutes(mux, secretDB, writeJSON, authFor)
		}

		// Local deployment controller only; the local door is the trust boundary.
		mux.HandleFunc("POST /standalone/complete", authFor(limitClassWrite, writePolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
			if !cfg.Standalone || !c.entry.IsLocal() {
				writeJSON(w, http.StatusForbidden, map[string]any{"error": "local standalone controller only"})
				return
			}
			state, err := e.Store().CompleteStandalone()
			if err != nil {
				writeJSON(w, http.StatusConflict, map[string]any{"error": "standalone retirement is incomplete"})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"standalone_since": state.Since, "standalone_ready": state.Ready})
		}))
		mux.HandleFunc("GET /self", authFor(limitClassCheap, cheapPolicy, func(w http.ResponseWriter, r *http.Request, c caller) {
			mount := ""
			if c.entry.Element != "" {
				var ok bool
				mount, ok = e.Elements().PathOf(c.entry.Element)
				if !ok {
					// An entry bound to an element missing from this node's namespace is
					// inconsistent. Never treat it as unplaced; that would widen its scope.
					writeJSON(w, http.StatusConflict, map[string]any{
						"error": "the local service's bound element is not present in this node's namespace",
					})
					return
				}
			}
			since, ready := int64(0), false
			var formerAncestors []string
			if cfg.Standalone {
				state, err := e.Store().StandaloneGet()
				if err != nil || state == nil {
					writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "standalone journal unavailable"})
					return
				}
				since, ready = state.Since, state.Ready
				formerAncestors = state.FormerAncestors
			}
			writeJSON(w, http.StatusOK, map[string]any{
				"ulid":                        c.entry.ULID,
				"name":                        c.entry.Name,
				"node":                        e.NodeID(),
				"standalone_since":            since,
				"standalone_ready":            ready,
				"standalone_former_ancestors": formerAncestors,
				"element":                     c.entry.Element,
				"mount":                       mount,
				// The upload caps a local service must respect, read here instead of copying the
				// config.
				"limits": map[string]any{
					"max_record_bytes": cfg.Limits.EffectiveMaxRecordBytes(),
					"max_blob_bytes":   cfg.Limits.EffectiveMaxBlobBytes(),
				},
			})
		}))
	}

	// Admin routes are never mounted on the local door: an unregistered route 404s and
	// reveals nothing, while a refusing one would confirm it exists.
	if !local {
		if secretDB != nil {
			mountAdminSecretRoutes(mux, secretDB, writeJSON, adminFor)
		}

		// The enrollment door is the only write path for registry entries and requires
		// admin.
		mux.HandleFunc("POST /enroll", adminFor(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, maxEnrollBodyBytes)
			body, err := readBody(r)
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "enrollment request exceeds 256 KiB"})
					return
				}
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

		mux.HandleFunc("DELETE /enroll/{ulid}", adminFor(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request) {
			ulid := r.PathValue("ulid")
			// DELETE stays an immediate kill switch with no drain precondition. Revoke reports
			// wasDraining under its own lock, so a concurrent drain cannot slip in between a
			// check and the revoke.
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

		// POST /enroll/{ulid}/drain decommissions a child node: it keeps working while its
		// queue drains, and new commands under its mount are refused. The node revokes it
		// when the drain completes, or immediately on DELETE (outcome "forced").
		mux.HandleFunc("POST /enroll/{ulid}/drain", adminFor(limitClassAdmin, adminPolicy, func(w http.ResponseWriter, r *http.Request) {
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
			// reg.Drain increments colca_drains_active itself.
			writeJSON(w, http.StatusOK, map[string]any{"ulid": ulid, "offset": off, "status": uns.StatusDraining})
		}))

		mux.HandleFunc("GET /enroll", adminFor(limitClassScan, scanPolicy, func(w http.ResponseWriter, r *http.Request) {
			pageSize, after, err := pageRequest(r)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "max must be between 1 and 10000"})
				return
			}
			entries, next := reg.ListPage(after, pageSize)
			writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "next": next})
		}))

		mux.HandleFunc("GET /debug/state", adminFor(limitClassCheap, cheapPolicy, func(w http.ResponseWriter, r *http.Request) {
			streams := map[string]any{}
			// Derived from the store: test harnesses read a node's stream set here, so a copy
			// could drift.
			for _, st := range store.Streams() {
				streams[st] = map[string]any{"next_offset": e.Store().NextOffset(st)}
			}
			writeJSON(w, http.StatusOK, map[string]any{"ulid": cfg.ULID, "streams": streams})
		}))
	}

	return mux
}

// ownsCursor reports whether the caller may move this cursor
// (uns.Entry.OwnsCursor): its own prefix, ULID-based for keyed identities and
// name-based for local services, minus the cursors only the node writes. It is
// nil-safe: no entry owns nothing.
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

// Package repl implements node-to-node replication over mTLS: the parent side
// (POST /replicate, GET /downlink) and the child side (uplink push loop,
// long-poll downlink loop).
//
// Trust is pure key pinning — there is no CA anywhere. Both ends present a
// self-signed certificate that is nothing but a container for their ed25519
// node key. The parent authorises a request by looking the peer's leaf key up
// in its local registry (kind "node"; never by anything in the request body);
// the child compares the parent's leaf key against the pinned value from its
// own config — the parent's registry entry for the child must exist BEFORE
// the child connects (auth design §6.2, entry-before-connect).
//
// The child always speaks its own local coordinates. The parent decides where
// they land: every replicated topic gets the child's mount inserted, and every
// downlinked command gets it stripped again.
package repl

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	// longPollFor is how long an empty /downlink request waits for new data
	// before answering with an empty record list.
	longPollFor = 20 * time.Second
	// longPollEvery is the re-check interval inside that wait.
	longPollEvery = 200 * time.Millisecond

	defaultDownlinkMax = 200
	maxDownlinkMax     = 500
)

type Server struct {
	cfg     *config.Config
	eng     *engine.Engine
	id      *identity.Identity
	reg     *registry.Manager
	metrics *metrics.Metrics // nil-safe: every Metrics method is a no-op on a nil receiver
	log     *slog.Logger

	mu   sync.Mutex
	http *http.Server
	ln   net.Listener
}

func (s *Server) auditDenied(operation, reason string, entry *uns.Entry, metadata map[string]any) {
	if metadata == nil {
		metadata = map[string]any{}
	}
	metadata["door"] = metrics.DoorRepl
	d := engine.AuditDenial{Operation: operation, ReasonCode: reason, Metadata: metadata}
	if entry != nil {
		d.ActorID, d.ActorLabel, d.ActorKind = entry.ULID, entry.Name, entry.ActorKind()
	}
	_ = s.eng.RecordDenial(d)
}

// NewServer builds a replication server. m may be nil (unit tests and any
// caller that does not care about metrics).
func NewServer(cfg *config.Config, eng *engine.Engine, id *identity.Identity, reg *registry.Manager, m *metrics.Metrics) (*Server, error) {
	return &Server{cfg: cfg, eng: eng, id: id, reg: reg, metrics: m,
		log: slog.Default().With("node", cfg.ULID, "comp", "repl-server")}, nil
}

// childFromReq resolves the authenticated child from the TLS client cert,
// pinned against the local registry (kind "node"). This is the ONLY source of
// identity — nothing from the request body is trusted. A machine key at this
// door is rejected exactly like an unknown one, with its own metric reason.
//
// It also returns the child's mount, resolved from its element at this moment
// (id-grants design §4): every path this door builds or strips is built from
// that value, so a renamed element takes effect on the next request instead of
// needing a re-enrollment. A child whose element does not resolve is turned
// away — there is no position to insert its records at.
func (s *Server) childFromReq(r *http.Request) (*uns.Entry, string, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthUnknownKey)
		s.auditDenied("authenticate", metrics.AuthUnknownKey, nil, map[string]any{"route": r.URL.Path})
		return nil, "", fmt.Errorf("no client certificate")
	}
	pub, err := identity.PeerPubHex(r.TLS.PeerCertificates[0].Raw)
	if err != nil {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthUnknownKey)
		s.auditDenied("authenticate", metrics.AuthUnknownKey, nil, map[string]any{"route": r.URL.Path})
		return nil, "", err
	}
	entry, ok := s.reg.ByPubkey(pub)
	if !ok {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthUnknownKey)
		s.auditDenied("authenticate", metrics.AuthUnknownKey, nil, map[string]any{"route": r.URL.Path})
		return nil, "", fmt.Errorf("client key %s not enrolled at this node", short(pub))
	}
	if !entry.MayUseDoor(uns.DoorRepl) {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthKind)
		s.auditDenied("authenticate", metrics.AuthKind, entry, map[string]any{"route": r.URL.Path})
		return nil, "", fmt.Errorf("identity %s is kind %q — the repl door is for nodes", entry.ULID, entry.Kind)
	}
	mount, placed := s.eng.Elements().PathOf(entry.Element)
	if !placed {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthKind)
		s.auditDenied("authenticate", metrics.AuthKind, entry, map[string]any{"route": r.URL.Path})
		return nil, "", fmt.Errorf("identity %s binds to element %s, which is not placed at this node",
			entry.ULID, entry.Element)
	}
	return entry, mount, nil
}

// addDefinitions puts whatever definitions the child has not read yet into the
// response (definition-stream design §5).
//
// Two things this deliberately does NOT do, and both are the same reason: a
// definition has no position. It is not filtered to the child's subtree —
// nothing addresses it at one child — and its topic is not touched, so what the
// child stores is byte-identical to what this node stores. Commands need both
// operations; definitions need neither, which is why this is five lines and the
// command path is not.
func (s *Server) addDefinitions(resp map[string]any, childULID string, defAfter uint64, limit int) {
	recs, next, err := s.eng.Store().Read("definitions", defAfter, limit, nil)
	if err != nil {
		// Durable state the child does not get this round; it will ask again.
		// Never fail the whole poll over it — commands must keep flowing.
		s.log.Error("downlink: reading definitions failed — the child stays behind on them",
			"child", childULID, "err", err)
		return
	}
	out := make([]wireRec, 0, len(recs))
	for _, rec := range recs {
		out = append(out, wireRec{
			O: rec.Offset, T: rec.Topic, P: rec.Payload, TS: rec.TS,
			WB: rec.WrittenBy, AID: rec.ActorID, AL: rec.ActorLabel, AK: rec.ActorKind,
		})
	}
	resp["definitions"], resp["def_next"] = out, next
}

// ancestryFor builds the position a child mounted at mount must know: this
// node's own chain, extended by every element from here down to the child's
// (id-grants design §4). ok=false while this node's own position is unknown —
// a guessed chain is worse than none, because grants would resolve against a
// frame nobody chose.
//
// The elements come from this node's own index, which is exactly why the child
// cannot do this for itself: those records live here, published under this
// node's identity, and the child never sees them.
func (s *Server) ancestryFor(mount string) (uns.Ancestry, bool) {
	own, ok := s.eng.Ancestry()
	if !ok {
		return nil, false
	}
	return own.Extend(s.eng.Elements(), mount), true
}

// Start binds cfg.Repl.Addr and serves TLS in the background. It returns the
// resolved address, so a node configured with ":0" can report its real port.
func (s *Server) Start() (addr string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.http != nil {
		return "", fmt.Errorf("repl server already started")
	}
	cert, err := s.id.SelfSignedCert(s.cfg.ULID)
	if err != nil {
		return "", err
	}
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert, // pinning happens in childFromReq
		MinVersion:   tls.VersionTLS13,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /replicate", s.handleReplicate)
	mux.HandleFunc("GET /downlink", s.handleDownlink)

	ln, err := net.Listen("tcp", s.cfg.Repl.Addr)
	if err != nil {
		return "", err
	}
	s.ln = ln
	s.http = &http.Server{Handler: mux, TLSConfig: tlsCfg}
	go func(h *http.Server) {
		if err := h.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("repl server stopped", "err", err)
		}
	}(s.http)
	s.log.Info("repl server listening", "addr", ln.Addr().String())
	return ln.Addr().String(), nil
}

// Stop closes the listener and every active connection immediately.
//
// It deliberately uses Close, never Shutdown: a graceful shutdown waits for
// in-flight long-poll downlinks (up to longPollFor) and keeps the port bound in
// the meantime, which breaks restarting a node on the same address. When Stop
// returns, the port is free. Safe to call twice and before Start.
func (s *Server) Stop() {
	s.mu.Lock()
	h, ln := s.http, s.ln
	s.http, s.ln = nil, nil
	s.mu.Unlock()
	if h != nil {
		_ = h.Close()
	}
	if ln != nil {
		_ = ln.Close() // idempotent; guarantees the port is released before we return
	}
}

type wireRec struct {
	O   uint64 `json:"o"`
	OO  uint64 `json:"oo,omitempty"`
	T   string `json:"t"`
	P   []byte `json:"p"`
	TS  int64  `json:"ts"`
	WB  string `json:"wb,omitempty"`
	AID string `json:"aid,omitempty"`
	AL  string `json:"al,omitempty"`
	AK  string `json:"ak,omitempty"`
}

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	child, mount, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var in struct {
		Stream  string    `json:"stream"`
		Records []wireRec `json:"records"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	repl := make([]store.ReplRecord, 0, len(in.Records))
	for _, rec := range in.Records {
		childParsed, parseErr := uns.Parse(rec.T)
		if parseErr != nil {
			http.Error(w, parseErr.Error(), http.StatusBadRequest)
			return
		}
		childClass := s.eng.ClassOf(childParsed.Contract)
		// Direction and stream are properties of the child's record. In
		// particular, _StreamGap names its physical stream in Path before any
		// hierarchy mount is inserted.
		if uns.IsKnown(childClass) && !uns.MatchesUplinkStream(childClass, childParsed, in.Stream) {
			s.auditDenied("replicate", "direction_denied", child,
				map[string]any{"route": r.URL.Path, "stream": in.Stream, "contract": childParsed.Contract})
			http.Error(w, fmt.Sprintf("contract %s may not replicate upward on stream %s", childParsed.Contract, in.Stream), http.StatusForbidden)
			return
		}
		topic := uns.MountInsert(rec.T, mount)
		parsed, parseErr := uns.Parse(topic)
		if parseErr != nil {
			http.Error(w, parseErr.Error(), http.StatusBadRequest)
			return
		}
		class := s.eng.ClassOf(parsed.Contract)
		// During a rolling bundle update the parent may not know a new contract
		// the child already routes. Preserve that record on the named stream;
		// enforce direction and stream binding whenever this node does know the
		// class. The bundle-skew test pins this forward-compatible handoff.
		rr := store.ReplRecord{
			ChildOffset: rec.O, OriginOffset: rec.OO,
			Topic: topic, Payload: rec.P, TS: rec.TS,
			WrittenBy: rec.WB, ActorID: rec.AID,
			ActorLabel: rec.AL, ActorKind: rec.AK,
		}
		// Route by the ENGINE authority (bundle-aware): a bundle-declared
		// data/entity contract must KV-project here like at any door.
		if uns.IsOwnedState(class) {
			rr.KVPath, rr.KVNode = parsed.Path, parsed.NodeID
			// A replicated tombstone retires the path here too (retention
			// design §7.1): the empty payload is the wire truth, derived
			// exactly like the engine derives it on first ingest, so every
			// ancestor's KV + retained set converge on the same fact.
			rr.Delete = len(rec.P) == 0
		}
		repl = append(repl, rr)
	}
	// Through the engine, never straight into the store: the engine is the single
	// place every write converges, and it is what mirrors the newly applied
	// records onto this node's local MQTT bus.
	applied, hwm, err := s.eng.IngestReplicated(child.ULID, in.Stream, repl)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.log.Debug("replicate", "child", child.ULID, "stream", in.Stream, "received", len(repl), "applied", applied, "hwm", hwm)
	// now_ms is stamped at response-write time, after IngestReplicated has
	// already run — the parent's own authoritative-now estimate (time-sync
	// design §2.1), not raw local time, so corrections telescope down the
	// tree. The root has no offset, so this is its raw clock.
	writeJSON(w, map[string]any{"hwm": hwm, "now_ms": s.eng.AuthoritativeNow().UnixMilli()})
}

func (s *Server) handleDownlink(w http.ResponseWriter, r *http.Request) {
	child, mount, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	if after == 0 {
		after = 1
	}
	// The child's position on the definitions stream, independent of its
	// command position.
	defAfter, _ := strconv.ParseUint(r.URL.Query().Get("def_after"), 10, 64)
	if defAfter == 0 {
		defAfter = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("max"))
	if limit <= 0 || limit > maxDownlinkMax {
		limit = defaultDownlinkMax
	}

	// hello=1 answers immediately instead of long-polling: the child's first
	// contact after (re)connect learns its position (id-grants design §4) in
	// one RTT instead of one long-poll cycle — a fresh node must not stay
	// fail-closed for humans until the first idle poll drains.
	if r.URL.Query().Get("hello") == "1" {
		resp := map[string]any{
			"records": []wireRec{}, "next": after,
			// The child's start position when it has no cursor for THIS
			// parent (parent-scoped-cursors design §3.2/§3.3): commands
			// issued before this child was attached were addressed to
			// whatever occupied the mount then, and are not delivered to a
			// newcomer.
			"head": s.eng.Store().NextOffset("commands"),
		}
		if a, ok := s.ancestryFor(mount); ok {
			resp["ancestry"] = a
		}
		s.addDefinitions(resp, child.ULID, defAfter, limit)
		writeJSON(w, resp)
		return
	}

	// Spec §5.1 [delta]: persist the child's downlink progress as an ordinary
	// named cursor, c/downlink:{child-ulid}/commands ← max(existing, after),
	// keyed by the AUTHENTICATED identity (the after parameter only carries the
	// position, never who it belongs to). Without it, a parent pruning its
	// commands stream is blind to its slowest child. CursorAck gives exactly
	// the required semantics: forward-only, one synced write per actual
	// advance (an idle re-poll with the same after writes nothing), and the
	// ct/ last-advance stamp that puts these cursors under the §5.2 staleness
	// window like every other cursor.
	s.eng.Store().CursorAck(uns.DownlinkCursorPrefix+child.ULID, "commands", after)
	// The definitions cursor is the child's own, separate position: a child
	// caught up on commands may still be behind on definitions and vice versa,
	// and one stream must never drag the other's floor along (definition-stream
	// design §5).
	s.eng.Store().CursorAck(uns.DownlinkDefCursorPrefix+child.ULID, "definitions", defAfter)

	// Move-drain design §3.2: "completion is evaluated on every /downlink
	// poll by that child" — the other trigger is the 30s periodic tick
	// (RunDrainTicker). Cheap to call unconditionally when not draining
	// (evaluateDrain's own registry lookup short-circuits), but the status
	// check here avoids that lookup on the hot path for the common
	// non-draining case.
	if child.IsDraining() {
		s.evaluateDrain(child.ULID)
	}

	// A child only ever sees commands for its own subtree.
	filter := func(topic string) bool {
		p, err := uns.Parse(topic)
		if err != nil || !uns.IsCommand(s.eng.ClassOf(p.Contract)) {
			return false
		}
		return strings.HasPrefix(p.Path, mount+"/")
	}

	ctx := r.Context()
	deadline := time.Now().Add(longPollFor)
	ticker := time.NewTicker(longPollEvery)
	defer ticker.Stop()
	for {
		// next counts filtered-out records too, so the child's cursor skips over
		// commands addressed to its siblings instead of re-scanning them forever.
		recs, next, err := s.eng.Store().Read("commands", after, limit, filter)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Gap contract on the downlink wire (spec §6.2): same shape and same
		// journal as /fetch, offsets in PARENT coordinates. The after param is
		// the first offset the child has not consumed (Read starts there), so
		// the condition is after < LWM. A gap answers the poll immediately —
		// making the child wait out the long poll to learn its position is
		// inside a pruned hole would stall §6.3's log-and-continue handling.
		gap, hasGap := s.eng.Store().Gap("commands", after)
		// A definition waiting is as good a reason to answer as a command: a
		// child must not sit out a 20s poll while policy it needs is already
		// here.
		hasDefs := s.eng.Store().NextOffset("definitions") > defAfter
		if len(recs) > 0 || hasGap || hasDefs || time.Now().After(deadline) {
			out := make([]wireRec, 0, len(recs))
			for _, rec := range recs {
				stripped, ok := uns.MountStrip(rec.Topic, mount)
				if !ok {
					continue
				}
				out = append(out, wireRec{
					O: rec.Offset, T: stripped, P: rec.Payload, TS: rec.TS,
					WB: rec.WrittenBy, AID: rec.ActorID, AL: rec.ActorLabel, AK: rec.ActorKind,
				})
			}
			// now_ms is stamped HERE, at response-write time — after the long
			// poll wait, so sample error is one-way network latency (ms), not
			// the poll duration (up to longPollFor). It is the parent's own
			// authoritative-now estimate (time-sync design §2.1), not raw
			// local time; the root has no offset, so this is its raw clock.
			// No head here: it belongs to the hello response alone
			// (parent-scoped-cursors design §3.3). A child consumes it in
			// exactly one situation — initializing a command cursor for a
			// parent it has no cursor for — and that happens before the first
			// poll, so carrying it on every poll would be an integer nothing
			// reads.
			resp := map[string]any{
				"records": out, "next": next,
				"now_ms": s.eng.AuthoritativeNow().UnixMilli(),
			}
			// Position hand-down (id-grants design §4): the parent knows its own
			// chain and where the child sits inside it, so every downlink
			// response teaches the child its position. Omitted while this
			// node's own position is still unknown — never guess frames.
			if a, ok := s.ancestryFor(mount); ok {
				resp["ancestry"] = a
			}
			s.addDefinitions(resp, child.ULID, defAfter, limit)
			if hasGap {
				// §6.3: "next already points past the hole". With surviving
				// records Read guarantees that; with none it would stay at
				// `after` and the child would re-receive the gap forever, so
				// point it at the LWM explicitly.
				if lwm := gap.ToOffset + 1; next < lwm {
					resp["next"] = lwm
				}
				resp["gap"] = gap
				s.metrics.GapServed("commands", "downlink")
			}
			s.log.Debug("downlink", "child", child.ULID, "delivered", len(out), "next", resp["next"], "gap", hasGap)
			writeJSON(w, resp)
			return
		}
		select {
		case <-ctx.Done():
			// Client gone (or the server was stopped): never outlive the request.
			return
		case <-ticker.C:
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v) // nothing can be done once the header is written
}

// short abbreviates a hex key for log/error messages without assuming a length
// (a misconfigured pubkey must not panic the process).
func short(hexKey string) string {
	if len(hexKey) <= 8 {
		return hexKey + "…"
	}
	return hexKey[:8] + "…"
}

// Package repl replicates between nodes over mTLS: the parent side (POST
// /replicate, GET /downlink) and the child side (uplink push and long-poll
// downlink loops).
//
// Trust is key pinning with no CA. Both ends present a self-signed certificate
// that only carries their ed25519 node key. The parent looks the peer's key up
// in its registry (kind "node"), so a child must be enrolled before it
// connects; the child compares the parent's key with its configured pin.
//
// The child always speaks its own local coordinates: the parent inserts the
// child's mount into replicated topics and strips it from commands it sends
// down.
package repl

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/httpserver"
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

	defaultDownlinkMax = 200
	maxDownlinkMax     = 500
)

// downlinkStreams are the streams a downlink poll answers from.
var downlinkStreams = []string{"commands", "definitions"}

type Server struct {
	cfg     *config.Config
	eng     *engine.Engine
	id      *identity.Identity
	reg     *registry.Manager
	blobs   *blobstore.Store
	metrics *metrics.Metrics // nil-safe: every Metrics method is a no-op on a nil receiver
	log     *slog.Logger

	mu      sync.Mutex
	http    *http.Server
	ln      net.Listener
	limiter *httplimit.Limiter

	// upstreamClient is this node's own parent link, used to satisfy a child's
	// pull on a local miss. Set once at startup; nil at the root.
	upstreamClient *Client

	// downlinkWakes counts waiting downlink polls woken by a change to their
	// streams. Tests read it: a commit to any other stream must not wake them.
	downlinkWakes atomic.Int64

	// inflight lets the owner of shutdown see running handlers. Set once before
	// Start; nil in tests. See SetInflightTracker.
	inflight func(http.Handler) http.Handler
}

// SetInflightTracker installs the middleware every repl route runs inside, so
// the node can wait for handlers still using the store before closing it. Stop
// closes connections instead of draining them, so without it a handler could
// still be in ApplyReplicated when Pebble closes. Call it once before Start; nil
// means untracked.
func (s *Server) SetInflightTracker(mw func(http.Handler) http.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight = mw
}

// SetUpstream installs the parent link used for pull-through. Called once at
// startup, after the repl client exists.
func (s *Server) SetUpstream(c *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.upstreamClient = c
}

func (s *Server) upstream() *Client {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.upstreamClient
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

// NewServer builds a replication server. m may be nil, and blobs may be nil in
// tests that never touch the blob routes.
func NewServer(cfg *config.Config, eng *engine.Engine, id *identity.Identity, reg *registry.Manager, blobs *blobstore.Store, m *metrics.Metrics) (*Server, error) {
	return &Server{cfg: cfg, eng: eng, id: id, reg: reg, blobs: blobs, metrics: m, limiter: httplimit.New(),
		log: slog.Default().With("node", cfg.ULID, "comp", "repl-server")}, nil
}

// childFromReq resolves the authenticated child from its TLS client
// certificate, pinned against the registry (kind "node"); nothing in the body
// is trusted. A machine key is rejected like an unknown one. It also returns the
// child's mount, resolved from its element on every request, so a renamed
// element takes effect without re-enrollment; a child whose element does not
// resolve is refused.
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

// addDefinitions adds the definitions the child has not read yet to the
// response. Unlike commands they have no position, so they are neither filtered
// to the child's subtree nor rewritten: the child stores them byte for byte.
func (s *Server) addDefinitions(resp map[string]any, childULID string, defAfter uint64, limit int) {
	recs, next, err := readDefinitions(s.eng.Store(), defAfter, limit)
	if err != nil {
		s.log.Error("downlink: reading definitions failed — the child stays behind on them",
			"child", childULID, "err", err)
		return
	}
	out := make([]wireRec, 0, len(recs))
	for _, rec := range recs {
		out = append(out, wireRec{
			O: rec.Offset, T: rec.Topic, P: rec.Payload, TS: rec.TS,
			WB: rec.WrittenBy, AID: rec.ActorID, AL: rec.ActorLabel, AK: rec.ActorKind, AG: rec.ActorGroups,
		})
	}
	resp["definitions"], resp["def_next"] = out, next
}

// definitionReader preserves the compaction/cursor boundary independently of
// transport. The head must precede the iterator snapshot: sampling it afterwards
// can acknowledge a concurrent append that was never included in the response.
type definitionReader interface {
	NextOffset(string) uint64
	Read(string, uint64, int, func(string) bool) ([]store.StoredRecord, uint64, error)
}

func readDefinitions(source definitionReader, after uint64, limit int) ([]store.StoredRecord, uint64, error) {
	head := source.NextOffset("definitions")
	rows, next, err := source.Read("definitions", after, limit, nil)
	if err == nil && next == after && head > next {
		// Compaction may leave no rows between the old cursor and captured head.
		next = head
	}
	return rows, next, err
}

// ancestryFor builds the position a child at mount must know: this node's own
// chain extended by every element down to the child. ok is false while this
// node's own position is unknown, because grants would resolve against a guessed
// frame. Only this node can build it, since the elements live in its index.
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
	mux.HandleFunc("HEAD /blobs/{sha}", s.handleBlobHead)
	mux.HandleFunc("PUT /blobs/{sha}", s.handleBlobPut)
	mux.HandleFunc("GET /blobs/{sha}", s.handleBlobGet)

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.cfg.Repl.Addr)
	if err != nil {
		return "", err
	}
	s.ln = ln
	// Outermost, so a request counts for its whole life at this door, including the
	// rate limiter and the TLS and registry lookups, which all read node state.
	h := s.limitBeforeAuth(mux)
	if s.inflight != nil {
		h = s.inflight(h)
	}
	s.http = httpserver.New(h)
	s.http.TLSConfig = tlsCfg
	go func(h *http.Server) {
		if err := h.ServeTLS(ln, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("repl server stopped", "err", err)
		}
	}(s.http)
	s.log.Info("repl server listening", "addr", ln.Addr().String())
	return ln.Addr().String(), nil
}

// Stop closes the listener and every connection at once. It uses Close, not
// Shutdown: a graceful shutdown would wait for long polls and keep the port
// bound, which breaks restarting a node on the same address. Safe to call twice
// and before Start.
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
	SkipFrom uint64   `json:"skip_from,omitempty"`
	O        uint64   `json:"o"`
	OO       uint64   `json:"oo,omitempty"`
	T        string   `json:"t"`
	P        []byte   `json:"p"`
	TS       int64    `json:"ts"`
	WB       string   `json:"wb,omitempty"`
	AID      string   `json:"aid,omitempty"`
	AL       string   `json:"al,omitempty"`
	AK       string   `json:"ak,omitempty"`
	AG       []string `json:"ag,omitempty"`
}

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	child, mount, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	release, ok := s.acquireRequest(w, limitClassReplication, child.ULID, replicationPolicy)
	if !ok {
		return
	}
	defer release()
	r.Body = http.MaxBytesReader(w, r.Body, replicateBodyLimit(s.cfg))
	var in struct {
		Stream  string    `json:"stream"`
		Records []wireRec `json:"records"`
		// Store is the child's store incarnation; empty from a child that
		// predates it.
		Store string `json:"store,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "replication request too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(in.Records) > maxReplicateRecords {
		http.Error(w, "replication batch exceeds 200 records", http.StatusRequestEntityTooLarge)
		return
	}
	// Check the stream before any record, whatever this node knows about the
	// contracts inside. The per-record check only covers classes this bundle
	// declares, so without this an unknown contract could land on definitions, the
	// one stream a child must never write, and stall definition delivery for the
	// whole subtree.
	if !uns.IsUplinkStream(in.Stream) {
		s.auditDenied("replicate", "direction_denied", child,
			map[string]any{"route": r.URL.Path, "stream": in.Stream})
		http.Error(w, fmt.Sprintf("stream %s does not accept replicated records", in.Stream), http.StatusForbidden)
		return
	}

	repl := make([]store.ReplRecord, 0, len(in.Records))
	var previous uint64
	for _, rec := range in.Records {
		if rec.O == 0 || rec.O <= previous {
			http.Error(w, "replication offsets must be positive and increasing", http.StatusBadRequest)
			return
		}
		if rec.SkipFrom != 0 {
			if in.Stream != "metrics" || rec.SkipFrom > rec.O || rec.SkipFrom <= previous || rec.T != "" || len(rec.P) != 0 || rec.OO != 0 || rec.TS != 0 || rec.WB != "" || rec.AID != "" || rec.AL != "" || rec.AK != "" || len(rec.AG) != 0 {
				http.Error(w, "invalid metric skip range", http.StatusBadRequest)
				return
			}
			repl = append(repl, store.ReplRecord{ChildOffset: rec.O, SkipFrom: rec.SkipFrom})
			previous = rec.O
			continue
		}
		previous = rec.O
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
		// During a rolling bundle update the parent may not know a contract the child
		// already routes. Keep that record on the named stream, which is known to rise;
		// direction and stream binding are enforced whenever this node knows the class.
		rr := store.ReplRecord{
			ChildOffset: rec.O, OriginOffset: rec.OO,
			Topic: topic, Payload: rec.P, TS: rec.TS,
			WrittenBy: rec.WB, ActorID: rec.AID,
			ActorLabel: rec.AL, ActorKind: rec.AK, ActorGroups: rec.AG,
		}
		// Route by the engine's bundle-aware classes: a declared data or entity
		// contract projects into KV here as at any door.
		if uns.IsOwnedState(class) {
			rr.KVPath, rr.KVNode = parsed.Path, parsed.NodeID
			// A replicated tombstone retires the path here too. It is derived from the
			// empty payload the same way the engine does on first ingest, so every ancestor
			// converges.
			rr.Delete = len(rec.P) == 0
		}
		repl = append(repl, rr)
	}
	// Through the engine, never straight into the store: the engine is the single
	// place every write converges, and it is what mirrors the newly applied
	// records onto this node's local MQTT bus.
	// A child whose store was rebuilt under the same identity restarts at offset
	// 1; the marks kept for its old store would drop everything it sends and
	// report it delivered. Adopting the new incarnation clears them first.
	reset, err := s.eng.Store().AdoptChildStore(child.ULID, in.Store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if reset {
		s.log.Warn("child store rebuilt: its replication marks were reset and it replicates from the start",
			"child", child.ULID, "store", in.Store)
	}
	applied, hwm, err := s.eng.IngestReplicated(child.ULID, in.Stream, repl)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.log.Debug("replicate", "child", child.ULID, "stream", in.Stream, "received", len(repl), "applied", applied, "hwm", hwm)
	// now_ms is stamped after IngestReplicated, from the parent's authoritative
	// clock rather than raw local time, so corrections carry down the tree. The
	// root has no offset, so there it is the raw clock.
	writeJSON(w, map[string]any{"hwm": hwm, "now_ms": s.eng.AuthoritativeNow().UnixMilli()})
}

func (s *Server) handleDownlink(w http.ResponseWriter, r *http.Request) {
	child, mount, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	release, ok := s.acquireRequest(w, limitClassReplDownlink, child.ULID, replDownlinkPolicy)
	if !ok {
		return
	}
	defer release()
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

	// hello=1 answers at once instead of long-polling, so a reconnecting child
	// learns its position in one round trip and a fresh node does not keep refusing
	// people until the first idle poll ends.
	if r.URL.Query().Get("hello") == "1" {
		head := s.eng.Store().NextOffset("commands")
		// A child with no cursor here yet starts at head: seat its delivery floor
		// there, so retention keeps what is queued for it from now on and its first
		// poll can confirm what it received. An existing cursor is left alone.
		if _, err := s.eng.Store().CursorSetIfAbsent(uns.DownlinkCursorPrefix+child.ULID, "commands", head); err != nil {
			s.log.Warn("downlink hello: delivery floor not seated", "child", child.ULID, "err", err)
		}
		resp := map[string]any{
			"records": []wireRec{}, "next": after,
			// Where a child with no cursor for this parent starts: commands issued before
			// it attached were meant for whatever held the mount then.
			"head": head,
		}
		if a, ok := s.ancestryFor(mount); ok {
			resp["ancestry"] = a
		}
		s.addDefinitions(resp, child.ULID, defAfter, limit)
		writeJSON(w, resp)
		return
	}

	// Persist the child's downlink progress as an ordinary cursor,
	// downlink:{child}/commands, keyed by the authenticated identity. Without it the
	// parent's commands prune would not see its slowest child. CursorAck is
	// forward-only, writes nothing for an idle re-poll and stamps the last advance
	// for the staleness window.
	s.noteForwarded(child.ULID, mount, after)
	s.eng.Store().CursorAck(uns.DownlinkCursorPrefix+child.ULID, "commands", after)
	// The child's definitions position is a separate cursor, so neither stream
	// holds back the other's floor.
	s.eng.Store().CursorAck(uns.DownlinkDefCursorPrefix+child.ULID, "definitions", defAfter)

	// Drain completion is checked on each poll by the draining child and by the
	// store-change/deadline wakeup (RunDrainCompletion). The status check skips that lookup in the
	// common case.
	if child.IsDraining() {
		s.evaluateDrain(child.ULID)
	}

	// A child only sees commands for its own subtree, enforced twice. This filter
	// decides what is read, and so what next counts, which lets the child's cursor
	// skip siblings' commands. MountStrip below decides what can be rewritten into
	// the child's coordinates, and a record it cannot rewrite is never sent. Neither
	// check is redundant.
	filter := func(topic string) bool {
		p, err := uns.Parse(topic)
		if err != nil || !uns.IsCommand(s.eng.ClassOf(p.Contract)) {
			return false
		}
		return uns.UnderMount(p.Path, mount)
	}

	ctx := r.Context()
	deadline := time.Now().Add(longPollFor)
	timer := time.NewTimer(longPollFor)
	defer timer.Stop()
	for {
		// Wake only on an append to this child's downlink streams. A parent with
		// many children commits replicated batches constantly; waking every waiting
		// poll on each of them made the wakeups grow with children × commits, each
		// taking the store lock. Pruning does not wake a poll: a gap can only open
		// below what the poll already read past, and the poll's deadline reports it.
		wakes, _ := s.eng.Store().StreamChanges(downlinkStreams)
		// next counts filtered-out records too, so the child's cursor skips over
		// commands addressed to its siblings instead of re-scanning them forever.
		recs, next, err := s.eng.Store().Read("commands", after, limit, filter)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// The downlink gap object has the same shape and journal as /fetch, in parent
		// coordinates. after is the first offset the child has not consumed, so there
		// is a gap when after < LWM. A gap answers at once instead of making the child
		// wait out the long poll.
		gap, hasGap := s.eng.Store().Gap("commands", after)
		// A waiting definition is as good a reason to answer as a command: a child
		// should not sit out a 20s poll while policy it needs is already here.
		hasDefs := s.eng.Store().NextOffset("definitions") > defAfter
		if len(recs) > 0 || hasGap || hasDefs || time.Now().After(deadline) {
			out := make([]wireRec, 0, len(recs))
			for _, rec := range recs {
				// The sender's grants are checked again as the command leaves this
				// node: one admitted days ago may no longer be authorized.
				if refusal := s.eng.ForwardRefusal(rec); refusal != "" {
					s.eng.RefuseForwarding(rec, refusal)
					continue
				}
				stripped, ok := uns.MountStrip(rec.Topic, mount)
				if !ok {
					continue
				}
				out = append(out, wireRec{
					O: rec.Offset, T: stripped, P: rec.Payload, TS: rec.TS,
					WB: rec.WrittenBy, AID: rec.ActorID, AL: rec.ActorLabel, AK: rec.ActorKind, AG: rec.ActorGroups,
				})
			}
			// now_ms is stamped here, after the long-poll wait, so the sample error is
			// network latency rather than the poll duration. It is the parent's
			// authoritative clock (raw on the root). No head: only the hello response
			// carries it.
			resp := map[string]any{
				"records": out, "next": next,
				"now_ms": s.eng.AuthoritativeNow().UnixMilli(),
			}
			// Every downlink response teaches the child its position in the parent's
			// chain. It is left out while this node's own position is unknown.
			if a, ok := s.ancestryFor(mount); ok {
				resp["ancestry"] = a
			}
			s.addDefinitions(resp, child.ULID, defAfter, limit)
			if hasGap {
				// next must point past the gap. With surviving records Read guarantees that;
				// with none it would stay at after and the child would receive the gap forever,
				// so point it at the LWM.
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
		case <-timer.C:
		case <-wakes["commands"]:
			s.downlinkWakes.Add(1)
		case <-wakes["definitions"]:
			s.downlinkWakes.Add(1)
		}
	}
}

// noteForwarded writes the "forwarded" progress ack for every command the child
// confirmed receiving since its last poll: the records between its downlink
// cursor here and after, the position it asks from now. The child stores a
// command and its own cursor in one write before it asks past it, so the
// position is a receipt. The ack is written before the cursor moves: a crash in
// between repeats a progress ack rather than losing one. Nothing is scanned
// before the child's first confirmed poll, or when the child asks for nothing
// new.
// maxDownlinkScan bounds one read of noteForwarded; the loop continues past it.
const maxDownlinkScan = 4096

func (s *Server) noteForwarded(child, mount string, after uint64) {
	st := s.eng.Store()
	from, known := st.CursorLookup(uns.DownlinkCursorPrefix+child, "commands")
	if !known || after <= from {
		return
	}
	if lwm := st.LWM("commands"); lwm > from {
		from = lwm
	}
	wants := func(r store.StoredRecord) bool {
		p, err := uns.Parse(r.Topic)
		return err == nil && uns.IsCommand(s.eng.ClassOf(p.Contract)) && uns.UnderMount(p.Path, mount) &&
			uns.CommandWantsProgress(r.Payload)
	}
	for from < after {
		recs, next, err := st.ReadRecordsBounded(context.Background(), "commands", from, maxDownlinkMax, int(min(after-from, uint64(maxDownlinkScan))), wants)
		if err != nil {
			s.log.Warn("downlink: progress acks for forwarded commands not written", "child", child, "err", err)
			return
		}
		for _, r := range recs {
			if r.Offset < after {
				s.eng.NoteForwarded(r, child)
			}
		}
		if next <= from {
			return
		}
		from = next
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

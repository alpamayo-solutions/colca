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
func (s *Server) childFromReq(r *http.Request) (*uns.Entry, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthUnknownKey)
		return nil, fmt.Errorf("no client certificate")
	}
	pub, err := identity.PeerPubHex(r.TLS.PeerCertificates[0].Raw)
	if err != nil {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthUnknownKey)
		return nil, err
	}
	entry, ok := s.reg.ByPubkey(pub)
	if !ok {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthUnknownKey)
		return nil, fmt.Errorf("client key %s not enrolled at this node", short(pub))
	}
	if entry.Kind != uns.KindNode {
		s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthKind)
		return nil, fmt.Errorf("identity %s is kind %q — the repl door is for nodes", entry.ULID, entry.Kind)
	}
	return entry, nil
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
	O  uint64 `json:"o"`
	T  string `json:"t"`
	P  []byte `json:"p"`
	TS int64  `json:"ts"`
}

func (s *Server) handleReplicate(w http.ResponseWriter, r *http.Request) {
	child, err := s.childFromReq(r)
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
		topic := uns.MountInsert(rec.T, child.Mount)
		rr := store.ReplRecord{ChildOffset: rec.O, Topic: topic, Payload: rec.P, TS: rec.TS}
		if p, err := uns.Parse(topic); err == nil {
			cl := uns.ClassOf(p.Contract)
			if cl == uns.ClassData || cl == uns.ClassEntity {
				rr.KVPath, rr.KVNode = p.Path, p.NodeID
				// A replicated tombstone retires the path here too (retention
				// design §7.1): the empty payload is the wire truth, derived
				// exactly like the engine derives it on first ingest, so every
				// ancestor's KV + retained set converge on the same fact.
				rr.Delete = len(rec.P) == 0
			}
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
	writeJSON(w, map[string]any{"hwm": hwm})
}

func (s *Server) handleDownlink(w http.ResponseWriter, r *http.Request) {
	child, err := s.childFromReq(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	after, _ := strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
	if after == 0 {
		after = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("max"))
	if limit <= 0 || limit > maxDownlinkMax {
		limit = defaultDownlinkMax
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
	s.eng.Store().CursorAck(downlinkCursorPrefix+child.ULID, "commands", after)

	// A child only ever sees commands for its own subtree.
	filter := func(topic string) bool {
		p, err := uns.Parse(topic)
		if err != nil || uns.ClassOf(p.Contract) != uns.ClassCmd {
			return false
		}
		return strings.HasPrefix(p.Path, child.Mount+"/")
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
		if len(recs) > 0 || hasGap || time.Now().After(deadline) {
			out := make([]wireRec, 0, len(recs))
			for _, rec := range recs {
				stripped, ok := uns.MountStrip(rec.Topic, child.Mount)
				if !ok {
					continue
				}
				out = append(out, wireRec{O: rec.Offset, T: stripped, P: rec.Payload, TS: rec.TS})
			}
			resp := map[string]any{"records": out, "next": next}
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

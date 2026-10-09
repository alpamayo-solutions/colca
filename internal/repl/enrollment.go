package repl

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/enroll"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// maxEnrollRequestBytes bounds a request body; TPM evidence is a few KiB.
const maxEnrollRequestBytes = 256 << 10

// errRevoked is the cause of a request cancelled because its identity was
// revoked or blocked, or its key changed, while it ran.
var errRevoked = errors.New("the identity was revoked or its key changed")

// SetEnrollment wires the node's enrollment manager: the request route, and
// the certificate check at admission. Call it before Start. Without it the door
// admits enrolled keys by pin alone, as before issued certificates.
func (s *Server) SetEnrollment(m *enroll.Manager) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enrollment = m
}

func (s *Server) enrollMgr() *enroll.Manager {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enrollment
}

// handleEnrollRequest is POST /enroll/request: the one route an unknown key
// may call (node enrollment spec §4). The key is the TLS peer's; the body is
// advisory except for TPM evidence, which the manager verifies.
func (s *Server) handleEnrollRequest(w http.ResponseWriter, r *http.Request) {
	m := s.enrollMgr()
	if m == nil {
		http.Error(w, "enrollment requests are not served here", http.StatusNotFound)
		return
	}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		http.Error(w, "no client certificate", http.StatusUnauthorized)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxEnrollRequestBytes)
	var req enroll.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "enrollment request: "+err.Error(), http.StatusBadRequest)
		return
	}
	code, resp := m.HandleRequest(r.TLS.PeerCertificates[0].PublicKey, req, replicationSourceKey(r))
	if code == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "3600")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

// admitCertificate checks the certificate an enrolled child presented (§7.1,
// §8): one this node issued must be valid and name the child; a self-signed one
// admits only an entry adopted without a certificate (cert_state none), and
// not at all with enrollment.require_issued_cert.
func (s *Server) admitCertificate(r *http.Request, entry *uns.Entry) error {
	m := s.enrollMgr()
	if m == nil {
		return nil
	}
	leaf := r.TLS.PeerCertificates[0]
	if m.Issuer().IssuedHere(leaf) {
		return m.Issuer().Check(leaf, entry.ULID, time.Now())
	}
	if entry.CertState == uns.CertStateNone && !m.RequireIssuedCert() {
		return nil
	}
	return enroll.ErrCertSelfSigned
}

// inflightSet holds the cancel functions of a child's running requests.
type inflightSet struct {
	mu   sync.Mutex
	next uint64
	byID map[string]map[uint64]context.CancelCauseFunc
}

// track ties r to child's in-flight set, so CancelChild ends it. done must be
// called when the handler returns.
func (s *Server) track(r *http.Request, child string) (*http.Request, func()) {
	ctx, cancel := context.WithCancelCause(r.Context())
	s.running.mu.Lock()
	if s.running.byID == nil {
		s.running.byID = map[string]map[uint64]context.CancelCauseFunc{}
	}
	id := s.running.next
	s.running.next++
	set := s.running.byID[child]
	if set == nil {
		set = map[uint64]context.CancelCauseFunc{}
		s.running.byID[child] = set
	}
	set[id] = cancel
	s.running.mu.Unlock()
	return r.WithContext(ctx), func() {
		s.running.mu.Lock()
		delete(s.running.byID[child], id)
		if len(s.running.byID[child]) == 0 {
			delete(s.running.byID, child)
		}
		s.running.mu.Unlock()
		cancel(nil)
	}
}

// CancelChild ends every request of the child that is running on this door: a
// waiting downlink poll answers 401 at once instead of after its long poll
// (node enrollment spec §8.1). The registry calls it, through the node's kick,
// on revoke, block and key change.
func (s *Server) CancelChild(ulid string) {
	s.running.mu.Lock()
	set := s.running.byID[ulid]
	cancels := make([]context.CancelCauseFunc, 0, len(set))
	for _, c := range set {
		cancels = append(cancels, c)
	}
	s.running.mu.Unlock()
	for _, c := range cancels {
		c(errRevoked)
	}
	if len(cancels) > 0 {
		s.log.Info("in-flight requests of a revoked child ended", "child", ulid, "requests", len(cancels))
	}
}

// revoked reports whether r was ended by CancelChild.
func revoked(r *http.Request) bool {
	return errors.Is(context.Cause(r.Context()), errRevoked)
}

// refuseRevoked answers a request CancelChild ended.
func (s *Server) refuseRevoked(w http.ResponseWriter, child *uns.Entry, route string) {
	s.metrics.AuthReject(metrics.DoorRepl, metrics.AuthUnknownKey)
	s.auditDenied("authenticate", "revoked", child, map[string]any{"route": route})
	http.Error(w, "revoked while the request ran", http.StatusUnauthorized)
}

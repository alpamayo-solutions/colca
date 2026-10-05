package repl

import (
	"math"
	"net"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	maxReplicateRecords       = 200
	defaultReplicateBodyBytes = 64 << 20
	limitClassReplAuth        = "auth"
	limitClassReplNode        = "node"
	limitClassReplication     = "replication"
	limitClassReplDownlink    = "downlink"
	limitClassReplTransfer    = "transfer"
)

var (
	// replAuthPolicy guards the door per source address against callers whose
	// key is not an enrolled node: unknown keys, machines, floods.
	replAuthPolicy = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 64, GlobalConcurrent: 512}
	// replNodePolicy is the door's limit for an enrolled node, per node. Many
	// children share one address behind a site router or a carrier NAT, so their
	// address says nothing about load. The per-route classes below bound the
	// door's total.
	replNodePolicy = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 8}
	// A push waits for the group commit that makes it durable (see
	// store.ApplyReplicated), so pushes in flight grow with the children times the
	// disk's sync time: 1000 children at 3 pushes/s and 80 ms is 240. The global
	// bound is the memory guard; each child holds at most two bodies.
	replicationPolicy = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 2, GlobalConcurrent: 1024}
	// A child holds one downlink poll open all the time, so its slots scale with
	// the children, not with load, and must never be taken from uplink pushes.
	// An idle poll costs a goroutine and a timer.
	replDownlinkPolicy = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 2, GlobalConcurrent: 8192}
	replTransferPolicy = httplimit.Policy{RatePerSecond: 10, Burst: 20, PerCallerConcurrent: 2, GlobalConcurrent: 32}
)

func replicationSourceKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

// admission picks the door-level limit for a request. TLS 1.3 has already
// proven that the caller holds the private key of the certificate it presented,
// so an enrolled node's key identifies that node before the registry checks in
// childFromReq; everyone else is limited by source address.
func (s *Server) admission(r *http.Request) (class, caller string, policy httplimit.Policy) {
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		if pub, err := identity.PeerPubHex(r.TLS.PeerCertificates[0].Raw); err == nil {
			if entry, ok := s.reg.ByPubkey(pub); ok && entry.MayUseDoor(uns.DoorRepl) {
				return limitClassReplNode, entry.ULID, replNodePolicy
			}
		}
	}
	return limitClassReplAuth, replicationSourceKey(r), replAuthPolicy
}

func (s *Server) limitBeforeAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		class, caller, policy := s.admission(r)
		release, ok := s.acquireRequest(w, class, caller, policy)
		if !ok {
			return
		}
		defer release()
		next.ServeHTTP(w, r)
	})
}

// replicateBodyLimit keeps the default protocol envelope at 64 MiB while
// ensuring an operator-approved single record still fits. The 2x factor
// covers base64 JSON encoding and metadata around one record.
func replicateBodyLimit(cfg *config.Config) int64 {
	forOneRecord := int64(cfg.Limits.EffectiveMaxRecordBytes())*2 + 64<<10 //nolint:gosec // config caps max_record_bytes at 1 GiB
	if forOneRecord > defaultReplicateBodyBytes {
		return forOneRecord
	}
	return defaultReplicateBodyBytes
}

func (s *Server) acquireRequest(w http.ResponseWriter, class, child string, policy httplimit.Policy) (func(), bool) {
	release, retry, ok := s.limiter.Acquire(class, child, policy)
	if ok {
		return release, true
	}
	seconds := int(math.Ceil(retry.Seconds()))
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
	http.Error(w, "request limit exceeded", http.StatusTooManyRequests)
	s.metrics.HTTPRequestLimited(metrics.DoorRepl, class)
	return nil, false
}

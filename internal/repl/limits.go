package repl

import (
	"math"
	"net"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

const (
	maxReplicateRecords       = 200
	defaultReplicateBodyBytes = 64 << 20
	limitClassReplAuth        = "auth"
	limitClassReplication     = "replication"
	limitClassReplTransfer    = "transfer"
)

var (
	replAuthPolicy     = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 64, GlobalConcurrent: 512}
	replicationPolicy  = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 2, GlobalConcurrent: 128}
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

func (s *Server) limitBeforeAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		release, ok := s.acquireRequest(w, limitClassReplAuth, replicationSourceKey(r), replAuthPolicy)
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
	forOneRecord := int64(cfg.Limits.EffectiveMaxRecordBytes())*2 + 64<<10
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

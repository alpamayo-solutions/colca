package httpapi

import (
	"math"
	"net"
	"net/http"
	"strconv"

	"github.com/alpamayo-solutions/colca/internal/httplimit"
	"github.com/alpamayo-solutions/colca/internal/metrics"
)

const (
	maxAckBodyBytes    = 16 << 10
	maxEnrollBodyBytes = 256 << 10
	defaultListPage    = 1_000
	maxListPage        = 10_000
)

const (
	limitClassHealth   = "health"
	limitClassMetrics  = "metrics"
	limitClassAuth     = "auth"
	limitClassCheap    = "cheap"
	limitClassWrite    = "write"
	limitClassFetch    = "fetch"
	limitClassScan     = "scan"
	limitClassAdmin    = "admin"
	limitClassTransfer = "transfer"
)

var (
	healthPolicy   = httplimit.Policy{RatePerSecond: 20, Burst: 40, PerCallerConcurrent: 8, GlobalConcurrent: 256}
	metricsPolicy  = httplimit.Policy{RatePerSecond: 10, Burst: 30, PerCallerConcurrent: 4, GlobalConcurrent: 4}
	authPolicy     = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 64, GlobalConcurrent: 512}
	cheapPolicy    = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 32, GlobalConcurrent: 256}
	writePolicy    = httplimit.Policy{RatePerSecond: 100, Burst: 250, PerCallerConcurrent: 32, GlobalConcurrent: 128}
	fetchPolicy    = httplimit.Policy{RatePerSecond: 25, Burst: 50, PerCallerConcurrent: 16, GlobalConcurrent: 128}
	scanPolicy     = httplimit.Policy{RatePerSecond: 5, Burst: 10, PerCallerConcurrent: 4, GlobalConcurrent: 32}
	adminPolicy    = httplimit.Policy{RatePerSecond: 10, Burst: 50, PerCallerConcurrent: 4, GlobalConcurrent: 16}
	transferPolicy = httplimit.Policy{RatePerSecond: 10, Burst: 20, PerCallerConcurrent: 4, GlobalConcurrent: 32}
)

type endpointAuth func(string, httplimit.Policy, func(http.ResponseWriter, *http.Request, caller)) http.HandlerFunc
type endpointAdminAuth func(string, httplimit.Policy, http.HandlerFunc) http.HandlerFunc

func callerLimitKey(c caller) string {
	switch {
	case c.human != nil:
		return "human:" + c.human.Sub
	case c.entry != nil:
		return string(c.entry.Kind) + ":" + c.entry.ULID
	default:
		return "admin"
	}
}

func sourceLimitKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}

func acquireRequest(
	w http.ResponseWriter,
	r *http.Request,
	limiter *httplimit.Limiter,
	m *metrics.Metrics,
	door, class, key string,
	policy httplimit.Policy,
) (func(), bool) {
	release, retry, ok := limiter.Acquire(class, key, policy)
	if ok {
		return release, true
	}
	retrySeconds := int(math.Ceil(retry.Seconds()))
	if retrySeconds < 1 {
		retrySeconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write([]byte(`{"error":"request limit exceeded"}` + "\n"))
	m.HTTPRequestLimited(door, class)
	return nil, false
}

func pageRequest(r *http.Request) (size int, after string, err error) {
	size = defaultListPage
	if raw := r.URL.Query().Get("max"); raw != "" {
		size, err = strconv.Atoi(raw)
		if err != nil || size <= 0 || size > maxListPage {
			return 0, "", strconv.ErrSyntax
		}
	}
	return size, r.URL.Query().Get("after"), nil
}

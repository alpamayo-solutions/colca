package httpapi

import (
	"encoding/json"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"

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
	limitClassWatch    = "watch"
	limitClassAdmin    = "admin"
	limitClassTransfer = "transfer"
)

var (
	healthPolicy  = httplimit.Policy{RatePerSecond: 20, Burst: 40, PerCallerConcurrent: 8, GlobalConcurrent: 256}
	metricsPolicy = httplimit.Policy{RatePerSecond: 10, Burst: 30, PerCallerConcurrent: 4, GlobalConcurrent: 4}
	authPolicy    = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 64, GlobalConcurrent: 512}
	cheapPolicy   = httplimit.Policy{RatePerSecond: 100, Burst: 200, PerCallerConcurrent: 32, GlobalConcurrent: 256}
	writePolicy   = httplimit.Policy{RatePerSecond: 100, Burst: 250, PerCallerConcurrent: 32, GlobalConcurrent: 128}
	fetchPolicy   = httplimit.Policy{RatePerSecond: 25, Burst: 50, PerCallerConcurrent: 16, GlobalConcurrent: 128}
	scanPolicy    = httplimit.Policy{RatePerSecond: 5, Burst: 10, PerCallerConcurrent: 4, GlobalConcurrent: 32}
	// A watch is one long-lived connection per consumer, reopened only after it
	// drops: few new ones a second, several held open at a time.
	watchPolicy    = httplimit.Policy{RatePerSecond: 2, Burst: 8, PerCallerConcurrent: 8, GlobalConcurrent: 256}
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

// callerLabel names a caller for the per-caller metrics with bounded
// cardinality: a registered identity by kind and name, every person as
// "human", the token as "admin".
func callerLabel(c caller) string {
	switch {
	case c.human != nil:
		return "human"
	case c.entry != nil:
		name := c.entry.Name
		if name == "" {
			name = c.entry.ULID
		}
		return string(c.entry.Kind) + ":" + name
	default:
		return "admin"
	}
}

// contractLabel is the contract filter of a read as one metric label value.
func contractLabel(contracts []string) string {
	switch len(contracts) {
	case 0:
		return "all"
	case 1:
		return contracts[0]
	default:
		return "multiple"
	}
}

// prefixDepthLabel is how many path segments a prefix names, capped at "5+".
func prefixDepthLabel(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "0"
	}
	if depth := strings.Count(prefix, "/") + 1; depth < 5 {
		return strconv.Itoa(depth)
	}
	return "5+"
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
	release, refusal, ok := limiter.Acquire(class, key, policy)
	if ok {
		return release, true
	}
	writeRefusal(w, class, refusal)
	m.HTTPRequestLimited(door, class)
	return nil, false
}

// writeRefusal answers a request the limiter refused: 429 in every case, with a
// body whose code says which bound was reached. Only a rate refusal carries
// Retry-After. A concurrency refusal clears when the caller closes one of the
// requests it holds open (for /watch, a long-lived stream), not after a delay,
// so a Retry-After of one second would only invite a retry loop that never
// succeeds.
func writeRefusal(w http.ResponseWriter, class string, refusal httplimit.Refusal) {
	body := map[string]any{"code": string(refusal.Reason), "class": class, "limit": refusal.Limit}
	switch refusal.Reason {
	case httplimit.ReasonRate:
		retrySeconds := int(math.Ceil(refusal.RetryAfter.Seconds()))
		if retrySeconds < 1 {
			retrySeconds = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
		body["error"] = "request rate limit exceeded"
	case httplimit.ReasonCallerConcurrency:
		body["error"] = "too many concurrent requests: this caller already holds the most open " + class +
			" requests it may; close one before opening another"
	case httplimit.ReasonGlobalConcurrency:
		body["error"] = "too many concurrent requests: the node holds the most open " + class + " requests it serves"
	default:
		body["error"] = "too many callers: the node tracks the most callers it may"
	}
	raw, _ := json.Marshal(body) // strings and an int: cannot fail
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write(append(raw, '\n'))
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

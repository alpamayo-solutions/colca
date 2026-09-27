package door

import (
	"errors"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// HTTPError preserves server backpressure through wrapping and service boundaries.
type HTTPError struct {
	StatusCode int
	RetryAfter time.Duration
	Cause      error
}

func (e *HTTPError) Error() string { return e.Cause.Error() }
func (e *HTTPError) Unwrap() error { return e.Cause }

func httpResponseError(response *http.Response, cause error) error {
	result := &HTTPError{StatusCode: response.StatusCode, Cause: cause}
	if response.StatusCode != http.StatusTooManyRequests {
		return result
	}
	value := response.Header.Get("Retry-After")
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		if seconds > 0 && !math.IsNaN(seconds) && !math.IsInf(seconds, 0) && seconds < float64(math.MaxInt64)/float64(time.Second) {
			result.RetryAfter = time.Duration(seconds * float64(time.Second))
		}
	} else if deadline, err := http.ParseTime(value); err == nil {
		result.RetryAfter = max(0, time.Until(deadline))
	}
	return result
}

// RetryDelay honors Retry-After for 429; otherwise it jitters the caller's bounded
// exponential delay. Callers must wait this duration even if new hints arrive.
func RetryDelay(err error, fallback time.Duration) time.Duration {
	var response *HTTPError
	if errors.As(err, &response) && response.StatusCode == http.StatusTooManyRequests && response.RetryAfter > 0 {
		return response.RetryAfter
	}
	if fallback <= 0 {
		fallback = time.Second
	}
	return fallback/2 + time.Duration(rand.Int64N(int64(fallback-fallback/2)+1)) //nolint:gosec // Retry scheduling jitter is not a security value.
}

package door

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestRetryAfterSurvivesWrappedDoorError(t *testing.T) {
	response := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {"90"}}}
	err := fmt.Errorf("worker: %w", httpResponseError(response, fmt.Errorf("limited")))
	if delay := RetryDelay(err, time.Second); delay != 90*time.Second {
		t.Fatalf("delay=%s", delay)
	}
}
func TestInvalidRetryAfterUsesBoundedJitter(t *testing.T) {
	for _, value := range []string{"", "NaN", "+Inf", "-1", "invalid"} {
		response := &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{"Retry-After": {value}}}
		delay := RetryDelay(httpResponseError(response, fmt.Errorf("limited")), 2*time.Second)
		if delay < time.Second || delay > 2*time.Second {
			t.Fatalf("%q delay=%s", value, delay)
		}
	}
}

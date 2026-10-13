package repl

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// linkReminderInterval is how long a lane may stay down before it says so again.
// The first failure and the recovery are always logged; in between the lane is a
// state, and a child waiting to be enrolled should not log every retry.
const linkReminderInterval = 5 * time.Minute

// replError is a refusal the parent answered with, as opposed to a transport
// failure. Status and the parent's message are fields, so callers need not parse
// a string, as with BlobPutError.
type replError struct {
	Route  string
	Status int
	Body   string
	// RetryAfter is the parent's Retry-After on a 429, 0 without one.
	RetryAfter time.Duration
	// Store is the parent's store incarnation on a 409 that asks for a
	// re-sync, "" otherwise.
	Store string
}

// StoreChanged reports whether the parent refused because its store is not the
// incarnation this node recorded: nothing is wrong with the request, the node
// has to re-sync first (resync.go).
func (e *replError) StoreChanged() bool { return e.Status == http.StatusConflict && e.Store != "" }

// Busy reports whether the parent asked to be called again later (429): it is
// shedding load, it did not refuse the batch.
func (e *replError) Busy() bool { return e.Status == http.StatusTooManyRequests }

// retryAfterHeader reads a Retry-After given in seconds, bounded to a minute.
func retryAfterHeader(resp *http.Response) time.Duration {
	secs, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After")))
	if err != nil || secs <= 0 {
		return 0
	}
	return time.Duration(min(secs, 60)) * time.Second
}

func (e *replError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s: %s", e.Route, replicationStatusMeaning(e.Status))
	}
	return fmt.Sprintf("%s: %s — the parent's answer: %s", e.Route, replicationStatusMeaning(e.Status), e.Body)
}

// Refused reports whether the parent answered 4xx other than 429: it read the
// request and said no. The child retries anyway, on purpose; see pushOnce in
// RunUplink.
func (e *replError) Refused() bool {
	return e.Status >= 400 && e.Status < 500 && !e.Busy() && !e.StoreChanged()
}

// Unauthorized reports whether the parent does not know this node's key (401):
// the node is not enrolled there, or not any more.
func (e *replError) Unauthorized() bool { return e.Status == http.StatusUnauthorized }

// isUnauthorized reports whether err is the parent's 401.
func isUnauthorized(err error) bool {
	var refusal *replError
	return errors.As(err, &refusal) && refusal.Unauthorized()
}

// Bounds of the wait between requests to a parent that does not know this
// node's key.
const (
	unenrolledRetryMin = time.Second
	unenrolledRetryMax = 10 * time.Second
)

// unenrolledBackoff spaces the requests of a node its parent does not know. An
// edge started before its enrolment used to ask on every lane twice a second,
// about ten refused requests a second for as long as nobody enrolled it. Every
// request of the client waits here, so one refusal quiets all lanes, and the
// wait doubles up to unenrolledRetryMax: enrolment is noticed within that.
type unenrolledBackoff struct {
	mu    sync.Mutex
	delay time.Duration
	until time.Time
}

// refused notes a 401 received at now. A refusal that arrives while a wait is
// already running (another lane's request, sent before it) does not extend it.
func (b *unenrolledBackoff) refused(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if now.Before(b.until) {
		return
	}
	b.delay = min(max(2*b.delay, unenrolledRetryMin), unenrolledRetryMax)
	b.until = now.Add(b.delay/2 + rand.N(b.delay/2+1)) //nolint:gosec // retry jitter
}

// accepted ends the backoff: the parent answered a request of this node.
func (b *unenrolledBackoff) accepted() {
	b.mu.Lock()
	b.delay, b.until = 0, time.Time{}
	b.mu.Unlock()
}

// wait is how long requests are still held back at now.
func (b *unenrolledBackoff) wait(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return max(b.until.Sub(now), 0)
}

// readReason reads the parent's explanation off a refused response, bounded so
// a misbehaving peer cannot write an unbounded string into this node's logs.
func readReason(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return strings.TrimSpace(string(body))
}

// replicationStatusMeaning turns a parent's HTTP status into something a reader
// can act on. The status only gives the class of answer; the body the parent
// sent says why, and replError carries it. The text must not guess: a 403 has
// several causes, not only a missing enrollment.
func replicationStatusMeaning(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return fmt.Sprintf(
			"http %d — the parent did not accept this node's key: it has not enrolled this node yet, "+
				"or the key it pinned is not ours (enroll the node at the parent)",
			status)
	case http.StatusForbidden:
		return fmt.Sprintf(
			"http %d — the parent read the request and refused it (an identity that may not use this door, "+
				"or a record offered on a stream it may not travel on)",
			status)
	case http.StatusNotFound:
		return fmt.Sprintf("http %d — the parent does not serve this route (wrong URL, or an older build)", status)
	case http.StatusConflict:
		return fmt.Sprintf("http %d — the parent's store is not the one this node recorded; the node re-syncs", status)
	case http.StatusRequestEntityTooLarge:
		return fmt.Sprintf("http %d — the batch exceeded the parent's request bound", status)
	case http.StatusTooManyRequests:
		return fmt.Sprintf("http %d — the parent is rate-limiting this child", status)
	}
	if status >= 500 {
		return fmt.Sprintf("http %d — the parent failed to serve the request", status)
	}
	return fmt.Sprintf("http %d", status)
}

// linkState remembers whether a named lane is currently failing, so the log
// reads like a state: down once, up once, and a reminder during a long outage.
// Every retry still happens.
type linkState struct {
	mu    sync.Mutex
	lanes map[string]*laneFailure
}

type laneFailure struct {
	since      time.Time
	lastLogged time.Time
	attempts   int
}

func newLinkState() *linkState {
	return &linkState{lanes: map[string]*laneFailure{}}
}

// Failed records one failed attempt on a lane and answers whether this one is
// worth logging: the first failure, then once per linkReminderInterval. The
// attempt count and the elapsed time are returned so the line can carry them.
func (s *linkState) Failed(lane string, now time.Time) (report bool, attempts int, down time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	failure := s.lanes[lane]
	if failure == nil {
		failure = &laneFailure{since: now, lastLogged: now, attempts: 1}
		s.lanes[lane] = failure
		return true, 1, 0
	}
	failure.attempts++
	if now.Sub(failure.lastLogged) >= linkReminderInterval {
		failure.lastLogged = now
		return true, failure.attempts, now.Sub(failure.since)
	}
	return false, failure.attempts, now.Sub(failure.since)
}

// Recovered clears a lane. It reports whether the lane had been failing, since
// only then is a line worth writing, along with what the outage cost.
func (s *linkState) Recovered(lane string, now time.Time) (wasFailing bool, attempts int, down time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	failure := s.lanes[lane]
	if failure == nil {
		return false, 0, 0
	}
	delete(s.lanes, lane)
	return true, failure.attempts, now.Sub(failure.since)
}

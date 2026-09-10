package repl

import (
	"fmt"
	"io"
	"net/http"
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
}

func (e *replError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("%s: %s", e.Route, replicationStatusMeaning(e.Status))
	}
	return fmt.Sprintf("%s: %s — the parent's answer: %s", e.Route, replicationStatusMeaning(e.Status), e.Body)
}

// Refused reports whether the parent answered 4xx: it read the request and said
// no. The child retries anyway, on purpose; see pushOnce in RunUplink.
func (e *replError) Refused() bool { return e.Status >= 400 && e.Status < 500 }

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

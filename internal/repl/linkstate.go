package repl

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// linkReminderInterval is how long an uplink or downlink lane may stay down
// before it says so again. The first failure and the recovery are always
// logged; between them the lane is a STATE, and repeating it per retry is how
// an expected startup wait (a child polling a parent that has not enrolled it
// yet) wrote 114 warnings in four minutes.
const linkReminderInterval = 5 * time.Minute

// replError is a refusal the parent ANSWERED with, as opposed to a transport
// failure (dial error, timeout, reset), which never becomes one of these.
// The status and the parent's own words are carried as fields rather than
// folded into prose, so a caller can decide what the failure means without
// parsing a string — the same shape BlobPutError already has.
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

// Refused reports whether the parent answered 4xx: it was reachable, it read
// the request, and it said no. Retrying the identical request unchanged
// cannot make it succeed — but the child retries anyway, and deliberately.
// See the rule written down in RunUplink's pushOnce.
func (e *replError) Refused() bool { return e.Status >= 400 && e.Status < 500 }

// readReason reads the parent's explanation off a refused response, bounded:
// it is a log line, not a payload, and an ill-behaved peer must not be able
// to write an unbounded string into this node's logs.
func readReason(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return strings.TrimSpace(string(body))
}

// replicationStatusMeaning turns a parent's HTTP status into something a
// reader can act on. "http 401" alone says only that something was refused —
// it does not say by whom, why, or what fixes it, which is the whole content
// of the line for whoever is looking at it at 2am.
//
// A refusal never speaks for the parent: 403 was once reported as "the parent
// has not enrolled this node yet", which is one of several things a 403 can
// mean and was flatly wrong for the common one (a record refused on the
// stream it was offered on). The status says what class of answer it is; the
// body the parent sent says why, and replError carries it.
func replicationStatusMeaning(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return fmt.Sprintf(
			"http %d — the parent did not accept this node's key: it has not enrolled this node yet, "+
				"or the key it pinned is not ours (place the node with `colca node enroll`)",
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

// linkState remembers whether one named lane is currently failing.
//
// A replication lane is a state — reachable or not — and the log should read
// like one: it goes down once, it comes back once, and a long outage says so
// on a bounded interval. Every retry in between still happens; it is just not
// news. This is deliberately NOT the sampling the broker applies to mochi's
// logging: nothing here is dropped by frequency, the transitions are simply
// the events worth reporting.
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

// Recovered clears a lane. It reports whether the lane HAD been failing —
// only then is the recovery worth a line — along with what the outage cost.
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

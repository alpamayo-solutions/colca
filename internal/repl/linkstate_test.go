package repl

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// A lane that stays down must not narrate every retry: the first failure and
// the recovery are the events, with a bounded reminder in between. A child
// waiting to be enrolled wrote 114 warnings in four minutes before this.
func TestALaneReportsItsTransitionsNotItsRetries(t *testing.T) {
	state := newLinkState()
	start := time.Now()

	report, attempts, _ := state.Failed("uplink:entities", start)
	if !report || attempts != 1 {
		t.Fatalf("the first failure must be reported: report=%v attempts=%d", report, attempts)
	}

	// Four minutes of retries, one every two seconds: all silent.
	now := start
	for i := 0; i < 120; i++ {
		now = now.Add(2 * time.Second)
		if report, _, _ := state.Failed("uplink:entities", now); report {
			t.Fatalf("retry at %s reported while the lane was already known down", now.Sub(start))
		}
	}

	// Past the reminder interval it says so again, carrying the cost so far.
	now = start.Add(linkReminderInterval + time.Second)
	report, attempts, down := state.Failed("uplink:entities", now)
	if !report {
		t.Fatal("a lane down longer than the reminder interval must say so again")
	}
	if attempts != 122 {
		t.Fatalf("the reminder must carry every attempt since the outage began, got %d", attempts)
	}
	if down < linkReminderInterval {
		t.Fatalf("the reminder must carry how long the lane has been down, got %s", down)
	}
}

func TestRecoveryIsReportedOnceAndOnlyAfterAFailure(t *testing.T) {
	state := newLinkState()
	start := time.Now()

	// A lane that never failed has no recovery to announce.
	if wasFailing, _, _ := state.Recovered("uplink:entities", start); wasFailing {
		t.Fatal("a healthy lane must not report a recovery")
	}

	state.Failed("uplink:entities", start)
	state.Failed("uplink:entities", start.Add(time.Second))
	wasFailing, attempts, down := state.Recovered("uplink:entities", start.Add(10*time.Second))
	if !wasFailing || attempts != 2 || down < 10*time.Second {
		t.Fatalf("recovery must carry the outage: failing=%v attempts=%d down=%s", wasFailing, attempts, down)
	}
	// And the lane is clean again — a later recovery announces nothing.
	if wasFailing, _, _ := state.Recovered("uplink:entities", start.Add(20*time.Second)); wasFailing {
		t.Fatal("recovery must clear the lane")
	}
}

// Lanes are independent: a failing entities stream must not silence the
// commands stream's own first failure.
func TestLanesAreTrackedIndependently(t *testing.T) {
	state := newLinkState()
	now := time.Now()

	state.Failed("uplink:entities", now)
	if report, _, _ := state.Failed("uplink:commands", now); !report {
		t.Fatal("a second lane's first failure must be reported")
	}
}

// The status alone ("http 401") says nothing a reader can act on. The 401 a
// child gets before its parent enrolls it is the single most common one, and
// it must name the remedy.
func TestAStatusIsTranslatedIntoSomethingActionable(t *testing.T) {
	unauthorized := replicationStatusMeaning(http.StatusUnauthorized)
	if !strings.Contains(unauthorized, "401") || !strings.Contains(unauthorized, "enroll") {
		t.Fatalf("401 must name the status and the remedy, got %q", unauthorized)
	}
	if !strings.Contains(replicationStatusMeaning(http.StatusNotFound), "route") {
		t.Fatalf("404 must say the route is not served, got %q", replicationStatusMeaning(http.StatusNotFound))
	}
	if !strings.Contains(replicationStatusMeaning(503), "parent failed") {
		t.Fatalf("5xx must blame the parent, got %q", replicationStatusMeaning(503))
	}
	// An unmapped status still carries its number rather than vanishing.
	if replicationStatusMeaning(418) != "http 418" {
		t.Fatalf("an unmapped status must still be reported, got %q", replicationStatusMeaning(418))
	}
}

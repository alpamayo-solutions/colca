package engine

import (
	"testing"
)

type recordingObserver struct {
	seen []string // contract + " " + topic
}

func (r *recordingObserver) Observe(contract, topic string, _ []byte) {
	r.seen = append(r.seen, contract+" "+topic)
}

// An observer reacts to records as they persist; the retained set persisted
// before this process existed never reached it — which is how a catalogue
// binding wiped between restarts stayed wiped forever. ReplayRetained hands
// the whole retained set to the observer once at startup. The binding
// outcome itself is the domain's and is pinned in plugins/uns
// (TestNewConnectorBindsDeclaredSignalsOnArrival and the re-declaration
// tests); THIS pin is the engine's half: everything retained reaches the
// observer, with its contract, and nothing does before the call.
func TestReplayRetainedHandsTheRetainedSetToTheObserver(t *testing.T) {
	e := newEngine(t) // persists two elements via IngestAdmin, observer nil

	obs := &recordingObserver{}
	e.SetObserver(obs) // wired late, as after a restart
	if len(obs.seen) != 0 {
		t.Fatalf("nothing replayed yet, but observer saw %v", obs.seen)
	}

	e.ReplayRetained()

	want := map[string]bool{
		"_SystemElement colca/v1/_SystemElement/" + e.NodeID() + "/m1":  false,
		"_SystemElement colca/v1/_SystemElement/" + e.NodeID() + "/hmi": false,
	}
	for _, s := range obs.seen {
		if _, ok := want[s]; ok {
			want[s] = true
		}
	}
	for key, seen := range want {
		if !seen {
			t.Fatalf("retained record %q never reached the observer; saw %v", key, obs.seen)
		}
	}
}

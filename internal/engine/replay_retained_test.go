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

// ReplayRetained hands every retained record to the observer with its contract,
// and nothing before the call. The binding outcome itself is tested in
// plugins/uns.
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

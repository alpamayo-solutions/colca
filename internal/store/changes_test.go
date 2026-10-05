package store

import (
	"errors"
	"github.com/cockroachdb/pebble/v2"
	"testing"
)

func TestStreamWakeFollowsCommitAndDoesNotLoseRacingAppend(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	changed, before := s.Changes()
	apply := s.appendApply
	s.appendApply = func(*pebble.Batch, *pebble.WriteOptions) error { return errors.New("disk unavailable") }
	if _, _, err := s.Append("entities", []Record{{Topic: "x"}}); err == nil {
		t.Fatal("expected failure")
	}
	select {
	case <-changed:
		t.Fatal("uncommitted write woke reader")
	default:
	}
	s.appendApply = apply
	if _, _, err := s.Append("entities", []Record{{Topic: "x"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-changed:
	default:
		t.Fatal("commit between snapshot and wait was lost")
	}
	next, after := s.Changes()
	if after["entities"].Next != before["entities"].Next+1 {
		t.Fatal(after)
	}
	select {
	case <-next:
		t.Fatal("new snapshot already signaled")
	default:
	}
	if _, _, err := s.ApplyReplicated("child", "entities", []ReplRecord{{ChildOffset: 1, Topic: "remote"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-next:
	default:
		t.Fatal("replicated commit did not wake reader")
	}
}

func TestContractHintsIgnoreUnrelatedRecordsAndIncludeReplication(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, initial := s.Changes("_Signal")
	if _, _, err := s.Append("entities", []Record{{Topic: "prekit/v1/_ServiceDetails/node/worker"}}); err != nil {
		t.Fatal(err)
	}
	_, health := s.Changes("_Signal")
	if initial["entities"] != health["entities"] {
		t.Fatal("health invalidated signal view")
	}
	if _, _, err := s.Append("entities", []Record{{Topic: "prekit/v1/_Signal/node/signal"}}); err != nil {
		t.Fatal(err)
	}
	_, signal := s.Changes("_Signal")
	if signal["entities"] == health["entities"] {
		t.Fatal("signal commit omitted")
	}
	if _, _, err := s.ApplyReplicated("child", "entities", []ReplRecord{{ChildOffset: 1, Topic: "prekit/v1/_Signal/child/signal", Delete: true}}); err != nil {
		t.Fatal(err)
	}
	_, retired := s.Changes("_Signal")
	if retired["entities"] == signal["entities"] {
		t.Fatal("replicated tombstone omitted")
	}
}

// A watch scoped to contracts reports the stream's next offset in its hints.
// It must come from the same snapshot as the scoped positions: an append
// between two separate reads paired positions that include the record with a
// next offset that does not, so the hint understated the head and the wake
// channel, taken after the append, never announced it.
func TestScopedStreamChangesTakesNextWithThePositions(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	streams := []string{"commands"}
	contracts := []string{"_Command"}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, _, err := s.Append("commands", []Record{{Topic: "prekit/v1/_Command/node/x"}}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 20000; i++ {
		wake, next, positions := s.ScopedStreamChanges(streams, contracts)
		if positions["commands"].Next > next["commands"] {
			close(stop)
			<-done
			t.Fatalf("scoped position %d is ahead of the next offset %d", positions["commands"].Next, next["commands"])
		}
		select {
		case <-wake:
		default:
		}
	}
	close(stop)
	<-done
	if _, next, _ := s.ScopedStreamChanges([]string{"commands", "nope"}, contracts); len(next) != 1 {
		t.Fatalf("unknown stream reported: %v", next)
	}
}

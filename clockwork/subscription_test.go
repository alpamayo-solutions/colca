package clockwork

import (
	"context"
	"testing"
	"time"
)

func TestSubscriptionDoesNotLoseRacingChangeAndClearsOnReconnect(t *testing.T) {
	s := &Subscription{Node: "node", Dependencies: []string{"colca/v1/_ServiceDetails/node/source"}}
	before := s.Changes()
	s.observe("colca/v1/_ClockProgress/node/source", []byte(`{"run_id":"run","processed_at":10}`), true)
	select {
	case <-before:
	default:
		t.Fatal("lost change between snapshot and wait")
	}
	rows, _ := s.State(context.Background())
	if len(rows) != 1 {
		t.Fatal("missing retained progress")
	}
	next := s.Changes()
	s.Reset()
	select {
	case <-next:
	default:
		t.Fatal("disconnect did not wake worker")
	}
	rows, _ = s.State(context.Background())
	if len(rows) != 0 {
		t.Fatal("used pre-disconnect progress")
	}
}

func TestSubscriptionRequiresLiveTimeAndSchedulesWake(t *testing.T) {
	s := &Subscription{Node: "node", Dependencies: []string{"colca/v1/_ServiceDetails/node/source"}}
	topic := "colca/v1/_TimeSync/node"
	s.observe(topic, []byte(`{"now_ms":100000}`), true)
	if s.RealNow("mqtt") != 0 {
		t.Fatal("accepted retained real time")
	}
	s.observe(topic, []byte(`{"now_ms":100000}`), false)
	if now := s.RealNow("mqtt"); now < 100 || now > 101 {
		t.Fatalf("bad projection %f", now)
	}
	s.Reset()
	if s.RealNow("mqtt") != 0 {
		t.Fatal("reused old beacon")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() { Wait(ctx, s.Changes(), time.Hour); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
}

func TestSubscriptionNotifiesCombinedWorkerWithoutHTTPScan(t *testing.T) {
	calls := 0
	s := &Subscription{Node: "node", Dependencies: []string{"colca/v1/_ServiceDetails/node/source"}, Notify: func() { calls++ }}
	s.observe("colca/v1/_ClockProgress/node/source", []byte(`{"run_id":"run","processed_at":10}`), true)
	s.Reset()
	if calls != 2 {
		t.Fatalf("got %d wakeups, want pushed progress and disconnect", calls)
	}
}

func TestLocalAliasIgnoresOtherWorkersAndKeepsEarlyProgress(t *testing.T) {
	calls := 0
	s := &Subscription{Node: "node", Dependencies: []string{"./source"}, Notify: func() { calls++ }}
	marker := []byte(`{"run_id":"run","processed_at":10}`)
	s.observe("colca/v1/_ClockProgress/node/mounted/source", marker, true)
	s.observe("colca/v1/_ClockProgress/node/mounted/other", marker, true)
	s.observe("colca/v1/_ServiceDetails/node/mounted/other", []byte(`{"name":"other"}`), true)
	s.observe("colca/v1/_ClockProgress/node/mounted/other", marker, false)
	if calls != 0 {
		t.Fatalf("unrelated or unresolved markers caused %d wakeups", calls)
	}
	s.observe("colca/v1/_ServiceDetails/node/mounted/source", []byte(`{"name":"source"}`), true)
	rows, _ := s.State(context.Background())
	if len(rows) != 2 || calls != 1 {
		t.Fatalf("lost early marker or identity: rows=%d calls=%d", len(rows), calls)
	}
	s.observe("colca/v1/_ClockProgress/node/mounted/source", marker, false)
	if calls != 1 {
		t.Fatal("duplicate marker woke consumer")
	}
	s.observe("colca/v1/_ServiceDetails/node/mounted/source", nil, false)
	if calls != 2 {
		t.Fatal("retirement did not wake consumer")
	}
	s.Reset()
	rows, _ = s.State(context.Background())
	if len(rows) != 0 {
		t.Fatal("reconnect kept old state")
	}
}

func TestHeartbeatAgeUsesLocalReceiptAndRejectsRetainedReplay(t *testing.T) {
	now := time.Now()
	topic := "colca/v1/_ClockProgress/node/source/_service"
	s := &Subscription{Node: "node", Dependencies: []string{"colca/v1/_ServiceDetails/node/source/_service"}, Now: func() time.Time { return now }}
	payload := []byte(`{"run_id":"run","processed_at":10,"observed_at":900000,"ready":true}`)
	s.observe(topic, payload, true)
	if s.Fresh(topic) {
		t.Fatal("retained state is not live health")
	}
	s.observe(topic, payload, false)
	if !s.Fresh(topic) {
		t.Fatal("remote epoch skew invalidated a live heartbeat")
	}
	now = now.Add(16 * time.Second)
	s.observe(topic, payload, false)
	if s.Fresh(topic) {
		t.Fatal("duplicate renewed stale health")
	}
	s.observe(topic, []byte(`{"run_id":"run","processed_at":10,"observed_at":900005,"ready":true}`), false)
	if !s.Fresh(topic) {
		t.Fatal("new heartbeat did not restore health")
	}
	s.Reset()
	if s.Fresh(topic) {
		t.Fatal("disconnect retained old health")
	}
}

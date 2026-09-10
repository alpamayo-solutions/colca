package mqttsrv

import (
	"fmt"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// closeBudget is how long shutdown may take before the test calls it hung. The
// failure is a deadlock, so a generous bound cannot flake.
const closeBudget = 30 * time.Second

// promptCloseBudget is the tighter bound for the "shutdown is not merely finite,
// it is quick" claim.
const promptCloseBudget = 5 * time.Second

// Shutdown could deadlock: mochi's GetByListener takes a read lock twice, and a
// disconnecting client's Delete queued in between blocks it. Server.Close avoids
// reaching it with clients in flight; these tests pin that.

// waitAttached blocks until the broker has attached want clients. paho's Connect
// returns at CONNACK, before mochi finished attachClient, and closing in that
// window trips a WaitGroup race inside mochi.
func waitAttached(t *testing.T, w *world, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if n := w.srv.S.Clients.Len(); n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d clients attached before shutdown", w.srv.S.Clients.Len(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Shutdown finishes while clients are attached. The deadline is the assertion.
func TestCloseReturnsWithClientsAttached(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()

	for i := range 4 {
		connect(t, addr, fmt.Sprintf("shutdown-client-%d", i), w.m1)
	}
	waitAttached(t, w, 4)

	done := make(chan error, 1)
	go func() { done <- w.srv.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close with clients attached: %v", err)
		}
	case <-time.After(closeBudget):
		t.Fatal("Close did not return with clients attached — shutdown deadlocked (see mqttsrv.Server.Close)")
	}
}

// Shutdown is prompt, not merely finite: a disconnected client unwinds in
// milliseconds.
func TestCloseIsPromptWithAClientAttached(t *testing.T) {
	w := newWorld(t)
	connect(t, w.srv.Addr(), "drain-me", w.m1)
	waitAttached(t, w, 1)

	start := time.Now()
	if err := w.srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= promptCloseBudget {
		t.Fatalf("Close took %v, over the %v a disconnected client needs to unwind — "+
			"shutdown is stalling somewhere, and a stalled shutdown is what the deadlock needs",
			elapsed, promptCloseBudget)
	}
}

// Once closing, the door refuses new connections, or the drain could never finish.
func TestClosingDoorRefusesNewConnections(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()

	// Prove the door is open first, so a rejection below means "closing" and
	// not a broken fixture.
	c, err := tryConnect(addr, "before-close", w.m1, w.m1.ULID)
	if err != nil {
		t.Fatalf("connect before close: %v", err)
	}
	c.Disconnect(100)

	w.srv.hook.closing.Store(true)
	if c, err := tryConnect(addr, "during-close", w.m1, w.m1.ULID); err == nil {
		c.Disconnect(100)
		t.Fatal("a closing node accepted a new connection — it must refuse, " +
			"or the drain races arrivals it can never outrun")
	}
}

// Clients disconnect while Close runs, over many rounds. The deadlock is a race
// inside mochi that no outside test can force; this one only fails if shutdown
// really does not return.
func TestCloseRacesDisconnectingClients(t *testing.T) {
	for round := range 8 {
		w := newWorld(t)
		addr := w.srv.Addr()

		clients := make([]paho.Client, 0, 3)
		for i := range 3 {
			clients = append(clients, connect(t, addr, fmt.Sprintf("racer-%d-%d", round, i), w.m1))
		}
		// Race Close against disconnecting clients, not against unfinished handshakes.
		waitAttached(t, w, 3)

		var wg sync.WaitGroup
		for _, c := range clients {
			wg.Add(1)
			go func(c paho.Client) { defer wg.Done(); c.Disconnect(0) }(c)
		}

		done := make(chan error, 1)
		go func() { done <- w.srv.Close() }()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("round %d: Close: %v", round, err)
			}
		case <-time.After(closeBudget):
			t.Fatalf("round %d: Close did not return while clients were disconnecting — "+
				"shutdown deadlocked (see mqttsrv.Server.Close)", round)
		}
		wg.Wait()
	}
}

package mqttsrv

import (
	"fmt"
	"sync"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
)

// closeBudget is how long a shutdown may take before the test calls it hung. It
// is deliberately far above any real shutdown: the failure it catches is a
// deadlock, where Close never returns at all, so any finite bound works and a
// generous one cannot flake on a loaded CI runner.
const closeBudget = 30 * time.Second

// promptCloseBudget is the tighter bound for the "shutdown is not merely finite,
// it is quick" claim.
const promptCloseBudget = 5 * time.Second

// Shutdown used to be able to hang forever. mochi's Clients.GetByListener
// (clients.go:95) holds a read lock and then calls Clients.Len, which takes the
// same read lock again; Go blocks that second acquisition the moment a writer
// is queued, and the writer is Clients.Delete — what every client runs as it
// disconnects. Closing the listeners while a client unwound therefore deadlocked
// Close against that client. It surfaced as a 10-minute package timeout in
// TestTimeSyncBeaconPeriodicCadence, i.e. as an unrelated test, which is why it
// gets its own named coverage here.
//
// Server.Close now raises the door's closing flag, disconnects its clients from
// a copied snapshot (holding no lock), and shuts the listeners down with a
// no-op closer — so closeListenerClients is never reached with clients in
// flight, and GetByListener never runs with a writer queued behind it.

// A node must finish shutting down while clients are attached — the case that
// deadlocked. The deadline is the assertion: on the old code Close never
// returned at all, so any bound catches it.
func TestCloseReturnsWithClientsAttached(t *testing.T) {
	w := newWorld(t)
	addr := w.srv.Addr()

	for i := range 4 {
		connect(t, addr, fmt.Sprintf("shutdown-client-%d", i), w.m1)
	}

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

// Shutdown must be prompt, not merely finite. A Close that crawled would mean
// clients are not unwinding when told to, which is the state the deadlock needs;
// a client that has been disconnected unwinds in milliseconds.
func TestCloseIsPromptWithAClientAttached(t *testing.T) {
	w := newWorld(t)
	connect(t, w.srv.Addr(), "drain-me", w.m1)

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

// The other half of the drain: once closing, the door must refuse arrivals.
// Without this a connection accepted mid-drain becomes the very writer the drain
// just removed, and the map cannot reach empty while clients keep arriving.
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

// The trigger in the wild was a client disconnecting at the same moment the
// listeners closed, so this drives that collision directly: clients dropping
// while Close runs, over enough rounds to hit the interleaving.
//
// Honest about what it is — the deadlock is a race inside mochi, so no test
// outside that package can force it deterministically. This one cannot fail
// spuriously (it only fails if shutdown genuinely fails to return), and the two
// tests above pin the conditions that make the race unreachable; this one is the
// end-to-end net beneath them.
func TestCloseRacesDisconnectingClients(t *testing.T) {
	for round := range 8 {
		w := newWorld(t)
		addr := w.srv.Addr()

		clients := make([]paho.Client, 0, 3)
		for i := range 3 {
			clients = append(clients, connect(t, addr, fmt.Sprintf("racer-%d-%d", round, i), w.m1))
		}

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

package historian

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// startBundledNode is a node that validates against the generated contracts,
// as a deployed one does.
func startBundledNode(t *testing.T) *node.Node {
	t.Helper()
	base := t.TempDir()
	keyFile := filepath.Join(base, "n.key")
	if _, err := identity.Generate(keyFile); err != nil {
		t.Fatal(err)
	}
	n, err := node.Start(&config.Config{
		ULID:      "n-hist",
		DataDir:   filepath.Join(base, "data"),
		KeyFile:   keyFile,
		API:       config.API{LocalAddr: "127.0.0.1:0"},
		MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
		Contracts: config.Contracts{Bundle: contractstest.GeneratedBundlePath(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n
}

// lockedStore records every applied row; the bridge and the test read it from
// different goroutines.
type lockedStore struct {
	mu      sync.Mutex
	applied int64
	rows    []Row
}

func (s *lockedStore) Applied(context.Context, string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied, nil
}

func (s *lockedStore) Apply(_ context.Context, rows []Row, _ string, offset int64, _ string) ([]Rejection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, rows...)
	s.applied = offset
	return nil, nil
}

func (s *lockedStore) count(signalID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, row := range s.rows {
		if row.SignalID == signalID {
			n++
		}
	}
	return n
}

func ingest(t *testing.T, n *node.Node, topic string, payload any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Engine.IngestAdmin(topic, body); err != nil {
		t.Fatalf("ingesting %s: %v", topic, err)
	}
}

func defineSignal(t *testing.T, n *node.Node, path, id string, logged bool) {
	ingest(t, n, "colca/v1/_Signal/n-hist/"+path,
		map[string]any{"id": id, "name": path, "is_logged": logged, "is_published": true})
}

func sample(t *testing.T, n *node.Node, path, id string, at int, value float64) {
	ingest(t, n, "colca/v1/_Metric/n-hist/"+path,
		map[string]any{"signal_id": id, "timestamp": float64(at), "value": value})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting until %s", what)
}

// Against a real node: the flag is read from the node's _Signal records, a
// change to it is followed through the entities stream, samples of a signal
// that is not logged are consumed (the metrics cursor reaches the head) and
// the signal cursor keeps up.
func TestANodesIsLoggedFlagDecidesWhatTheHistorianStores(t *testing.T) {
	n := startBundledNode(t)
	const current, temp = "01K00000000000000000CVRRNT", "01K000000000000000000000TP"
	defineSignal(t, n, "press/current", current, false)
	defineSignal(t, n, "press/temp", temp, true)
	for i := range 5 {
		sample(t, n, "press/current", current, 1_700_000_000+i, float64(i))
		sample(t, n, "press/temp", temp, 1_700_000_000+i, float64(i))
	}

	client := func() *door.Client {
		return &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var changes, signalChanges door.Signal
	go client().WatchForever(ctx, []string{"metrics", "entities"}, 20*time.Millisecond, 100*time.Millisecond,
		func(hint door.Hint) {
			for _, stream := range hint.Streams {
				switch stream {
				case "metrics":
					changes.Notify()
				case "entities":
					signalChanges.Notify()
				}
			}
		}, nil)
	signals := &Signals{Door: client(), Changes: signalChanges.Changes}
	store := &lockedStore{}
	bridge := &Bridge{Door: client(), Store: store, Signals: signals, Changes: changes.Changes}
	done := make(chan struct{}, 2)
	go func() { _ = signals.Run(ctx); done <- struct{}{} }()
	go func() { _ = bridge.Run(ctx); done <- struct{}{} }()
	defer func() { cancel(); <-done; <-done }()

	metricsHead := func() int64 { return int64(n.Store.NextOffset("metrics")) }
	eventually(t, "the first samples are consumed", func() bool {
		return int64(n.Store.CursorGet(Cursor, "metrics")) == metricsHead()
	})
	if got := store.count(temp); got != 5 {
		t.Fatalf("%d rows of the logged signal, want 5", got)
	}
	if got := store.count(current); got != 0 {
		t.Fatalf("%d rows of a signal that says is_logged false, want 0", got)
	}
	if bridge.NotLogged() != 5 {
		t.Fatalf("NotLogged = %d, want 5", bridge.NotLogged())
	}

	// The operator turns history on for the current: new samples are stored.
	defineSignal(t, n, "press/current", current, true)
	eventually(t, "the flag change is followed", func() bool { return signals.Logged(current) })
	for i := range 3 {
		sample(t, n, "press/current", current, 1_700_000_100+i, float64(i))
	}
	eventually(t, "the new samples are stored", func() bool { return store.count(current) == 3 })

	// And off again.
	defineSignal(t, n, "press/current", current, false)
	eventually(t, "the flag change is followed", func() bool { return !signals.Logged(current) })
	for i := range 3 {
		sample(t, n, "press/current", current, 1_700_000_200+i, float64(i))
	}
	eventually(t, "the last samples are consumed", func() bool {
		return int64(n.Store.CursorGet(Cursor, "metrics")) == metricsHead()
	})
	if got := store.count(current); got != 3 {
		t.Fatalf("%d rows of the current, want 3: samples after is_logged false were stored", got)
	}
	eventually(t, "the signal cursor is at the entities head", func() bool {
		return n.Store.CursorGet(SignalCursor, "entities") == n.Store.NextOffset("entities")
	})
	if signals.Problem() != "" {
		t.Fatalf("signal follower reports %q", signals.Problem())
	}
}

// A restarted historian loads the current definitions before it reads a
// sample, so a backlog of an unlogged signal is not written on startup.
func TestABacklogIsFilteredByTheFlagLoadedAtStartup(t *testing.T) {
	n := startBundledNode(t)
	const current = "01K00000000000000000CVRRNT"
	defineSignal(t, n, "press/current", current, true)
	defineSignal(t, n, "press/current", current, false)
	for i := range 4 {
		sample(t, n, "press/current", current, 1_700_000_000+i, float64(i))
	}
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	signals := &Signals{Door: client}
	store := &lockedStore{}
	bridge := &Bridge{Door: client, Store: store, Signals: signals}
	ctx := context.Background()
	if err := signals.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	for {
		fetched, _, err := bridge.pass(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if fetched == 0 {
			break
		}
	}
	if got := store.count(current); got != 0 {
		t.Fatalf("%d backlog rows written for a signal that says is_logged false", got)
	}
	if acked := int64(n.Store.CursorGet(Cursor, "metrics")); acked != int64(n.Store.NextOffset("metrics")) {
		t.Fatalf("metrics cursor at %d, head %d", acked, n.Store.NextOffset("metrics"))
	}
}

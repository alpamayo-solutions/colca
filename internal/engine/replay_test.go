package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const redeliveredCounter = "colca_command_redelivered_total"

// replayHarness lets replay tests steer per topic which subscribers are listening,
// and records what was published.
type replayHarness struct {
	e  *Engine
	m  *metrics.Metrics
	mu sync.Mutex
	// sub is keyed by topic and subscriber ULID, so the stub can say someone else is
	// listening but not the target.
	sub map[[2]string]bool
	out []string
	// onDeliver, when set, runs inside the delivery callback with h.mu released, so a
	// test can hold one replay mid-publish.
	onDeliver func(topic string)
}

func newReplayHarness(t *testing.T, ids fakeIDs) *replayHarness {
	t.Helper()
	captureLogs(t)
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := &replayHarness{sub: map[[2]string]bool{}}
	m := metrics.New(s, config.Retention{}, nil)
	e := New(s, &config.Config{ULID: "n-edge1"}, ids, func(topic string, _ []byte, _ bool) {
		h.mu.Lock()
		h.out = append(h.out, topic)
		hook := h.onDeliver
		h.mu.Unlock()
		if hook != nil {
			hook(topic)
		}
	}, m, nil)
	e.SetSubscriberCheck(func(topic, ulid string) bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.sub[[2]string{topic, ulid}]
	})
	h.e, h.m = e, m
	placeTestElements(t, e)
	return h
}

// listen and deafen model one identity connecting and dropping; with a clean
// session the subscription disappears on disconnect.
func (h *replayHarness) listen(ulid string, topics ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, topic := range topics {
		h.sub[[2]string{topic, ulid}] = true
	}
}

func (h *replayHarness) deafen(ulid string, topics ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, topic := range topics {
		h.sub[[2]string{topic, ulid}] = false
	}
}

// published returns the topics handed to the bus since the last reset, so a
// test can tell a replay's publishes apart from the original ingest's.
func (h *replayHarness) published() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.out...)
}

func (h *replayHarness) resetPublished() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.out = nil
}

func (h *replayHarness) cursor(entry *uns.Entry) uint64 {
	return h.e.Store().CursorGet(entry.CommandCursor(), "commands")
}

func machine(t *testing.T, ids fakeIDs, ulid string) *uns.Entry {
	t.Helper()
	e, ok := ids.entries[ulid]
	if !ok {
		t.Fatalf("fixture has no entry %q", ulid)
	}
	return e
}

// A command published while the machine was not listening is republished when it
// subscribes.
func TestOwedCommandIsReplayedWhenTheMachineSubscribes(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"

	if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-owed")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, h.m, undeliveredCounter); got != 1 {
		t.Fatalf("%s = %v, want 1 — the setup did not actually miss the machine", undeliveredCounter, got)
	}

	h.resetPublished()
	h.listen("m1", topic) // the machine connects and its SUBSCRIBE is registered
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 1 {
		t.Fatalf("ReplayOwedCommands = %d, want 1", n)
	}
	if got := h.published(); len(got) != 1 || got[0] != topic {
		t.Fatalf("replay published %v, want exactly [%q]", got, topic)
	}
	if got := metricstest.Value(t, h.m, redeliveredCounter); got != 1 {
		t.Fatalf("%s = %v, want 1", redeliveredCounter, got)
	}
}

// A command that reached a subscriber is not replayed. This is the denominator
// for every not-replayed assertion here.
func TestDeliveredCommandIsNotReplayed(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"
	h.listen("m1", topic)

	if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-delivered")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	h.resetPublished()
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 0 {
		t.Fatalf("ReplayOwedCommands = %d, want 0 (it was delivered the first time)", n)
	}
	if got := h.published(); len(got) != 0 {
		t.Fatalf("replay published %v, want nothing", got)
	}
}

// A replay never hands one identity another's commands.
func TestReplayNeverHandsOneMachineAnothersCommands(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	forM1 := "colca/v1/_CmdParam/m1/temp/set"

	if _, err := h.e.IngestAdmin(forM1, cmdPayload("corr-m1")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	h.resetPublished()
	// Both identities listen on m1's command topic, so only record selection keeps
	// m1's command from hmi.
	h.listen("hmi", forM1)
	h.listen("m1", forM1)
	if n := h.e.ReplayOwedCommands(machine(t, ids, "hmi")); n != 0 {
		t.Fatalf("ReplayOwedCommands(hmi) = %d, want 0 — it was handed m1's command", n)
	}
	if got := h.published(); len(got) != 0 {
		t.Fatalf("replay published %v to hmi, want nothing", got)
	}
	// And the denominator: m1 itself is still owed it, so the zero above is a
	// refusal rather than an empty stream.
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 1 {
		t.Fatalf("ReplayOwedCommands(m1) = %d, want 1 — the command was not owed to anyone, so the hmi assertion proves nothing", n)
	}
}

// An expired command is never replayed, and the cursor moves past it so it is not
// rescanned.
func TestExpiredCommandIsSkippedAndLeftBehind(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"

	if _, err := h.e.IngestAdmin(topic, expiredCmdPayload(t, "corr-expired")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	head := h.e.Store().NextOffset("commands")
	h.resetPublished()
	h.listen("m1", topic)
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 0 {
		t.Fatalf("ReplayOwedCommands = %d, want 0 (expired)", n)
	}
	if got := h.cursor(machine(t, ids, "m1")); got != head {
		t.Fatalf("cursor = %d, want %d — an expired command must be left behind, not rescanned forever", got, head)
	}
}

func TestStandaloneRetiresOwedCommandsWithoutHoldingNewOnes(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	old := "colca/v1/_CmdParam/m1/temp/old"
	fresh := "colca/v1/_CmdParam/m1/temp/new"
	if _, err := h.e.IngestAdmin(old, cmdPayload("old-owner")); err != nil {
		t.Fatal(err)
	}
	if err := h.e.Store().StandalonePut(&store.StandaloneState{
		Since: time.Now().Unix(), PATs: map[string]bool{},
		CommandsBefore: h.e.Store().NextOffset("commands"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.e.IngestAdmin(fresh, cmdPayload("new-owner")); err != nil {
		t.Fatal(err)
	}
	h.resetPublished()
	h.listen("m1", old, fresh)
	if count := h.e.ReplayOwedCommands(machine(t, ids, "m1")); count != 1 {
		t.Fatalf("replayed %d commands, want only the new owner's one", count)
	}
	if got := h.published(); len(got) != 1 || got[0] != fresh {
		t.Fatalf("retired command replayed: %v", got)
	}
	if h.e.owedBelow("commands", 1, 2, "m1") {
		t.Fatal("retired command still blocks immediate delivery of new commands")
	}
}

// A replay stops at the first record nobody is listening on, so commands never
// arrive out of order.
func TestReplayStopsAtTheFirstRecordWithNoSubscriber(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	first := "colca/v1/_CmdParam/m1/temp/set"
	second := "colca/v1/_CmdParam/m1/speed/set"

	for _, topic := range []string{first, second} {
		if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-"+topic)); err != nil {
			t.Fatalf("IngestAdmin %s: %v", topic, err)
		}
	}
	h.resetPublished()
	// The machine listens to only one of the two, to put an unlistened record before
	// a listened one.
	h.listen("m1", second)
	h.deafen("m1", first)
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 0 {
		t.Fatalf("ReplayOwedCommands = %d, want 0 — it skipped past a record nobody was listening on", n)
	}
	if got := h.published(); len(got) != 0 {
		t.Fatalf("replay published %v, want nothing (the first record blocks the second)", got)
	}
	// Once the earlier record can be delivered, both flow, in order.
	h.listen("m1", first)
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 2 {
		t.Fatalf("ReplayOwedCommands = %d, want 2", n)
	}
	got := h.published()
	if len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("replay published %v, want [%q %q] in that order", got, first, second)
	}
}

// A second subscribe does not republish what the first one replayed.
func TestReplayIsIdempotentAcrossSubscribes(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"

	if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-once")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	h.listen("m1", topic)
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 1 {
		t.Fatalf("first ReplayOwedCommands = %d, want 1", n)
	}
	h.resetPublished()
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 0 {
		t.Fatalf("second ReplayOwedCommands = %d, want 0 (already replayed)", n)
	}
	if got := h.published(); len(got) != 0 {
		t.Fatalf("second replay published %v, want nothing", got)
	}
}

// Nothing is owed to identities without an MQTT door: a child node and a nil
// entry. Otherwise every ancestor would replay a descendant's commands onto its
// bus.
func TestReplayOffersNothingToIdentitiesWithNoMQTTDoor(t *testing.T) {
	ids := testIDs()
	ids.entries["n-child"] = &uns.Entry{ULID: "n-child", Kind: uns.KindNode, Element: "el-m1"}
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/n-child/m1/set"

	if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-child")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	h.resetPublished()
	h.listen("m1", topic)
	if n := h.e.ReplayOwedCommands(ids.entries["n-child"]); n != 0 {
		t.Fatalf("ReplayOwedCommands(child node) = %d, want 0 — it is fed over replication", n)
	}
	if n := h.e.ReplayOwedCommands(nil); n != 0 {
		t.Fatalf("ReplayOwedCommands(nil) = %d, want 0", n)
	}
	if got := h.published(); len(got) != 0 {
		t.Fatalf("replay published %v, want nothing", got)
	}
}

// A command arriving while an older one is owed is held, and the replay delivers
// both in order, each once. A forward ack would have moved the floor past the
// older command and lost it. This happens whenever a replay stalled or hit its
// cap.
func TestACommandArrivingBehindAnOwedOneWaitsForIt(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	first := "colca/v1/_CmdParam/m1/temp/set"
	second := "colca/v1/_CmdParam/m1/speed/set"

	// A: published while the machine is away, so it is owed.
	if _, err := h.e.IngestAdmin(first, cmdPayload("corr-A")); err != nil {
		t.Fatalf("IngestAdmin A: %v", err)
	}
	// B: published once the machine listens. It must not go out live while A is owed.
	h.listen("m1", first, second)
	h.resetPublished()
	if _, err := h.e.IngestAdmin(second, cmdPayload("corr-B")); err != nil {
		t.Fatalf("IngestAdmin B: %v", err)
	}
	if got := h.published(); len(got) != 0 {
		t.Fatalf("live publish of %v jumped the queue ahead of the owed command", got)
	}

	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 2 {
		t.Fatalf("ReplayOwedCommands = %d, want 2 — a command was stranded", n)
	}
	got := h.published()
	if len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatalf("replay published %v, want [%q %q] in that order", got, first, second)
	}
}

// The delivery floor moves only for a subscription of the command's own target.
// Here only an observer with read:# is listening when the command lands.
func TestAThirdPartySubscriberDoesNotMarkACommandDelivered(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"

	h.listen("observer", topic) // a passive reader, subscribed to m1's commands
	if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-observed")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	if got := metricstest.Value(t, h.m, undeliveredCounter); got != 1 {
		t.Fatalf("%s = %v, want 1 — an observer's subscription was taken for the machine's", undeliveredCounter, got)
	}

	h.resetPublished()
	h.listen("m1", topic) // now the machine itself arrives
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 1 {
		t.Fatalf("ReplayOwedCommands = %d, want 1 — the command was lost to the observer's subscription", n)
	}
}

// Two concurrent replays for one identity publish each command once. The
// interleaving is forced: the first replay blocks in its deliver callback while
// the second gets every chance to publish. A timing-based version passes without
// the lock.
func TestConcurrentReplaysForOneIdentityPublishOnce(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"

	if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-race")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	h.resetPublished()
	h.listen("m1", topic)

	inDeliver := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.onDeliver = func(string) {
		once.Do(func() {
			close(inDeliver)
			<-release
		})
	}

	var wg sync.WaitGroup
	total := make([]int, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		total[0] = h.e.ReplayOwedCommands(machine(t, ids, "m1"))
	}()

	<-inDeliver // replay #1 is mid-publish, cursor not yet advanced
	wg.Add(1)
	go func() {
		defer wg.Done()
		total[1] = h.e.ReplayOwedCommands(machine(t, ids, "m1"))
	}()
	// Let replay 2 read the unadvanced cursor. Without the lock it publishes; with it,
	// it waits for replay 1.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := total[0] + total[1]; got != 1 {
		t.Fatalf("the two replays published %d commands between them, want 1", got)
	}
	if got := h.published(); len(got) != 1 {
		t.Fatalf("bus received %v, want exactly one copy of the command", got)
	}
}

// Live delivery works while the floor lags the head. The floor only advances over
// this machine's records, so unrelated records in between are normal; the gate
// must ask whether anything below is owed, not whether the floor is at this
// record.
func TestDeliveryWorksWhenTheFloorLagsBehindUnrelatedRecords(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	forM1 := "colca/v1/_CmdParam/m1/temp/set"
	forHMI := "colca/v1/_CmdParam/hmi/temp/set"
	h.listen("m1", forM1)
	h.listen("hmi", forHMI)

	// m1's first command: delivered, floor moves to just past it.
	if _, err := h.e.IngestAdmin(forM1, cmdPayload("corr-1")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	// Records that are none of m1's business pile up in between.
	for range 5 {
		if _, err := h.e.IngestAdmin(forHMI, cmdPayload("corr-other")); err != nil {
			t.Fatalf("IngestAdmin other: %v", err)
		}
	}
	before := metricstest.Value(t, h.m, undeliveredCounter)
	h.resetPublished()

	// m1's next command, now well above its floor, must still go out live.
	if _, err := h.e.IngestAdmin(forM1, cmdPayload("corr-2")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	got := h.published()
	if len(got) != 1 || got[0] != forM1 {
		t.Fatalf("published %v, want exactly [%q] — a lagging floor blocked live delivery", got, forM1)
	}
	if now := metricstest.Value(t, h.m, undeliveredCounter); now != before {
		t.Fatalf("%s rose from %v to %v for a command that WAS delivered", undeliveredCounter, before, now)
	}
	if n := h.e.ReplayOwedCommands(machine(t, ids, "m1")); n != 0 {
		t.Fatalf("ReplayOwedCommands = %d, want 0 — the floor did not cross the unrelated records", n)
	}
}

// A live delivery racing a replay publishes once. The replay captures the head
// when it starts, so it can see a record ingest is still publishing; holding the
// target's lock across publish and advance prevents the second copy. Forced:
// ingest is held in its deliver callback while the replay runs.
func TestALiveDeliveryRacingAReplayPublishesOnce(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"
	h.listen("m1", topic)
	h.resetPublished() // drop the element placements the harness wrote

	inDeliver := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.onDeliver = func(string) {
		once.Do(func() {
			close(inDeliver)
			<-release
		})
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-live")); err != nil {
			t.Errorf("IngestAdmin: %v", err)
		}
	}()

	<-inDeliver // B is on the bus; the floor has not moved yet
	replayed := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		replayed = h.e.ReplayOwedCommands(machine(t, ids, "m1"))
	}()
	time.Sleep(50 * time.Millisecond) // let the replay reach the cursor read
	close(release)
	<-done
	wg.Wait()

	if replayed != 0 {
		t.Fatalf("replay published %d copies of a command the ingest path had just delivered", replayed)
	}
	if got := h.published(); len(got) != 1 {
		t.Fatalf("bus received %v, want exactly one copy of the command", got)
	}
}

// Ordinary delivery advances the cursor too, so a long-connected machine's replay
// scan stays short.
func TestDeliveryAdvancesTheCursorSoReplayStaysCheap(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	topic := "colca/v1/_CmdParam/m1/temp/set"
	h.listen("m1", topic)

	for range 3 {
		if _, err := h.e.IngestAdmin(topic, cmdPayload("corr-live")); err != nil {
			t.Fatalf("IngestAdmin: %v", err)
		}
	}
	head := h.e.Store().NextOffset("commands")
	if got := h.cursor(machine(t, ids, "m1")); got != head {
		t.Fatalf("cursor = %d, want %d (head) — delivered commands did not move the floor", got, head)
	}
}

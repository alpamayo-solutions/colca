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

// replayHarness is the undelivered harness with the two things the replay
// tests must steer per topic rather than once per engine: which topics
// currently have a subscriber, and what was actually published to the bus.
type replayHarness struct {
	e  *Engine
	m  *metrics.Metrics
	mu sync.Mutex
	// sub is keyed by {topic, subscriber ULID}: the stub must be able to say
	// "somebody is listening here, but not the machine this command is for",
	// which is the whole substance of the identity rule.
	sub map[[2]string]bool
	out []string
	// onDeliver, when set, runs inside the delivery callback — the hook the
	// concurrency test uses to hold one replay mid-publish while another runs.
	// Called with h.mu released, so the hook may call back into the harness.
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

// listen/deafen model one identity connecting and dropping. Under a clean
// session (cmd/colca-machine) the subscription leaves mochi's topic index the
// moment the machine disconnects, which is exactly what deafen represents.
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

// The gap itself, closed: a command published while the machine was not
// listening is republished when it subscribes. Before this, the record sat in
// the stream and nothing ever read it on the machine's behalf.
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

// The denominator for every "is not replayed" assertion in this file: a
// command that DID reach a subscriber must not be replayed. Without this half,
// a ReplayOwedCommands that always returned 0 would satisfy all of them.
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

// A replay must never hand one identity another's commands. This is the
// engine-side wiring of uns.OwedCommand's identity rule — the guard that stops
// a subscribing machine from draining the whole command stream onto the bus.
func TestReplayNeverHandsOneMachineAnothersCommands(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	forM1 := "colca/v1/_CmdParam/m1/temp/set"

	if _, err := h.e.IngestAdmin(forM1, cmdPayload("corr-m1")); err != nil {
		t.Fatalf("IngestAdmin: %v", err)
	}
	h.resetPublished()
	// Both identities are listening on m1's command topic — hmi may, holding
	// cmd: over el-m1 — so nothing but the record-selection rule can keep m1's
	// command away from hmi.
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

// An expired command is never replayed, and — the half that matters for cost —
// the cursor moves past it, so it is not rescanned on every future subscribe.
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

// Order is the whole point of a cursor: a replay that hit a record nobody is
// listening on must STOP there rather than skip ahead, or the machine receives
// a later command before an earlier one it never got at all.
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
	// The machine subscribed to only one of the two — an artificial split, but
	// it is the only way to put an unlistened record BEFORE a listened one.
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

// A second subscribe must not re-publish what the first one already replayed.
// Machines reconnect and resubscribe routinely; an unadvanced cursor would
// re-run every command in the backlog each time.
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

// Nothing is owed to an identity that cannot be handed a command over this bus
// — a child node (fed over replication) and a nil entry both answer "no door".
// Without this the replay would publish a descendant's transiting commands
// onto the local bus at every ancestor.
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

// A command arriving while an older one is still owed must
// not strand that older one, and must not overtake it either.
//
// The floor is a watermark: it can say "everything below is handled", never "A
// is owed but B was delivered". So the ingest path advances it only by
// compare-and-swap from the record's own offset, and — the half that makes
// that coherent — publishes live only when the floor stands at that record. B
// is therefore HELD, and the replay hands the machine A then B, in order, each
// once. A forward ack instead would have moved the floor past A and lost it.
//
// This is not a narrow race: it is reached whenever a replay stalled or hit
// its cap, and also in the microseconds between mochi registering a
// subscription and OnSubscribed running the replay.
func TestACommandArrivingBehindAnOwedOneWaitsForIt(t *testing.T) {
	ids := testIDs()
	h := newReplayHarness(t, ids)
	first := "colca/v1/_CmdParam/m1/temp/set"
	second := "colca/v1/_CmdParam/m1/speed/set"

	// A: published while the machine is away — owed.
	if _, err := h.e.IngestAdmin(first, cmdPayload("corr-A")); err != nil {
		t.Fatalf("IngestAdmin A: %v", err)
	}
	// B: published once the machine is listening. It must NOT go out live,
	// because A is still owed and would otherwise be overtaken.
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

// The delivery floor must move only for a subscription
// belonging to the command's OWN target.
//
// An observer holding read:# legitimately subscribes to command topics for
// diagnostics. If the subscriber check answered "does anyone subscribe here",
// that observer would mark an absent machine's commands delivered — no replay,
// and no counter either, so the loss would be silent. This is the finding's
// scenario exactly: only the observer is listening when the command lands.
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

// Two replays for one identity must not publish the same
// command twice, and a command run twice is a physical-world action.
//
// The overlap is real: a session takeover does not wait for the displaced
// connection's OnSubscribed to return, and two clients presenting one
// certificate under different client IDs coexist without takeover at all.
// Neither the per-record subscriber check nor the cursor's compare-and-swap
// dedupes a publish — only the per-identity lock does.
// The interleaving is FORCED rather than raced for. Two goroutines started
// together almost never overlap in the few microseconds that matter, so a
// timing-based version of this test passes with the lock removed — verified by
// mutation, which is why it is written this way. The first replay blocks inside
// its own deliver callback until the second replay has been given every chance
// to read the same cursor and publish from it.
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
	// Give replay #2 a real chance to read the un-advanced cursor and publish
	// from it. Without the per-identity lock it does exactly that; with the
	// lock it blocks here until replay #1 releases.
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

// The delivery floor legitimately lags head, and live delivery must survive
// that. Found by the level-3 suite, which is the first place the commands
// stream is long enough for it to show.
//
// The floor advances only over records concerning ONE machine, while the
// commands stream also carries acks and other machines' commands. So after m1
// is handed a command at offset 10 its floor is 11, and the next command
// addressed to it may land at offset 30 with a dozen unrelated records in
// between. A gate asking "is the floor exactly at this record" reads that
// ordinary state as "something is owed" and holds every command forever,
// delivering nothing at all. The gate has to ask whether anything is owed
// BELOW the record instead.
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

// The other half of finding I1: a live delivery racing a replay must not hand
// the machine two copies either.
//
// The window is real because a replay captures the stream head when it starts,
// so a command appended just before that is inside its scan while the ingest
// path is still publishing it. Ingest publishes B and has not yet advanced the
// floor; the replay reads that un-advanced floor, sees B as owed, and publishes
// it a second time. Holding the target's lock across ingest's publish-and-
// advance is what closes it — the same lock the replay takes.
//
// Forced, not raced for: ingest is held inside its own deliver callback while
// the replay runs.
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

// The cursor advances on ordinary delivery, not only on replay. This is what
// keeps a long-connected machine's replay scan short: without it the cursor
// would sit at 1 forever and every subscribe would rescan the whole stream to
// discover it owes nothing.
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

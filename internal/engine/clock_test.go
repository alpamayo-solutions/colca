package engine

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/hostclock"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// clockWorld is an engine and metrics on one injected clock: a settable wall
// clock and a settable host reading.
type clockWorld struct {
	e    *Engine
	m    *metrics.Metrics
	wall *fakeNow
	host hostclock.Status
}

func newClockWorld(t *testing.T, isRoot bool) *clockWorld {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	w := &clockWorld{wall: &fakeNow{t: time.UnixMilli(1_700_000_000_000)}}
	clk := clock.NewWithHost(isRoot, w.wall.now, func() hostclock.Status { return w.host })
	w.m = metrics.New(s, config.Retention{}, clk)
	w.e = New(s, &config.Config{ULID: "n-edge1"}, testIDs(), func(string, []byte, bool) {}, w.m, clk)
	w.e.SetSubscriberCheck(func(string, string) bool { return false })
	placeTestElements(t, w.e)
	return w
}

// observeNow hands the engine a parent sample taken with no round trip at the
// world's current time: the parent's clock reads skew ahead of this node's.
func (w *clockWorld) observeNow(skew time.Duration) {
	at := w.wall.now()
	w.e.ObserveParentClock(clock.ParentSample{NowMS: at.Add(skew).UnixMilli(), Sent: at, Received: at})
}

const (
	parentSkewLine      = "colca_clock_parent_skew_seconds"
	parentSampleAgeLine = "colca_clock_parent_sample_age_seconds"
	hostSyncLine        = "colca_host_clock_synchronized"
	hostEstimatedLine   = "colca_host_clock_estimated_error_seconds"
	hostMaxErrorLine    = "colca_host_clock_max_error_seconds"
)

// The skew to the parent is exported as measured, signed, with the age of the
// sample, and only once there is a sample.
func TestParentSkewMetricsFollowTheParentsNowMS(t *testing.T) {
	w := newClockWorld(t, false)
	for _, line := range []string{parentSkewLine, parentSampleAgeLine} {
		if v, ok := metricstest.Lookup(t, w.m, line); ok {
			t.Fatalf("%s = %v before any response from the parent, want the series absent", line, v)
		}
	}

	w.observeNow(7 * time.Second) // the parent is 7s ahead
	if v := metricstest.Value(t, w.m, parentSkewLine); v != 7 {
		t.Fatalf("%s = %v, want 7", parentSkewLine, v)
	}
	if v := metricstest.Value(t, w.m, parentSampleAgeLine); v != 0 {
		t.Fatalf("%s = %v right after the sample, want 0", parentSampleAgeLine, v)
	}

	w.wall.advance(30 * time.Second)
	if v := metricstest.Value(t, w.m, parentSampleAgeLine); v != 30 {
		t.Fatalf("%s = %v, want 30", parentSampleAgeLine, v)
	}
	if v := metricstest.Value(t, w.m, parentSkewLine); v != 7 {
		t.Fatalf("%s = %v, want the last measurement 7 until the next sample", parentSkewLine, v)
	}

	w.observeNow(-250 * time.Millisecond) // the parent is 250ms behind
	if v := metricstest.Value(t, w.m, parentSkewLine); v != -0.25 {
		t.Fatalf("%s = %v, want -0.25", parentSkewLine, v)
	}
}

// A root has no parent: the parent series are never exported there.
func TestParentSkewMetricsAreAbsentOnTheRoot(t *testing.T) {
	w := newClockWorld(t, true)
	w.observeNow(time.Minute)
	for _, line := range []string{parentSkewLine, parentSampleAgeLine} {
		if v, ok := metricstest.Lookup(t, w.m, line); ok {
			t.Fatalf("%s = %v on the root, want the series absent", line, v)
		}
	}
}

// The host series carry the kernel's verdict, and are absent when it cannot be
// read: an unreported clock is neither synchronized nor unsynchronized.
func TestHostClockMetricsCarryTheKernelsVerdict(t *testing.T) {
	w := newClockWorld(t, true)
	for _, line := range []string{hostSyncLine, hostEstimatedLine, hostMaxErrorLine} {
		if v, ok := metricstest.Lookup(t, w.m, line); ok {
			t.Fatalf("%s = %v with an unreported host clock, want the series absent", line, v)
		}
	}

	w.host = hostclock.Status{Reported: true, Synchronized: true, EstimatedError: 1500 * time.Microsecond, MaxError: 250 * time.Millisecond}
	for line, want := range map[string]float64{hostSyncLine: 1, hostEstimatedLine: 0.0015, hostMaxErrorLine: 0.25} {
		if v := metricstest.Value(t, w.m, line); v != want {
			t.Fatalf("%s = %v, want %v", line, v, want)
		}
	}

	w.host = hostclock.Status{Reported: true, Synchronized: false, EstimatedError: 16 * time.Second, MaxError: 16 * time.Second}
	for line, want := range map[string]float64{hostSyncLine: 0, hostEstimatedLine: 16, hostMaxErrorLine: 16} {
		if v := metricstest.Value(t, w.m, line); v != want {
			t.Fatalf("%s = %v, want %v", line, v, want)
		}
	}
}

// A parent whose clock disagrees changes nothing this node decides. A command
// that is live on this node's clock is live, although the parent's clock says
// it expired a minute ago; and it expires when this node's clock passes its
// deadline.
func TestCommandExpiryIsJudgedOnTheNodesOwnClockWhateverTheParentsClockSays(t *testing.T) {
	w := newClockWorld(t, false)
	command := func(corr string, deadline time.Time) []byte {
		b, err := json.Marshal(map[string]any{"correlation_id": corr, "expires_at": deadline.UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	deadline := w.wall.now().Add(10 * time.Second)
	w.observeNow(time.Minute) // parent: 50s past the deadline

	if got := w.e.Now(); !got.Equal(w.wall.now()) {
		t.Fatalf("Now = %v after a parent sample, want the node's own wall clock %v", got, w.wall.now())
	}
	const topic = "colca/v1/_CmdParam/m1/temp/set"
	if _, err := w.e.IngestAdmin(topic, command("live", deadline)); err != nil {
		t.Fatal(err)
	}
	if got := metricstest.Value(t, w.m, undeliveredCounter); got != 1 {
		t.Fatalf("%s = %v, want 1: the command is live on this node's clock and reached no subscriber", undeliveredCounter, got)
	}

	w.wall.advance(11 * time.Second) // this node's clock passes the deadline
	if _, err := w.e.IngestAdmin(topic, command("expired", deadline)); err != nil {
		t.Fatal(err)
	}
	if got := metricstest.Value(t, w.m, undeliveredCounter); got != 1 {
		t.Fatalf("%s = %v, want still 1: a command past its deadline on this node's clock is expired, not undelivered", undeliveredCounter, got)
	}
}

// One log line when the skew passes the alarm threshold, none while it stays
// beyond it, one when it is back.
func TestParentSkewIsLoggedOnTheCrossingAndOnRecoveryOnly(t *testing.T) {
	logs := captureLogs(t)
	w := newClockWorld(t, false)
	const exceeded, recovered = "clock skew to the parent exceeds the alarm threshold", "clock skew to the parent is back within the alarm threshold"
	count := func(marker string) int { return strings.Count(logs.String(), marker) }
	observe := w.observeNow

	observe(4 * time.Second)
	if count(exceeded) != 0 || count(recovered) != 0 {
		t.Fatalf("a skew within the threshold must log nothing:\n%s", logs.String())
	}
	observe(6 * time.Second)
	observe(8 * time.Second)
	observe(-9 * time.Second)
	if count(exceeded) != 1 || count(recovered) != 0 {
		t.Fatalf("want exactly one line for the crossing and none for the samples after it, got %d and %d recoveries:\n%s",
			count(exceeded), count(recovered), logs.String())
	}
	observe(time.Second)
	observe(0)
	if count(exceeded) != 1 || count(recovered) != 1 {
		t.Fatalf("want exactly one recovery line, got %d crossings and %d recoveries:\n%s",
			count(exceeded), count(recovered), logs.String())
	}
	observe(-6 * time.Second)
	if count(exceeded) != 2 {
		t.Fatalf("a second crossing must log again, got %d:\n%s", count(exceeded), logs.String())
	}
}

// The first response after a link that stalled for seconds is late, not skewed:
// both hosts are on one clock and the response sat on the link for 6s. Nothing
// is logged, the metric and the /health block keep the measurement from before
// the stall, and the dropped sample is counted. The same 6s as a real skew over
// a short round trip is logged.
func TestAStalledResponseRaisesNoSkewAlarm(t *testing.T) {
	logs := captureLogs(t)
	w := newClockWorld(t, false)
	const exceeded, recovered = "clock skew to the parent exceeds the alarm threshold", "clock skew to the parent is back within the alarm threshold"
	const discarded = "colca_clock_parent_samples_discarded_total"
	w.observeNow(12 * time.Millisecond)

	sent := w.wall.now()
	const hold, stall = 20 * time.Second, 6 * time.Second
	w.wall.advance(hold + stall)
	w.e.ObserveParentClock(clock.ParentSample{NowMS: sent.Add(hold).UnixMilli(), Held: hold, Sent: sent, Received: w.wall.now()})
	w.observeNow(12 * time.Millisecond) // the next response, on the working link

	if strings.Contains(logs.String(), exceeded) || strings.Contains(logs.String(), recovered) {
		t.Fatalf("a stalled response must not be logged as skew:\n%s", logs.String())
	}
	if v := metricstest.Value(t, w.m, discarded); v != 1 {
		t.Fatalf("%s = %v, want 1", discarded, v)
	}
	if v := metricstest.Value(t, w.m, parentSkewLine); v != 0.012 {
		t.Fatalf("%s = %v, want 0.012", parentSkewLine, v)
	}

	// Between the stalled response and the next one, the measurement from
	// before the stall stands, with its age.
	w2 := newClockWorld(t, false)
	w2.observeNow(12 * time.Millisecond)
	sent = w2.wall.now()
	w2.wall.advance(hold + stall)
	w2.e.ObserveParentClock(clock.ParentSample{NowMS: sent.Add(hold).UnixMilli(), Held: hold, Sent: sent, Received: w2.wall.now()})
	if status := w2.e.ClockStatus(); status["parent_skew_ms"] != int64(12) || status["parent_sample_age_s"] != float64(26) {
		t.Fatalf("/health clock after a stalled response = %v, want the 12ms measured 26s ago", status)
	}
	if v := metricstest.Value(t, w2.m, parentSkewLine); v != 0.012 {
		t.Fatalf("%s = %v after a stalled response, want the previous 0.012", parentSkewLine, v)
	}

	// A parent that is 6s ahead, answered over 40ms.
	sent = w.wall.now()
	w.wall.advance(40 * time.Millisecond)
	w.e.ObserveParentClock(clock.ParentSample{NowMS: sent.Add(6*time.Second + 20*time.Millisecond).UnixMilli(), Sent: sent, Received: w.wall.now()})
	if strings.Count(logs.String(), exceeded) != 1 {
		t.Fatalf("a 6s skew over a 40ms round trip must be logged once:\n%s", logs.String())
	}
	if v := metricstest.Value(t, w.m, parentSkewLine); v != 6 {
		t.Fatalf("%s = %v, want 6", parentSkewLine, v)
	}
}

// The /health clock block has one shape on every node; what is not known is
// null.
func TestClockStatusShape(t *testing.T) {
	keys := func(m map[string]any) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		return out
	}
	sameKeys := func(got map[string]any, want ...string) bool {
		have := map[string]bool{}
		for _, k := range keys(got) {
			have[k] = true
		}
		wanted := map[string]bool{}
		for _, k := range want {
			wanted[k] = true
		}
		return reflect.DeepEqual(have, wanted)
	}

	root := newClockWorld(t, true)
	status := root.e.ClockStatus()
	if !sameKeys(status, "wall_now_ms", "is_root", "host", "parent_skew_ms", "parent_sample_age_s") {
		t.Fatalf("clock block keys = %v", keys(status))
	}
	host := status["host"].(map[string]any)
	if !sameKeys(host, "synchronized", "estimated_error_ms") {
		t.Fatalf("clock.host keys = %v", keys(host))
	}
	if status["wall_now_ms"] != int64(1_700_000_000_000) || status["is_root"] != true {
		t.Fatalf("root clock block = %v", status)
	}
	if status["parent_skew_ms"] != nil || status["parent_sample_age_s"] != nil {
		t.Fatalf("the root has no parent, want null parent fields: %v", status)
	}
	if host["synchronized"] != nil || host["estimated_error_ms"] != nil {
		t.Fatalf("an unreported host clock must be null, never false or 0: %v", host)
	}

	child := newClockWorld(t, false)
	child.host = hostclock.Status{Reported: true, Synchronized: true, EstimatedError: 1500 * time.Microsecond}
	if status := child.e.ClockStatus(); status["parent_skew_ms"] != nil || status["parent_sample_age_s"] != nil {
		t.Fatalf("a child that never heard from its parent must report null parent fields: %v", status)
	}
	child.observeNow(-1250 * time.Millisecond)
	child.wall.advance(4 * time.Second)
	status = child.e.ClockStatus()
	host = status["host"].(map[string]any)
	if status["is_root"] != false || status["wall_now_ms"] != int64(1_700_000_004_000) ||
		status["parent_skew_ms"] != int64(-1_250) || status["parent_sample_age_s"] != float64(4) ||
		host["synchronized"] != true || host["estimated_error_ms"] != 1.5 {
		t.Fatalf("child clock block = %v", status)
	}
}

// _TimeSync is no contract any more. Its old four-segment topic is refused by
// the grammar at every door, like any other four-segment topic, and a
// five-segment one names an unknown contract.
func TestTheRetiredTimeSyncTopicFailsTheGrammarAtEveryDoor(t *testing.T) {
	w := newClockWorld(t, true)
	const grammarLine = `colca_rejected_publishes_total{reason="grammar"}`
	before := metricstest.Value(t, w.m, grammarLine)
	payload := []byte(`{"now_ms":1}`)

	rejections := 0
	for _, topic := range []string{"colca/v1/_TimeSync/x", "colca/v1/_Metric/x", "colca/v1/_TimeSync/n-edge1/extra"} {
		if _, err := w.e.IngestClient("m1", topic, payload); ReasonOf(err) != metrics.ReasonGrammar {
			t.Fatalf("client publish of %s: reason %q (%v), want grammar", topic, ReasonOf(err), err)
		}
		// The admin and human doors answer an untyped error; the reason they count
		// is asserted on the metric below.
		if _, err := w.e.IngestAdmin(topic, payload); err == nil {
			t.Fatalf("admin publish of %s was accepted", topic)
		}
		if _, err := w.e.IngestHuman(humanEntry(t, "read:#", "cmd:#:admin", "admin:#"), topic, payload); err == nil {
			t.Fatalf("human publish of %s was accepted", topic)
		}
		rejections += 3
	}
	if got := metricstest.Value(t, w.m, grammarLine); got != before+float64(rejections) {
		t.Fatalf("%s = %v, want %v", grammarLine, got, before+float64(rejections))
	}
	if _, ok := metricstest.Lookup(t, w.m, `colca_rejected_publishes_total{reason="time_sync"}`); ok {
		t.Fatal("the time_sync rejection reason must not be exported any more")
	}
}

// _TimeSync was never stored, so no stream of an older child carries one. A
// batch that names the topic anyway does not hold the lane: the records around
// it apply and the HWM passes it.
func TestAReplicatedBatchNamingTheRetiredTimeSyncTopicDoesNotHoldTheLane(t *testing.T) {
	w := newClockWorld(t, true)
	recs := []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/child1/m1/a", Payload: []byte(`{"v":1}`), TS: 1, KVPath: "child1/m1/a", KVNode: "m1"},
		{ChildOffset: 2, Topic: "colca/v1/_TimeSync/n-child", Payload: []byte(`{"now_ms":1}`), TS: 1},
		{ChildOffset: 3, Topic: "colca/v1/_Metric/m1/child1/m1/b", Payload: []byte(`{"v":2}`), TS: 2, KVPath: "child1/m1/b", KVNode: "m1"},
	}
	_, hwm, err := w.e.IngestReplicated("n-child", "metrics", recs)
	if err != nil {
		t.Fatal(err)
	}
	if hwm != 3 {
		t.Fatalf("hwm = %d, want 3: the lane must move past the record", hwm)
	}
	for _, path := range []string{"child1/m1/a", "child1/m1/b"} {
		if got := mustKVScan(t, w.e.Store(), path); len(got) != 1 {
			t.Fatalf("KV %s: %d entries, want 1: the records around the retired topic must apply", path, len(got))
		}
	}
}

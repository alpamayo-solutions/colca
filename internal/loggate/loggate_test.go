package loggate

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

const (
	pressError = "colca/v1/_Log/n1/line1/press/ERROR"
	pressInfo  = "colca/v1/_Log/n1/line1/press/INFO"
	otherInfo  = "colca/v1/_Log/n1/line1/oven/INFO"
)

var t0 = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func logPayload(message string, extra map[string]any) []byte {
	body := map[string]any{
		"timestamp": t0.Format(time.RFC3339Nano), "level": "ERROR", "message": message,
		"logger_name": "press.driver", "module": "driver", "function": "connect", "line_no": 42,
	}
	if extra != nil {
		body["extra"] = extra
	}
	raw, _ := json.Marshal(body)
	return raw
}

func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return body
}

func newGate(window time.Duration, perService, tracked int) *Gate[string] {
	return New(Config{Window: window, MaxPerService: perService, MaxTracked: tracked}, "node")
}

func TestRepeatsCollapseIntoOneSummaryWhenTheWindowEnds(t *testing.T) {
	g := newGate(time.Minute, 0, 16)
	if v, _, w := g.Admit(t0, pressError, logPayload("connection refused", nil), "svc"); v != Store || len(w) != 0 {
		t.Fatalf("first record: verdict %v writes %d, want stored and nothing else", v, len(w))
	}
	for i := 1; i < 1000; i++ {
		at := t0.Add(time.Duration(i) * 50 * time.Millisecond)
		if v, _, _ := g.Admit(at, pressError, logPayload("connection refused", map[string]any{"attempt": i}), "svc"); v != Collapsed {
			t.Fatalf("record %d: verdict %v, want collapsed", i, v)
		}
	}
	if w := g.Due(t0.Add(59 * time.Second)); len(w) != 0 {
		t.Fatalf("before the window ends: %d writes, want none", len(w))
	}
	writes := g.Due(t0.Add(time.Minute))
	if len(writes) != 1 {
		t.Fatalf("window end: %d writes, want one summary", len(writes))
	}
	s := writes[0]
	if s.Topic != pressError || s.Author != "svc" || s.Notice {
		t.Fatalf("summary %+v: want the original topic and writer", s)
	}
	body := decode(t, s.Payload)
	if body["message"] != "connection refused (×999 in 60 s)" {
		t.Fatalf("message %q", body["message"])
	}
	extra := body["extra"].(map[string]any)
	if extra["repeated"] != float64(999) || extra["repeat_window_s"] != float64(60) {
		t.Fatalf("extra %v", extra)
	}
	if extra["attempt"] != float64(999) {
		t.Fatalf("the summary must carry the last withheld record's payload, got extra %v", extra)
	}
	if extra["first_repeat_at"] != t0.Add(50*time.Millisecond).Format(time.RFC3339Nano) ||
		extra["last_repeat_at"] != t0.Add(999*50*time.Millisecond).Format(time.RFC3339Nano) {
		t.Fatalf("repeat times %v / %v", extra["first_repeat_at"], extra["last_repeat_at"])
	}
	if body["line_no"] != float64(42) || body["logger_name"] != "press.driver" {
		t.Fatalf("the other fields must be kept: %v", body)
	}
	if r, _ := g.Tracked(); r != 0 {
		t.Fatalf("%d repeat keys still held after the window ended", r)
	}
	// The next window starts fresh: its first record is stored.
	if v, _, _ := g.Admit(t0.Add(61*time.Second), pressError, logPayload("connection refused", nil), "svc"); v != Store {
		t.Fatalf("first record of the next window: %v, want stored", v)
	}
}

func TestAWindowWithoutRepeatsWritesNothing(t *testing.T) {
	g := newGate(time.Minute, 0, 16)
	g.Admit(t0, pressError, logPayload("once", nil), "svc")
	if w := g.Due(t0.Add(time.Hour)); len(w) != 0 {
		t.Fatalf("%d writes for a record that never repeated", len(w))
	}
}

func TestTheKeyIsTopicLoggerAndMessage(t *testing.T) {
	g := newGate(time.Minute, 0, 16)
	g.Admit(t0, pressError, logPayload("a", nil), "svc")
	for name, rec := range map[string]struct {
		topic   string
		payload []byte
	}{
		"another message": {pressError, logPayload("b", nil)},
		"another level":   {pressInfo, logPayload("a", nil)},
		"another logger":  {pressError, []byte(`{"message":"a","logger_name":"other"}`)},
	} {
		if v, _, _ := g.Admit(t0.Add(time.Second), rec.topic, rec.payload, "svc"); v != Store {
			t.Fatalf("%s: verdict %v, want stored", name, v)
		}
	}
}

func TestALateRecordEndsItsOwnWindowBeforeTheTimerDoes(t *testing.T) {
	g := newGate(time.Minute, 0, 16)
	g.Admit(t0, pressError, logPayload("x", nil), "svc")
	g.Admit(t0.Add(time.Second), pressError, logPayload("x", nil), "svc")
	v, _, writes := g.Admit(t0.Add(2*time.Minute), pressError, logPayload("x", nil), "svc")
	if v != Store || len(writes) != 1 {
		t.Fatalf("verdict %v with %d writes, want stored after the previous window's summary", v, len(writes))
	}
	if msg := decode(t, writes[0].Payload)["message"]; msg != "x (×1 in 60 s)" {
		t.Fatalf("summary %q", msg)
	}
	// The ended window's timer entry must not write it again.
	if w := g.Due(t0.Add(90 * time.Second)); len(w) != 0 {
		t.Fatalf("stale deadline wrote %d records", len(w))
	}
}

func TestAServiceOverItsBudgetIsCappedAndToldOnce(t *testing.T) {
	g := newGate(time.Minute, 600, 4096)
	stored, dropped := 0, 0
	for i := range 2434 {
		v, _, _ := g.Admit(t0.Add(time.Duration(i)*time.Millisecond), pressInfo, logPayload(fmt.Sprintf("line %d", i), nil), "svc")
		switch v {
		case Store:
			stored++
		case RateLimited:
			dropped++
		default:
			t.Fatalf("record %d: %v", i, v)
		}
	}
	if stored != 600 || dropped != 1834 {
		t.Fatalf("stored %d dropped %d, want 600 and 1834", stored, dropped)
	}
	// Another service keeps its own budget.
	if v, _, _ := g.Admit(t0.Add(3*time.Second), otherInfo, logPayload("fine", nil), "oven"); v != Store {
		t.Fatalf("another service: %v, want stored", v)
	}
	writes := g.Due(t0.Add(time.Minute))
	if len(writes) != 1 {
		t.Fatalf("%d writes at the window end, want one drop notice", len(writes))
	}
	n := writes[0]
	if n.Topic != "colca/v1/_Log/n1/line1/press/WARNING" || n.Author != "node" || !n.Notice {
		t.Fatalf("notice %+v", n)
	}
	body := decode(t, n.Payload)
	if body["message"] != "1834 log record(s) dropped: press exceeded 600 records in 60 s" || body["level"] != "WARNING" {
		t.Fatalf("notice body %v", body)
	}
	extra := body["extra"].(map[string]any)
	if extra["dropped"] != float64(1834) || extra["window_s"] != float64(60) {
		t.Fatalf("notice extra %v", extra)
	}
	for _, field := range []string{"timestamp", "level", "message", "logger_name", "module", "function", "line_no"} {
		if _, ok := body[field]; !ok {
			t.Fatalf("the notice lacks required _Log field %q: %v", field, body)
		}
	}
}

func TestRepeatsDoNotSpendTheBudget(t *testing.T) {
	g := newGate(time.Minute, 2, 16)
	for range 50 {
		g.Admit(t0, pressError, logPayload("same", nil), "svc")
	}
	if v, _, _ := g.Admit(t0, pressError, logPayload("different", nil), "svc"); v != Store {
		t.Fatalf("a withheld repeat spent the budget: %v", v)
	}
}

func TestFullTablesLetRecordsPastAndSayWhy(t *testing.T) {
	g := newGate(time.Minute, 0, 2)
	g.Admit(t0, pressError, logPayload("a", nil), "svc")
	g.Admit(t0, pressError, logPayload("b", nil), "svc")
	for range 3 {
		v, untracked, _ := g.Admit(t0, pressError, logPayload("c", nil), "svc")
		if v != Store || untracked != UntrackedRepeat {
			t.Fatalf("verdict %v untracked %v, want stored and UntrackedRepeat", v, untracked)
		}
	}
	if r, _ := g.Tracked(); r != 2 {
		t.Fatalf("%d repeat keys held, want the bound 2", r)
	}

	capped := newGate(time.Minute, 1, 1)
	capped.Admit(t0, pressInfo, logPayload("a", nil), "svc")
	v, untracked, _ := capped.Admit(t0, otherInfo, logPayload("a", nil), "svc")
	if v != Store || untracked != UntrackedService {
		t.Fatalf("second service with a full table: %v %v", v, untracked)
	}
	if _, s := capped.Tracked(); s != 1 {
		t.Fatalf("%d services held, want the bound 1", s)
	}
}

func TestCloseFlushesEverythingAndThenPasses(t *testing.T) {
	g := newGate(time.Hour, 1, 16)
	g.Admit(t0, pressError, logPayload("x", nil), "svc")
	g.Admit(t0, pressError, logPayload("x", nil), "svc")
	g.Admit(t0, pressError, logPayload("y", nil), "svc") // over the budget of 1
	writes := g.Close()
	if len(writes) != 2 {
		t.Fatalf("Close wrote %d, want the summary and the drop notice", len(writes))
	}
	if v, _, _ := g.Admit(t0, pressError, logPayload("x", nil), "svc"); v != Store {
		t.Fatalf("after Close: %v, want every record stored", v)
	}
	if _, ok := g.NextDeadline(); ok {
		t.Fatal("a closed gate holds no deadline")
	}
}

func TestAZeroWindowTurnsTheGateOff(t *testing.T) {
	g := newGate(0, 1, 16)
	for range 5 {
		if v, _, _ := g.Admit(t0, pressError, logPayload("x", nil), "svc"); v != Store {
			t.Fatalf("window 0: %v, want stored", v)
		}
	}
}

func TestUnreadablePayloadsAreCappedButNotCollapsed(t *testing.T) {
	g := newGate(time.Minute, 2, 16)
	for i, want := range []Verdict{Store, Store, RateLimited} {
		if v, _, _ := g.Admit(t0, pressError, []byte(`not json`), "svc"); v != want {
			t.Fatalf("record %d: %v, want %v", i, v, want)
		}
	}
}

func TestNextDeadlineAndWake(t *testing.T) {
	g := newGate(time.Minute, 0, 16)
	if _, ok := g.NextDeadline(); ok {
		t.Fatal("an empty gate has no deadline")
	}
	g.Admit(t0, pressError, logPayload("x", nil), "svc")
	select {
	case <-g.Wake():
	default:
		t.Fatal("the first deadline must wake the runner")
	}
	at, ok := g.NextDeadline()
	if !ok || !at.Equal(t0.Add(time.Minute)) {
		t.Fatalf("deadline %v %v", at, ok)
	}
	g.Admit(t0.Add(time.Second), pressError, logPayload("y", nil), "svc")
	select {
	case <-g.Wake():
		t.Fatal("a later deadline must not wake the runner")
	default:
	}
}

func TestSummaryMessageFormat(t *testing.T) {
	raw, err := summaryPayload(logPayload("boom", nil), 3, 1500*time.Millisecond, t0, t0)
	if err != nil {
		t.Fatal(err)
	}
	if msg := decode(t, raw)["message"].(string); !strings.HasSuffix(msg, "(×3 in 1.5 s)") {
		t.Fatalf("message %q", msg)
	}
}

func TestLargeRecordsAreNotHeldForCollapse(t *testing.T) {
	g := newGate(time.Minute, 0, 16)
	big := logPayload(strings.Repeat("x", MaxCollapsiblePayload), nil)
	for range 3 {
		if v, _, _ := g.Admit(t0, pressError, big, "svc"); v != Store {
			t.Fatalf("a record over MaxCollapsiblePayload: %v, want stored", v)
		}
	}
	if r, _ := g.Tracked(); r != 0 {
		t.Fatalf("%d repeat keys held for records too large to collapse", r)
	}
}

func TestTheHeldPayloadIsACopy(t *testing.T) {
	g := newGate(time.Minute, 0, 16)
	g.Admit(t0, pressError, logPayload("x", nil), "svc")
	buf := logPayload("x", nil)
	g.Admit(t0, pressError, buf, "svc")
	for i := range buf {
		buf[i] = ' '
	}
	writes := g.Close()
	if len(writes) != 1 || decode(t, writes[0].Payload)["message"] != "x (×1 in 60 s)" {
		t.Fatalf("summary built from a reused buffer: %v", writes)
	}
}

func TestAnInfoFloodDoesNotCrowdOutTheError(t *testing.T) {
	g := newGate(time.Minute, 10, 16)
	for i := range 50 {
		g.Admit(t0, pressInfo, logPayload(fmt.Sprintf("debug %d", i), nil), "svc")
	}
	if v, _, _ := g.Admit(t0, pressError, logPayload("PLC connection lost", nil), "svc"); v != Store {
		t.Fatalf("the error after an INFO flood: %v, want stored", v)
	}
	// Errors have a budget of their own, bounded too.
	for i := range 20 {
		g.Admit(t0, pressError, logPayload(fmt.Sprintf("error %d", i), nil), "svc")
	}
	writes := g.Close()
	if len(writes) != 1 {
		t.Fatalf("%d writes, want one drop notice", len(writes))
	}
	body := decode(t, writes[0].Payload)
	if body["message"] != "51 log record(s) dropped: press exceeded 10 records in 60 s, 11 of them at WARNING or above" {
		t.Fatalf("notice %q", body["message"])
	}
	if extra := body["extra"].(map[string]any); extra["dropped"] != float64(51) || extra["dropped_warning_and_above"] != float64(11) {
		t.Fatalf("notice extra %v", extra)
	}
}

func TestHeldPayloadsStayWithinTheByteBudget(t *testing.T) {
	g := newGate(time.Minute, 0, 4096)
	pad := strings.Repeat("x", MaxCollapsiblePayload-300)
	payload := func(i int) []byte { return logPayload(fmt.Sprintf("%d %s", i, pad), nil) }
	keys := MaxHeldBytes/len(payload(0)) + 50
	untracked := 0
	for i := range keys {
		if v, _, _ := g.Admit(t0, pressError, payload(i), "svc"); v != Store {
			t.Fatalf("first record of key %d: %v", i, v)
		}
		v, u, _ := g.Admit(t0, pressError, payload(i), "svc")
		switch {
		case v == Collapsed:
		case v == Store && u == UntrackedRepeat:
			untracked++
		default:
			t.Fatalf("repeat of key %d: %v %v", i, v, u)
		}
	}
	if g.held > MaxHeldBytes {
		t.Fatalf("held %d bytes, over the budget of %d", g.held, MaxHeldBytes)
	}
	if untracked == 0 {
		t.Fatal("past the byte budget a repeat must be stored, not held")
	}
	g.Close()
	if g.held != 0 {
		t.Fatalf("%d bytes still counted after every window ended", g.held)
	}
}

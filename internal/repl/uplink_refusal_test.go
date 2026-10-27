package repl

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A batch the parent answered and refused is held, counted as a refusal and
// logged as one, not as "the parent is down". Permanent refusals, such as a
// record on a stream it may not travel on or one larger than the parent's limit,
// are not fixed by waiting. Holding instead of skipping is deliberate; see
// pushOnce in RunUplink.
func TestARefusedUplinkBatchIsHeldAndNamedARefusal(t *testing.T) {
	logs := captureLogs(t)
	f := newParentFixture(t)

	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cm := metrics.New(cs, config.Retention{}, nil)

	// The denominator first: an ordinary metric, which must rise.
	if _, _, err := cs.Append("metrics", []store.Record{
		{Topic: "colca/v1/_Metric/n-child/temp", Payload: []byte(`{"v":1}`), TS: 1},
	}); err != nil {
		t.Fatal(err)
	}
	cl := mustClient(t, f.addr, f.pid.PublicHex(), f.cid)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunUplink(cl, ceng, nil, cm, stop) }()
	waitFor(t, "the ordinary metric to reach the parent", 10*time.Second, func() bool {
		return f.ps.NextOffset("metrics") == 2
	})

	// Then a record the parent will refuse on this stream, since a command never
	// rises. Writing it straight into the stream is what a buggy or half-upgraded
	// child amounts to.
	if _, _, err := cs.Append("metrics", []store.Record{
		{Topic: "colca/v1/_CmdParam/n-child/m1/go",
			Payload: []byte(`{"correlation_id":"c1","expires_at":99999999999}`), TS: 2},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the parent to refuse it", 10*time.Second, func() bool {
		return metricstest.Value(t, cm, `colca_uplink_refused_total{stream="metrics"}`) >= 1
	})
	close(stop)
	waitForClosed(t, "RunUplink to stop", done, 5*time.Second)

	// The hold: nothing more reached the parent, and the cursor stopped
	// exactly at the refused record rather than stepping over it.
	if got := f.ps.NextOffset("metrics"); got != 2 {
		t.Fatalf("parent metrics head = %d, want 2: only the record before the refused one", got)
	}
	if pos := cs.CursorGet(uns.UplinkCursor(cl.ParentPub()), "metrics"); pos != 2 {
		t.Fatalf("uplink cursor = %d, want 2 — a refused record must be held, never skipped", pos)
	}
	if got := metricstest.Value(t, cm, `colca_uplink_push_failures_total{stream="metrics"}`); got < 1 {
		t.Fatalf("colca_uplink_push_failures_total = %v, want the failure counted as well", got)
	}

	out := logs.String()
	if !strings.Contains(out, "uplink refused by the parent") {
		t.Fatalf("no refusal line in the log:\n%s", out)
	}
	if strings.Contains(out, "uplink is down") {
		t.Fatalf("a refusal was reported as an outage:\n%s", out)
	}
	if strings.Contains(out, "has not enrolled this node yet") {
		t.Fatalf("a refusal was blamed on enrollment, which the parent never said:\n%s", out)
	}
	if !strings.Contains(out, "may not replicate upward") {
		t.Fatalf("the parent's own explanation is missing from the line:\n%s", out)
	}
}

// One record the parent refuses for good holds its lane only for the grace.
// The records before it in the same batch rise at once (the refused batch is
// sent again at half its size until the refused record stands alone), the
// refused one is then set aside with a copy, a finding, a counter and an error
// that say it did not reach the parent, and the records after it rise.
// Clearing the copy retires the finding.
func TestARecordRefusedAlonePastTheGraceIsSetAsideAndTheLaneMovesOn(t *testing.T) {
	logs := captureLogs(t)
	f := newParentFixture(t)

	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	ceng.SetContracts(findingBundle(t))
	cm := metrics.New(cs, config.Retention{}, nil)

	// Before, the refused record (a command never rises), and after, in one page.
	if _, _, err := cs.Append("metrics", []store.Record{
		{Topic: "colca/v1/_Metric/n-child/temp", Payload: []byte(`{"v":1}`), TS: 1},
		{Topic: "colca/v1/_Metric/n-child/temp", Payload: []byte(`{"v":2}`), TS: 2},
		{Topic: "colca/v1/_CmdParam/n-child/m1/go", Payload: []byte(`{"correlation_id":"c1","expires_at":99999999999}`), TS: 3},
		{Topic: "colca/v1/_Metric/n-child/temp", Payload: []byte(`{"v":4}`), TS: 4},
	}); err != nil {
		t.Fatal(err)
	}
	cl := mustClient(t, f.addr, f.pid.PublicHex(), f.cid)
	cl.setAsideGrace = 2 * time.Second
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunUplink(cl, ceng, nil, cm, stop) }()
	defer func() {
		close(stop)
		waitForClosed(t, "RunUplink to stop", done, 5*time.Second)
	}()
	up := uns.UplinkCursor(cl.ParentPub())

	// Isolation: the two records before the refused one reach the parent while
	// it is still held.
	waitFor(t, "the records before the refused one to reach the parent", 10*time.Second, func() bool {
		return f.ps.NextOffset("metrics") == 3
	})
	if pos := cs.CursorGet(up, "metrics"); pos != 3 {
		t.Fatalf("uplink cursor = %d during the grace, want 3: the refused record is held", pos)
	}

	// Past the grace it is set aside and the record after it rises.
	waitFor(t, "the record after the refused one to reach the parent", 15*time.Second, func() bool {
		return f.ps.NextOffset("metrics") == 4
	})
	if pos := cs.CursorGet(up, "metrics"); pos != 5 {
		t.Fatalf("uplink cursor = %d, want 5: past the set-aside record and the one after it", pos)
	}
	if got := metricstest.Value(t, cm, `colca_uplink_set_aside_total{stream="metrics"}`); got != 1 {
		t.Fatalf("colca_uplink_set_aside_total = %v, want 1", got)
	}
	aside, err := cl.SetAsideRecords(cs)
	if err != nil {
		t.Fatal(err)
	}
	if len(aside) != 1 || aside[0].Offset != 3 || aside[0].Stream != "metrics" ||
		aside[0].Record.Topic != "colca/v1/_CmdParam/n-child/m1/go" || aside[0].Status == 0 {
		t.Fatalf("set-aside copies = %+v, want the refused command at offset 3 with the parent's status", aside)
	}
	finding := SetAsideFindingTopic("n-child")
	p, _ := uns.Parse(finding)
	entry, ok, err := cs.KVGet(p.Path, p.NodeID, finding)
	if err != nil || !ok {
		t.Fatalf("no uplink_set_aside finding (err %v)", err)
	}
	if !strings.Contains(string(entry.Payload), `"offset":3`) || !strings.Contains(string(entry.Payload), "NOT replicated") {
		t.Fatalf("the finding does not name the record or the loss: %s", entry.Payload)
	}
	if out := logs.String(); !strings.Contains(out, "uplink record set aside") || !strings.Contains(out, "NOT replicated") {
		t.Fatalf("no error line saying the record was not replicated:\n%s", out)
	}

	// Clearing the copy is the operator's decision; the finding goes with the last one.
	if found, err := cl.ClearSetAside(ceng, "metrics", 3); err != nil || !found {
		t.Fatalf("ClearSetAside = %v, %v", found, err)
	}
	if _, ok, _ := cs.KVGet(p.Path, p.NodeID, finding); ok {
		t.Fatal("the finding still stands after the last copy was cleared")
	}
	if aside, _ := cl.SetAsideRecords(cs); len(aside) != 0 {
		t.Fatalf("copies after clearing: %+v", aside)
	}
}

// A refused batch shrinks while it is refused and grows back once accepted.
func TestLaneHoldHalvesToTheRefusedRecordAndGrowsBack(t *testing.T) {
	batch := func(from, n int) []store.ReplRecord {
		out := make([]store.ReplRecord, n)
		for i := range out {
			out[i].ChildOffset = uint64(from + i) //nolint:gosec // small test offsets
		}
		return out
	}
	var h laneHold
	now := time.Unix(1000, 0)
	if h.refused(batch(1, 100), now, time.Minute) || h.limit != 50 {
		t.Fatalf("after a refused 100: limit %d, want 50", h.limit)
	}
	if h.refused(batch(1, 50), now, time.Minute) || h.limit != 25 {
		t.Fatalf("after a refused 50: limit %d, want 25", h.limit)
	}
	if h.refused(batch(7, 1), now, time.Minute) || h.offset != 7 {
		t.Fatalf("a single refused record must be held first: %+v", h)
	}
	if h.refused(batch(7, 1), now.Add(30*time.Second), time.Minute) {
		t.Fatal("set aside inside the grace")
	}
	if !h.refused(batch(7, 1), now.Add(time.Minute), time.Minute) {
		t.Fatal("not set aside once the grace is over")
	}
	skip := []store.ReplRecord{{ChildOffset: 9, SkipFrom: 8}}
	h = laneHold{}
	h.refused(skip, now, time.Minute)
	if h.refused(skip, now.Add(time.Hour), time.Minute) {
		t.Fatal("a skip range is not a record and is never set aside")
	}
	h = laneHold{limit: 2}
	h.accepted()
	if h.limit != 4 || h.offset != 0 {
		t.Fatalf("after an accepted batch: %+v, want limit 4", h)
	}
	h = laneHold{limit: replBatch / 2}
	h.accepted()
	if h.limit != 0 {
		t.Fatalf("limit %d, want back to no limit", h.limit)
	}
}

// findingBundle is a minimal loadable bundle that declares _Finding, the one
// contract the uplink writes through the engine.
func findingBundle(t *testing.T) *contracts.Table {
	t.Helper()
	body := map[string]any{
		"bundle_version": "test",
		"source":         map[string]any{"package": "colca-data-contracts", "git_sha": "fixture"},
		"contracts": map[string]any{"_Finding": map[string]any{
			"class": "entity", "tombstone": true,
			"schema": map[string]any{"type": "object", "properties": map[string]any{}},
		}},
	}
	canon, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	body["digest"], body["generated_at"] = hex.EncodeToString(sum[:]), "2026-10-27T00:00:00Z"
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	table, err := contracts.Load(path, "")
	if err != nil {
		t.Fatal(err)
	}
	return table
}

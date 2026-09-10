package repl

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
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

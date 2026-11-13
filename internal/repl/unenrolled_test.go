package repl

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/metrics/metricstest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The wait of a node its parent does not know doubles up to the bound, is not
// extended by a refusal that arrives while it runs, and ends with the first
// answer.
func TestUnenrolledBackoffDoublesToItsBoundAndEndsWhenAccepted(t *testing.T) {
	var b unenrolledBackoff
	now := time.Unix(1_800_000_000, 0)
	if w := b.wait(now); w != 0 {
		t.Fatalf("wait before any refusal = %v", w)
	}
	var waits []time.Duration
	for range 8 {
		b.refused(now)
		w := b.wait(now)
		waits = append(waits, w)
		b.refused(now.Add(w / 2)) // another lane's refusal, inside the wait
		if again := b.wait(now); again != w {
			t.Fatalf("a refusal inside the wait moved it from %v to %v", w, again)
		}
		now = now.Add(w)
	}
	if waits[0] < unenrolledRetryMin/2 || waits[0] > unenrolledRetryMin {
		t.Fatalf("first wait %v, want between %v and %v", waits[0], unenrolledRetryMin/2, unenrolledRetryMin)
	}
	last := waits[len(waits)-1]
	if last < unenrolledRetryMax/2 || last > unenrolledRetryMax {
		t.Fatalf("wait after %d refusals %v, want between %v and %v", len(waits), last, unenrolledRetryMax/2, unenrolledRetryMax)
	}
	b.accepted()
	if w := b.wait(now); w != 0 {
		t.Fatalf("wait after the parent answered = %v", w)
	}
	b.refused(now)
	if w := b.wait(now); w > unenrolledRetryMin {
		t.Fatalf("first wait after a recovery = %v, want at most %v", w, unenrolledRetryMin)
	}
}

// A node that runs before its parent enrolled it used to ask on every lane
// twice a second, and the parent wrote an audit record for each refusal. It
// now backs off, the parent writes one record under the key that knocked, and
// the node is connected within the backoff's bound once it is enrolled.
func TestAnUnenrolledChildBacksOffAndIsAuditedOnce(t *testing.T) {
	dir := t.TempDir()
	parentID := mustIdentity(t, filepath.Join(dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(dir, "c.key"))

	ps := mustStore(t, filepath.Join(dir, "pdata"))
	pcfg := &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	pm := metrics.New(ps, config.Retention{}, nil)
	preg, peng := nodeParts(t, ps, pcfg, nil, pm, nil)
	srv, addr := startServerWithMetrics(t, pcfg, peng, parentID, preg, pm)
	defer srv.Stop()

	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	// Something to push on two lanes, so the uplink asks as well.
	placeElement(t, ceng, "m1")
	if _, err := ceng.IngestAdmin("colca/v1/_Metric/n-child/m1/temp", []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	cl := mustClient(t, addr, parentID.PublicHex(), childID)

	stop := make(chan struct{})
	downDone, upDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(downDone); RunDownlink(cl, ceng, nil, stop) }()
	go func() { defer close(upDone); RunUplink(cl, ceng, nil, nil, stop) }()
	t.Cleanup(func() {
		close(stop)
		waitForClosed(t, "RunDownlink to return after stop", downDone, 5*time.Second)
		waitForClosed(t, "RunUplink to return after stop", upDone, 5*time.Second)
	})

	const window = 6 * time.Second
	time.Sleep(window)
	const rejected = `colca_auth_rejections_total{door="repl",reason="unknown_key"}`
	refusals := metricstest.Value(t, pm, rejected)
	if refusals < 2 {
		t.Fatalf("the parent refused %v requests in %s: the child is not asking, so the bound below proves nothing", refusals, window)
	}
	// Waits of 0.5-1 s, 1-2 s, 2-4 s: at most five rounds fit in six seconds,
	// each one request per loop. Without the backoff it is about 25.
	if refusals > 12 {
		t.Fatalf("the parent refused %v requests in %s, want at most 12: the child does not back off", refusals, window)
	}
	recs, _, err := ps.Read("audit", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("the parent wrote %d audit records for %v refusals, want 1", len(recs), refusals)
	}
	var event map[string]any
	if err := json.Unmarshal(recs[0].Payload, &event); err != nil {
		t.Fatal(err)
	}
	want := "unknown key " + identity.Fingerprint(childID.PublicHex())
	if event["actor_label"] != want || event["actor_kind"] != "anonymous" || event["reason_code"] != "unknown_key" {
		t.Fatalf("audit record names %v (%v, %v), want %q", event["actor_label"], event["actor_kind"], event["reason_code"], want)
	}
	if suppressed := metricstest.Value(t, pm, `colca_audit_denials_suppressed_total{door="repl",reason="unknown_key"}`); suppressed != refusals-1 {
		t.Fatalf("suppressed counter %v, want %v (every refusal but the first)", suppressed, refusals-1)
	}

	element := placeElement(t, peng, "child1")
	entry, err := json.Marshal(uns.Entry{ULID: "n-child", Pubkey: childID.PublicHex(), Kind: uns.KindNode, Element: element})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := preg.Enroll(entry); err != nil {
		t.Fatalf("enroll child: %v", err)
	}
	waitFor(t, "the child to connect once the parent enrolled it", unenrolledRetryMax+5*time.Second,
		func() bool { return cl.Status().State == UplinkConnected })
	waitFor(t, "the child's record to reach the parent", unenrolledRetryMax+5*time.Second,
		func() bool { return ps.NextOffset("metrics") > 1 })
}

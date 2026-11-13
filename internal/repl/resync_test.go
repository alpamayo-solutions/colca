package repl

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// What a parent does with a child's request, for every combination of the four
// facts it decides from.
func TestJudgeChild(t *testing.T) {
	const current = "store-b"
	for _, c := range []struct {
		name     string
		capable  bool
		recorded string
		ahead    bool
		want     childVerdict
	}{
		{"same incarnation, nothing ahead", true, current, false, childProceed},
		{"same incarnation, a position ahead: this store regressed", true, current, true, childReincarnate},
		{"another incarnation, nothing ahead", true, "store-a", false, childResync},
		{"another incarnation, a position ahead: settled when the id changed", true, "store-a", true, childResync},
		{"no incarnation recorded yet, nothing ahead", true, "", false, childProceed},
		{"no incarnation recorded yet, a position ahead", true, "", true, childResync},
		{"a child from before the handshake", false, "", false, childProceed},
		{"a child from before the handshake, a position ahead", false, "", true, childCannotResync},
	} {
		if got := judgeChild(current, c.capable, c.recorded, c.ahead); got != c.want {
			t.Errorf("%s: verdict %d, want %d", c.name, got, c.want)
		}
	}
}

// What a child does with its parent's answer.
func TestJudgeParent(t *testing.T) {
	for _, c := range []struct {
		name               string
		recorded, answered string
		asked              bool
		want               parentVerdict
	}{
		{"same incarnation", "store-a", "store-a", false, parentUnchanged},
		{"a new incarnation", "store-a", "store-b", false, parentChanged},
		{"same incarnation, the parent asks", "store-a", "store-a", true, parentChanged},
		{"first incarnation seen", "", "store-a", false, parentFirstSeen},
		{"first incarnation seen, the parent asks", "", "store-a", true, parentChanged},
		{"a parent from before incarnations", "store-a", "", false, parentPredatesIncarnations},
		{"a parent from before incarnations, nothing recorded", "", "", false, parentPredatesIncarnations},
	} {
		if got := judgeParent(c.recorded, c.answered, c.asked); got != c.want {
			t.Errorf("%s: verdict %d, want %d", c.name, got, c.want)
		}
	}
}

// Where a re-sync places one uplink cursor.
func TestPlanReplay(t *testing.T) {
	gap := func(from, to uint64) *ResyncGap { return &ResyncGap{Stream: "metrics", FromOffset: from, ToOffset: to} }
	for _, c := range []struct {
		name               string
		cursor, hwm, lwm   uint64
		wantRewind, wantTo uint64
		wantGap            *ResyncGap
	}{
		{"the parent holds everything sent", 41, 40, 1, 0, 0, nil},
		{"the parent holds more than the cursor says (a lost answer)", 41, 45, 1, 0, 0, nil},
		{"the cursor moved past records that are never pushed", 41, 30, 1, 31, 41, nil},
		{"the parent lacks a range still retained", 41, 20, 5, 21, 41, nil},
		{"the parent lacks a range partly pruned", 41, 20, 30, 30, 41, gap(21, 29)},
		{"the parent lacks a range fully pruned", 41, 20, 60, 0, 41, gap(21, 40)},
		{"a parent that holds nothing, the stream retained from its start", 41, 0, 1, 1, 41, nil},
		{"a parent that holds nothing, the start pruned", 41, 0, 10, 10, 41, gap(1, 9)},
	} {
		got := planReplay("metrics", c.cursor, c.hwm, c.lwm)
		if got.rewindTo != c.wantRewind || got.until != c.wantTo || !reflect.DeepEqual(got.gap, c.wantGap) {
			t.Errorf("%s: rewind to %d until %d gap %+v, want %d, %d, %+v",
				c.name, got.rewindTo, got.until, got.gap, c.wantRewind, c.wantTo, c.wantGap)
		}
	}
}

// handedLog counts the commands a node put on its bus, per correlation id.
type handedLog struct {
	mu   sync.Mutex
	seen map[string]int
}

func (h *handedLog) deliver(topic string, payload []byte, _ bool) {
	p, err := uns.Parse(topic)
	if err != nil || p.Contract != "_CmdParam" {
		return
	}
	var body struct {
		CorrelationID string `json:"correlation_id"`
	}
	_ = json.Unmarshal(payload, &body)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen[body.CorrelationID]++
}

func (h *handedLog) count(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seen[id]
}

// restoreRig is a parent and a child over real TLS, where the parent's store
// can be backed up and restored as a volume is: stopped, and its directory
// copied.
type restoreRig struct {
	t        *testing.T
	dir      string
	parentID *identity.Identity
	pcfg     *config.Config
	ps       *store.Store
	peng     *engine.Engine
	pm       *metrics.Metrics
	srv      *Server

	cs     *store.Store
	ceng   *engine.Engine
	cm     *metrics.Metrics
	cl     *Client
	handed *handedLog
}

func newRestoreRig(t *testing.T) *restoreRig {
	t.Helper()
	r := &restoreRig{t: t, dir: t.TempDir(), handed: &handedLog{seen: map[string]int{}}}
	r.parentID = mustIdentity(t, filepath.Join(r.dir, "p.key"))
	childID := mustIdentity(t, filepath.Join(r.dir, "c.key"))
	r.pcfg = &config.Config{ULID: "n-parent", Repl: config.Endpoint{Addr: "127.0.0.1:0"}}
	r.openParent(childSpec{"n-child", childID.PublicHex(), "child1"})

	r.cs = mustStore(t, filepath.Join(r.dir, "cdata"))
	r.cm = metrics.New(r.cs, config.Retention{}, nil)
	_, r.ceng = nodeParts(t, r.cs, &config.Config{ULID: "n-child"}, r.handed.deliver, r.cm, nil)
	r.cl = mustClient(t, r.pcfg.Repl.Addr, r.parentID.PublicHex(), childID)
	r.cl.FollowStore(r.cs)
	t.Cleanup(func() { r.closeParent() })
	return r
}

func (r *restoreRig) parentDir() string { return filepath.Join(r.dir, "pdata") }

// openParent starts the parent on its directory. children are enrolled on the
// first start only; afterwards the registry is what the store holds.
func (r *restoreRig) openParent(children ...childSpec) {
	r.t.Helper()
	ps, err := store.Open(r.parentDir())
	if err != nil {
		r.t.Fatalf("open the parent's store: %v", err)
	}
	r.ps = ps
	r.pm = metrics.New(ps, config.Retention{}, nil)
	preg, peng := nodeParts(r.t, ps, r.pcfg, nil, r.pm, nil, children...)
	r.peng = peng
	srv, addr := startServerWithMetrics(r.t, r.pcfg, peng, r.parentID, preg, r.pm)
	r.srv = srv
	r.pcfg.Repl.Addr = addr // the child dials this address after every restart
}

func (r *restoreRig) closeParent() {
	if r.srv == nil {
		return
	}
	r.srv.Stop()
	r.srv = nil
	if err := r.ps.Close(); err != nil {
		r.t.Errorf("close the parent's store: %v", err)
	}
}

// backUp stops the parent, copies its store and starts it again.
func (r *restoreRig) backUp() string {
	r.t.Helper()
	r.closeParent()
	backup := filepath.Join(r.dir, "pbackup")
	copyTree(r.t, r.parentDir(), backup)
	r.openParent()
	return backup
}

// restore stops the parent, puts the backup in place of its store and starts
// it again.
func (r *restoreRig) restore(backup string) {
	r.t.Helper()
	r.closeParent()
	if err := os.RemoveAll(r.parentDir()); err != nil {
		r.t.Fatal(err)
	}
	copyTree(r.t, backup, r.parentDir())
	r.openParent()
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0o700)
		}
		b, err := os.ReadFile(path) //nolint:gosec // a path below the test's own temp dir
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, rel), b, 0o600)
	})
	if err != nil {
		t.Fatalf("copy %s to %s: %v", from, to, err)
	}
}

func (r *restoreRig) run(loop func(stop <-chan struct{})) {
	r.t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { defer close(done); loop(stop) }()
	r.t.Cleanup(func() {
		close(stop)
		waitForClosed(r.t, "the replication loop to return after stop", done, 5*time.Second)
	})
}

func (r *restoreRig) runDownlink() {
	r.run(func(stop <-chan struct{}) { RunDownlink(r.cl, r.ceng, r.cm, stop) })
}

func (r *restoreRig) runUplink() {
	r.run(func(stop <-chan struct{}) { RunUplink(r.cl, r.ceng, nil, r.cm, stop) })
}

// command issues a command for the child's machine at the parent.
func (r *restoreRig) command(id string) {
	r.t.Helper()
	mustIngestAdmin(r.t, r.peng, "colca/v1/_CmdParam/m1/child1/m1/go",
		`{"correlation_id":"`+id+`","expires_at":99999999999}`)
}

func (r *restoreRig) awaitHanded(id string) {
	r.t.Helper()
	waitFor(r.t, "command "+id+" to be handed to the child's bus", 10*time.Second, func() bool {
		return r.handed.count(id) >= 1
	})
}

// parentHolds counts the child's metric records in the parent's stream.
func (r *restoreRig) parentHolds() int {
	r.t.Helper()
	recs, _, err := r.ps.Read("metrics", r.ps.LWM("metrics"), 1000, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	return len(recs)
}

func (r *restoreRig) childCommandsCursor() uint64 {
	return r.cs.CursorGet(uns.DownlinkCursor(r.cl.ParentPub()), downlinkStream)
}

// A restored parent's commands stream ends before the position its child read
// up to. Waiting there would leave the child without commands until the stream
// had grown past it. The child's
// position is placed where the restored store stands for it, so a command
// issued right after the restore is taken at once, and one it took before is
// not taken again.
func TestAChildTakesCommandsFromARestoredParentAtOnce(t *testing.T) {
	r := newRestoreRig(t)
	r.runDownlink()
	r.runUplink()
	waitForAttached(t, r.cs, r.cl)

	r.command("kept")
	r.awaitHanded("kept")
	backup := r.backUp()
	original := r.ps.StoreID()
	for _, id := range []string{"lost-1", "lost-2", "lost-3"} {
		r.command(id)
		r.awaitHanded(id)
	}
	stranded := r.childCommandsCursor()

	r.restore(backup)
	if head := r.ps.NextOffset("commands"); head >= stranded {
		t.Fatalf("the restored parent's commands head %d is not behind the child's position %d — "+
			"nothing is regressed and the test proves nothing", head, stranded)
	}
	if got := r.ps.StoreID(); got != original {
		t.Fatalf("the restored store has id %q, want the id it was copied with %q", got, original)
	}

	// Reuses an offset the child has read past.
	r.command("after-restore")
	r.awaitHanded("after-restore")

	if got := r.ps.StoreID(); got == original {
		t.Fatal("the restored parent kept its incarnation although its child proved it regressed")
	}
	if got, head := r.childCommandsCursor(), r.ps.NextOffset("commands"); got != head {
		t.Fatalf("the child's commands position is %d, want the restored parent's head %d", got, head)
	}
	if got := scrapeMetric(t, r.cm, "colca_uplink_resyncs_total"); got != 1 {
		t.Fatalf("re-syncs = %v, want 1", got)
	}
	if got := scrapeMetric(t, r.pm, `colca_store_incarnation_changes_total{reason="regression"}`); got != 1 {
		t.Fatalf("incarnation changes at the parent = %v, want 1", got)
	}
	if got := scrapeMetric(t, r.cm, "colca_downlink_cursor_beyond_head_total"); got != 0 {
		t.Fatalf("the child reported a stranded cursor %v times although it re-synced", got)
	}
	for _, id := range []string{"kept", "lost-1", "lost-2", "lost-3", "after-restore"} {
		if n := r.handed.count(id); n != 1 {
			t.Fatalf("command %s was handed to the child's bus %d times, want 1", id, n)
		}
	}

	// An operator gives the store a new incarnation while the child waits in a
	// long poll. The child is told at once, re-syncs, and nothing is replayed.
	if _, err := r.ps.Reincarnate("operator"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the child to re-sync with the new incarnation", 5*time.Second, func() bool {
		return scrapeMetric(t, r.cm, "colca_uplink_resyncs_total") == 2
	})
	r.command("after-change")
	r.awaitHanded("after-change")
	for _, id := range []string{"kept", "after-restore", "after-change"} {
		if n := r.handed.count(id); n != 1 {
			t.Fatalf("command %s was handed to the child's bus %d times, want 1", id, n)
		}
	}
	// The re-sync completes when the uplink has sent the replayed range again,
	// which is after the command above was handed down.
	waitFor(t, "the re-sync to complete", 5*time.Second, func() bool {
		status := r.cl.ResyncStatus(r.cs)
		return status != nil && status.State == resyncComplete
	})
	if status := r.cl.ResyncStatus(r.cs); status.Reason == "" {
		t.Fatalf("re-sync status = %+v, want a complete re-sync with a reason", status)
	}
}

// A restored parent lacks what its child sent since the backup, and the child's
// retention is shorter than that range. The child sends again what it still holds, says
// which range it no longer holds, and keeps replicating.
func TestAResyncReportsWhatRetentionAlreadyPruned(t *testing.T) {
	r := newRestoreRig(t)
	stopLoops := make(chan struct{})
	loops := make(chan struct{}, 2)
	go func() { RunDownlink(r.cl, r.ceng, r.cm, stopLoops); loops <- struct{}{} }()
	go func() { RunUplink(r.cl, r.ceng, nil, r.cm, stopLoops); loops <- struct{}{} }()

	seed(t, r.cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 2)
	waitFor(t, "the records from before the backup at the parent", 10*time.Second, func() bool { return r.parentHolds() == 2 })
	backup := r.backUp()
	seed(t, r.cs, "metrics", "colca/v1/_Metric/m1/m1/late%d", 8)
	waitFor(t, "the records sent since the backup at the parent", 10*time.Second, func() bool { return r.parentHolds() == 10 })
	// The parent holding them is not the child knowing it: the child records
	// the parent's confirmation when the reply arrives, and loops stopped
	// before that leave the mark at 2, so the restored parent looks current
	// and no re-sync starts.
	waitFor(t, "the child to record the parent's confirmation", 10*time.Second, func() bool {
		return r.cs.AckedMark(r.cl.ParentPub(), "metrics") == 10
	})

	// The child is stopped while the parent is restored, and its retention
	// prunes offsets 1..6: four of them are records the restored parent lacks.
	close(stopLoops)
	<-loops
	<-loops
	up := uns.UplinkCursor(r.cl.ParentPub())
	if n, err := r.cs.Prune("metrics", 7, []string{up}, nil); err != nil || n != 6 {
		t.Fatalf("prune: removed %d records, err %v — want 6", n, err)
	}
	r.restore(backup)
	if got := r.parentHolds(); got != 2 {
		t.Fatalf("the restored parent holds %d records, want the 2 of the backup", got)
	}

	// The downlink loop alone: the re-sync is decided and stands as replaying.
	r.runDownlink()
	var status *Resync
	waitFor(t, "the re-sync to start", 10*time.Second, func() bool {
		status = r.cl.ResyncStatus(r.cs)
		return status != nil
	})
	if status.State != resyncReplaying {
		t.Fatalf("re-sync state = %q with records still to send, want %q", status.State, resyncReplaying)
	}
	if want := []ResyncGap{{Stream: "metrics", FromOffset: 3, ToOffset: 6}}; !reflect.DeepEqual(status.Gaps, want) {
		t.Fatalf("gaps = %+v, want %+v", status.Gaps, want)
	}
	if want := (ResyncRange{From: 7, To: 11, Position: 7}); status.Streams["metrics"] != want {
		t.Fatalf("replayed metrics range = %+v, want %+v", status.Streams["metrics"], want)
	}
	if got := scrapeMetric(t, r.cm, "colca_uplink_resync_active"); got != 1 {
		t.Fatalf("colca_uplink_resync_active = %v while replaying, want 1", got)
	}
	if got := scrapeMetric(t, r.cm, `colca_uplink_resync_unreplayable_records_total{stream="metrics"}`); got != 4 {
		t.Fatalf("unreplayable metrics records = %v, want 4", got)
	}
	if got := r.parentHolds(); got != 2 {
		t.Fatalf("the parent holds %d records before the uplink ran, want 2", got)
	}

	// The uplink sends offsets 7..10 again; the re-sync completes and the gap
	// stays listed.
	r.runUplink()
	waitFor(t, "the re-sync to complete", 10*time.Second, func() bool {
		status = r.cl.ResyncStatus(r.cs)
		return status.State == resyncComplete
	})
	if got := r.parentHolds(); got != 6 {
		t.Fatalf("the parent holds %d records after the replay, want 6: the 2 it had and the 4 the child still held", got)
	}
	if status.Completed == nil || len(status.Gaps) != 1 {
		t.Fatalf("completed re-sync = %+v, want a completion time and the gap still listed", status)
	}
	if got := scrapeMetric(t, r.cm, "colca_uplink_resync_active"); got != 0 {
		t.Fatalf("colca_uplink_resync_active = %v after the replay, want 0", got)
	}

	seed(t, r.cs, "metrics", "colca/v1/_Metric/m1/m1/new%d", 1)
	waitFor(t, "a record written after the re-sync at the parent", 10*time.Second, func() bool { return r.parentHolds() == 7 })
}

// A re-sync that was replaying when the node stopped is still replaying when
// it starts again, and completes then.
func TestAResyncInProgressSurvivesARestartOfTheChild(t *testing.T) {
	r := newRestoreRig(t)
	seed(t, r.cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 5)
	state := parentState{Store: r.ps.StoreID(), Resync: &Resync{
		State: resyncReplaying, Reason: "test", ParentStore: r.ps.StoreID(), Started: time.Now().UTC(),
		Streams: map[string]ResyncRange{"metrics": {From: 1, To: 6}},
	}}
	raw, _ := json.Marshal(state)
	if err := r.cs.SetParentState(r.cl.ParentPub(), raw); err != nil {
		t.Fatal(err)
	}
	if err := PrepareUplink(r.cl, r.cs); err != nil {
		t.Fatal(err)
	}
	if status := r.cl.ResyncStatus(r.cs); status == nil || status.State != resyncReplaying {
		t.Fatalf("status after a restart = %+v, want the re-sync still replaying", status)
	}
	r.runDownlink()
	r.runUplink()
	waitFor(t, "the re-sync to complete after the restart", 10*time.Second, func() bool {
		return r.cl.ResyncStatus(r.cs).State == resyncComplete
	})
	if got := r.parentHolds(); got != 5 {
		t.Fatalf("the parent holds %d records, want 5", got)
	}
}

// A second re-sync while the first still replays, as when the parent is given a
// new incarnation meanwhile, takes over what the first had left to send: the
// node keeps saying "replaying" until that is sent too.
func TestAResyncThatInterruptsAnotherKeepsItsRange(t *testing.T) {
	r := newRestoreRig(t)
	seed(t, r.cs, "metrics", "colca/v1/_Metric/m1/m1/temp%d", 5)
	state := parentState{Store: "store-before", Resync: &Resync{
		State: resyncReplaying, Reason: "test", ParentStore: "store-before", Started: time.Now().UTC(),
		Streams: map[string]ResyncRange{"metrics": {From: 1, To: 6}},
	}}
	raw, _ := json.Marshal(state)
	if err := r.cs.SetParentState(r.cl.ParentPub(), raw); err != nil {
		t.Fatal(err)
	}
	if err := PrepareUplink(r.cl, r.cs); err != nil {
		t.Fatal(err)
	}
	r.runDownlink()
	var status *Resync
	waitFor(t, "the re-sync with the parent's actual store", 10*time.Second, func() bool {
		status = r.cl.ResyncStatus(r.cs)
		return status.ParentStore == r.ps.StoreID()
	})
	if want := (ResyncRange{From: 1, To: 6, Position: 1}); status.State != resyncReplaying || status.Streams["metrics"] != want {
		t.Fatalf("status = %q %+v, want replaying over %+v: the first re-sync's range is not sent yet",
			status.State, status.Streams["metrics"], want)
	}
	r.runUplink()
	waitFor(t, "the re-sync to complete", 10*time.Second, func() bool {
		return r.cl.ResyncStatus(r.cs).State == resyncComplete
	})
	if got := r.parentHolds(); got != 5 {
		t.Fatalf("the parent holds %d records, want 5", got)
	}
}

// A child from before the handshake states no incarnation. Against a parent
// that speaks it, it replicates exactly as before: its pushes are stored, its
// polls answered. If it is ahead of the parent's store it cannot be told, so
// the parent reports it and keeps its own incarnation.
func TestAChildFromBeforeTheHandshakeKeepsReplicating(t *testing.T) {
	r := newRestoreRig(t)
	original := r.ps.StoreID()
	rec := func(off uint64) store.ReplRecord {
		return store.ReplRecord{ChildOffset: off, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: int64(off)}
	}
	// Replicate and Poll send what an older child sends: no parent field.
	if hwm, err := r.cl.Replicate("metrics", []store.ReplRecord{rec(1), rec(2)}); err != nil || hwm != 2 {
		t.Fatalf("push: hwm=%d err=%v, want 2", hwm, err)
	}
	r.command("for-the-old-child")
	recs, next, _, err := r.cl.Downlink(1, 10, 5*time.Second)
	if err != nil || len(recs) != 1 || next != 2 {
		t.Fatalf("poll: %d records, next %d, err %v — want the command and next 2", len(recs), next, err)
	}

	// Ahead of the parent's head, as after a restore of the parent.
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, _, err := r.cl.Poll(ctx, 9, 1, 10, time.Second); err == nil {
		t.Fatal("a poll past the parent's head answered — it should wait as it always did")
	}
	if got := scrapeMetric(t, r.pm, "colca_repl_child_ahead_of_store_total"); got < 1 {
		t.Fatalf("colca_repl_child_ahead_of_store_total = %v, want the stranded child counted", got)
	}
	if got := r.ps.StoreID(); got != original {
		t.Fatal("the parent changed incarnation on the word of a child that cannot re-sync")
	}
	if hwm, err := r.cl.Replicate("metrics", []store.ReplRecord{rec(3)}); err != nil || hwm != 3 {
		t.Fatalf("push after the stale poll: hwm=%d err=%v, want 3", hwm, err)
	}
}

// A push that names another incarnation than the parent's is not stored: it
// would move the parent's mark past what the child has yet to send again.
func TestAPushForAnotherIncarnationIsRefusedUnstored(t *testing.T) {
	r := newRestoreRig(t)
	// Opened on a store that existed before, as every node but a brand-new one
	// is: only such a store can be a restored copy.
	r.closeParent()
	r.openParent()
	recs := []store.ReplRecord{{ChildOffset: 1, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":1}`), TS: 1}}
	env := envelope{store: r.cs.StoreID(), stream: "metrics", handshake: true, parent: "another-store"}
	_, _, err := r.cl.replicate(t.Context(), env, recs)
	var refusal *replError
	if !asReplError(err, &refusal) || !refusal.StoreChanged() || refusal.Store != r.ps.StoreID() {
		t.Fatalf("push for another incarnation: err=%v, want a store-changed refusal naming the parent's store", err)
	}
	if refusal.Refused() {
		t.Fatal("a store-changed refusal counts as a refused batch — it is a re-sync, not a rejection")
	}
	if got := r.parentHolds(); got != 0 {
		t.Fatalf("the parent stored %d records of the refused push", got)
	}
	env.parent = r.ps.StoreID()
	if hwm, _, err := r.cl.replicate(t.Context(), env, recs); err != nil || hwm != 1 {
		t.Fatalf("push for the parent's incarnation: hwm=%d err=%v, want 1", hwm, err)
	}
	// The parent confirmed offset 1. A child that says it was told 5 are
	// stored proves the store regressed: nothing is stored and the id changes.
	original := r.ps.StoreID()
	env.acked = 5
	more := []store.ReplRecord{{ChildOffset: 6, Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"v":2}`), TS: 2}}
	if _, _, err := r.cl.replicate(t.Context(), env, more); !asReplError(err, &refusal) || !refusal.StoreChanged() {
		t.Fatalf("push stating a mark the parent does not hold: err=%v, want a store-changed refusal", err)
	}
	if r.ps.StoreID() == original || r.parentHolds() != 1 {
		t.Fatalf("after the proof: store changed=%v, records=%d — want a new incarnation and nothing stored",
			r.ps.StoreID() != original, r.parentHolds())
	}
	if got := scrapeMetric(t, r.pm, `colca_store_incarnation_changes_total{reason="regression"}`); got != 1 {
		t.Fatalf("incarnation changes = %v, want 1", got)
	}

	// The new incarnation began in this run: its history cannot have regressed.
	// A child that claims to be ahead of it re-syncs alone; the id stands, so
	// no child can make its siblings re-sync over and over.
	second := r.ps.StoreID()
	env.parent = second
	if _, _, err := r.cl.replicate(t.Context(), env, more); !asReplError(err, &refusal) || !refusal.StoreChanged() {
		t.Fatalf("push stating a mark ahead of an incarnation of this run: err=%v, want a store-changed refusal", err)
	}
	if got := r.ps.StoreID(); got != second {
		t.Fatal("the store changed incarnation again on a claim against an incarnation that began in this run")
	}
	if got := r.parentHolds(); got != 1 {
		t.Fatalf("the parent stored a refused push: %d records, want 1", got)
	}
}

// A command the child holds is the same command at whatever offset the parent
// hands it: it is executed once. A different command is executed.
func TestACommandTheChildHoldsIsNotExecutedAgain(t *testing.T) {
	r := newRestoreRig(t)
	r.runDownlink()
	waitForAttached(t, r.cs, r.cl)
	r.command("once")
	r.awaitHanded("once")

	// The parent's stream gets the very same record a second time, as when the
	// command comes down to a restored parent again.
	held, _, err := r.ps.Read("commands", 1, 10, nil)
	if err != nil || len(held) != 1 {
		t.Fatalf("the parent's commands stream: %d records, err %v", len(held), err)
	}
	again := store.Record{Topic: held[0].Topic, Payload: held[0].Payload, TS: held[0].TS}
	if _, _, err := r.ps.Append("commands", []store.Record{again}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the child to recognise the command it holds", 10*time.Second, func() bool {
		return scrapeMetric(t, r.cm, "colca_downlink_commands_already_held_total") == 1
	})
	r.command("another")
	r.awaitHanded("another")
	if n := r.handed.count("once"); n != 1 {
		t.Fatalf("the held command was handed to the child's bus %d times, want 1", n)
	}
	if got, head := r.childCommandsCursor(), r.ps.NextOffset("commands"); got != head {
		t.Fatalf("the child's commands position is %d, want %d: a command it holds must not hold the cursor", got, head)
	}
}

// What the child holds is read from its commands stream when the loop starts,
// so a restart does not make it forget; acks on the same stream are not
// commands.
func TestCommandMemoryIsReadFromTheLocalStream(t *testing.T) {
	st := mustStore(t, t.TempDir())
	_, eng := nodeParts(t, st, &config.Config{ULID: "n-child"}, nil, nil, nil)
	command := store.Record{Topic: "colca/v1/_CmdParam/m1/m1/go", Payload: []byte(`{"correlation_id":"c1"}`), TS: 10}
	ack := store.Record{Topic: "colca/v1/_Ack/n-child/m1/go", Payload: []byte(`{"correlation_id":"c1","result_code":200}`), TS: 11}
	if _, _, err := st.Append("commands", []store.Record{command, ack}); err != nil {
		t.Fatal(err)
	}
	mem, err := loadCommandMemory(eng)
	if err != nil {
		t.Fatal(err)
	}
	if !mem.holds(command.Topic, command.Payload, command.TS) {
		t.Fatal("the command in the local stream is not remembered")
	}
	if mem.holds(ack.Topic, ack.Payload, ack.TS) {
		t.Fatal("an ack is remembered as a command")
	}
	for _, other := range []store.Record{
		{Topic: command.Topic, Payload: command.Payload, TS: 12},
		{Topic: command.Topic, Payload: []byte(`{"correlation_id":"c2"}`), TS: 10},
		{Topic: "colca/v1/_CmdParam/m1/m1/stop", Payload: command.Payload, TS: 10},
	} {
		if mem.holds(other.Topic, other.Payload, other.TS) {
			t.Fatalf("a different command is taken for the held one: %+v", other)
		}
	}
	// Entries below the stream's low-water mark go once the memory has grown.
	mem.pruneAt = 2
	mem.add("colca/v1/_CmdParam/m1/m1/new", []byte(`{"correlation_id":"c3"}`), 20, 9, 5)
	if mem.holds(command.Topic, command.Payload, command.TS) {
		t.Fatal("a command retention pruned is still remembered after the memory was trimmed")
	}
	if !mem.holds("colca/v1/_CmdParam/m1/m1/new", []byte(`{"correlation_id":"c3"}`), 20) {
		t.Fatal("the newest command was trimmed with the pruned ones")
	}
}

// After a re-sync a node reads definitions again from where the parent's store
// stands. One it holds byte for byte is not stored again; a changed one and a
// tombstone are.
func TestADefinitionTheChildHoldsIsNotStoredAgain(t *testing.T) {
	r := newRestoreRig(t)
	def := func(off uint64, payload string, ts int64) downResult {
		return downResult{DefNext: off + 1, Definitions: []DownRec{{ParentOffset: off, Topic: groupTopic, Payload: []byte(payload), TS: ts}}}
	}
	const first = `{"id":"01HGRP-OPS","name":"Ops"}`
	if !applyDefinitions(r.cl, r.ceng, r.cm, def(1, first, 100)) {
		t.Fatal("the first definition did not move the cursor")
	}
	if got := r.cs.NextOffset("definitions"); got != 2 {
		t.Fatalf("definitions next offset = %d after the first, want 2", got)
	}
	// Handed again at another offset: the cursor moves, nothing is stored.
	if !applyDefinitions(r.cl, r.ceng, r.cm, def(2, first, 100)) {
		t.Fatal("a definition the node holds held the cursor")
	}
	if got := r.cs.NextOffset("definitions"); got != 2 {
		t.Fatalf("definitions next offset = %d after the same definition again, want 2", got)
	}
	applyDefinitions(r.cl, r.ceng, r.cm, def(3, `{"id":"01HGRP-OPS","name":"Operations"}`, 101))
	if got := r.cs.NextOffset("definitions"); got != 3 {
		t.Fatalf("definitions next offset = %d after a changed definition, want 3", got)
	}
	applyDefinitions(r.cl, r.ceng, r.cm, def(4, ``, 102))
	if got := r.cs.NextOffset("definitions"); got != 4 {
		t.Fatalf("definitions next offset = %d after a tombstone, want 4", got)
	}
}

func asReplError(err error, target **replError) bool {
	re, ok := err.(*replError) //nolint:errorlint // replicate returns it unwrapped
	if ok {
		*target = re
	}
	return ok
}

// /healthz reads the re-sync status. syncMu is held across synced writes of
// the parent state and the uplink cursors, so the status must not wait for
// it: a stalled disk would otherwise stall the liveness probe too.
func TestResyncStatusDoesNotWaitForTheSyncLock(t *testing.T) {
	r := newRestoreRig(t)
	state := parentState{Store: r.ps.StoreID(), Resync: &Resync{
		State: resyncReplaying, Reason: "test", ParentStore: r.ps.StoreID(), Started: time.Now().UTC(),
		Streams: map[string]ResyncRange{"metrics": {From: 1, To: 6}},
	}}
	raw, _ := json.Marshal(state)
	if err := r.cs.SetParentState(r.cl.ParentPub(), raw); err != nil {
		t.Fatal(err)
	}
	if err := PrepareUplink(r.cl, r.cs); err != nil {
		t.Fatal(err)
	}

	r.cl.syncMu.Lock() // a writer in the middle of a synced write
	defer r.cl.syncMu.Unlock()
	got := make(chan *Resync, 1)
	go func() { got <- r.cl.ResyncStatus(r.cs) }()
	select {
	case status := <-got:
		if status == nil || status.State != resyncReplaying || status.Streams["metrics"].Position != 1 {
			t.Fatalf("status = %+v, want the replaying re-sync at position 1", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ResyncStatus waited for syncMu")
	}
}

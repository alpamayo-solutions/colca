package store

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// The indexes stay trusted across an unclean shutdown: every batch writes its
// index entries and the head they are complete through together, so a store
// that was never closed still knows the whole stream is indexed.

// closeUncleanly closes the database under the store without Store.Close, as a
// killed process leaves it: whatever was synced is there, nothing else ran.
func closeUncleanly(t *testing.T, s *Store) {
	t.Helper()
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSignalIndexCoverageSurvivesAnUncleanShutdown(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	appendSparse(t, s, 2000, map[int]string{1999: "a"})
	closeUncleanly(t, s)

	if s, err = Open(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if covered := s.sigFrom["metrics"]; covered != 1 {
		t.Fatalf("coverage starts at %d after an unclean shutdown, want 1: it restarted at the head", covered)
	}
	// A budget of one index entry finds the record behind 2000 busy ones.
	got, _, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 1, 0, []string{"a"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("after an unclean shutdown: %d records within a budget of one, want the indexed one", len(got))
	}
}

// The contract index is not walked after an unclean shutdown. The index here
// is broken on purpose, outside any batch, so a reconcile would show: it stays
// as it was, because nothing written through the store can break it.
func TestKVIndexIsNotReconciledAfterAnUncleanShutdown(t *testing.T) {
	dir := t.TempDir()
	s := openWithSignal(t, dir)
	breakIndex(t, s)
	closeUncleanly(t, s)

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	signals, _, err := s.KVScanPage("", "", 10, []string{"_Signal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(signals) != 0 {
		t.Fatalf("the index was reconciled after an unclean shutdown: %v", topicsOf(signals))
	}
}

// A version that appends without the indexed-head mark (one before it, or one
// without the indexes) moves the head alone. Then the contract index is
// reconciled and the signal index coverage restarts at the head.
func TestAWriteWithoutTheIndexedMarkRestartsBothIndexes(t *testing.T) {
	dir := t.TempDir()
	s := openWithSignal(t, dir)
	appendSparse(t, s, 100, map[int]string{50: "a"})
	breakIndex(t, s)
	head := s.NextOffset("metrics")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	val, err := json.Marshal(recEnc{Topic: "colca/v1/_Metric/n/a", Payload: []byte(`{"signal_id":"a","value":1}`), TS: 1, SignalID: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(streamKey("metrics", head), val, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Set(metaKey("metrics"), be64(head+1), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	assertReconciled(t, dir)
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if covered := s.sigFrom["metrics"]; covered != head+1 {
		t.Fatalf("coverage starts at %d, want the head %d", covered, head+1)
	}
	// The unindexed record is still found: the read scans below the coverage.
	got, next, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 0, 0, []string{"a"}, nil, func(r StoredRecord) bool { return r.SignalID == "a" })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].Offset != head || next != head+1 {
		t.Fatalf("read below the coverage: %+v next=%d, want two records ending at %d", got, next, head)
	}
}

// A read that starts below the coverage scans only up to it, within its budget,
// and then switches to the index: the records of other signals above the
// coverage cost nothing.
func TestAReadBelowTheCoverageSwitchesToTheIndexAtIt(t *testing.T) {
	s := mustOpen(t)
	// 100 records of busy, then the coverage starts, then 5000 busy and one a.
	appendSparse(t, s, 100, nil)
	s.mu.Lock()
	s.state.Lock()
	s.sigFrom["metrics"] = s.next["metrics"]
	s.state.Unlock()
	s.mu.Unlock()
	appendSparse(t, s, 5000, map[int]string{4999: "a"})
	keep := func(_, signalID string) bool { return signalID == "a" }

	// A budget of 101: the 100 unindexed records and one index entry.
	got, _, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 101, 0, []string{"a"}, keep, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SignalID != "a" {
		t.Fatalf("got %+v, want the one record of a, reached through the index", got)
	}

	// A budget smaller than the unindexed range ends the page inside it.
	got, next, err := s.ReadSignals(context.Background(), "metrics", 1, 10, 40, 0, []string{"a"}, keep, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 || next != 41 {
		t.Fatalf("got %d records next=%d, want an empty page ending at 41", len(got), next)
	}
}

// crashHelperEnv names the store directory the helper process appends to.
const crashHelperEnv = "COLCA_STORE_CRASH_HELPER_DIR"

// TestCrashHelperAppendsUntilKilled is the process the crash test kills. It
// appends synced batches forever and prints each one's head once it returned.
func TestCrashHelperAppendsUntilKilled(t *testing.T) {
	dir := os.Getenv(crashHelperEnv)
	if dir == "" {
		t.Skip("run by TestIndexesSurviveAKillBetweenBatches")
	}
	s, err := Open(dir)
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(2)
	}
	for i := 0; ; i++ {
		recs := make([]Record, 0, 51)
		for j := 0; j < 50; j++ {
			recs = append(recs, metricRecord("busy", int64(i*100+j)))
		}
		recs = append(recs, metricRecord(fmt.Sprintf("s%d", i%7), int64(i)))
		recs = append(recs, kvRec("_Signal", fmt.Sprintf("p/%d", i%13), fmt.Sprintf(`{"id":"%d"}`, i)))
		_, last, err := s.Append("metrics", recs[:51])
		if err != nil {
			fmt.Println("append:", err)
			os.Exit(2)
		}
		if _, _, err := s.Append("entities", recs[51:]); err != nil {
			fmt.Println("append:", err)
			os.Exit(2)
		}
		fmt.Println("head", last+1)
	}
}

// A process killed between (and during) synced batches leaves both indexes
// trusted and correct: the coverage still starts at the first record, every
// signal read through the index returns what a full scan returns, and the
// contract index lists exactly the KV entries.
func TestIndexesSurviveAKillBetweenBatches(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a process")
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelperAppendsUntilKilled$", "-test.count=1")
	cmd.Env = append(os.Environ(), crashHelperEnv+"="+dir)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(out)
	var acked uint64
	deadline := time.Now().Add(30 * time.Second)
	for acked < 20000 && lines.Scan() {
		line := lines.Text()
		if rest, ok := strings.CutPrefix(line, "head "); ok {
			if acked, err = strconv.ParseUint(rest, 10, 64); err != nil {
				t.Fatal(err)
			}
		} else if !strings.HasPrefix(line, "=== RUN") {
			t.Logf("helper: %s", line)
		}
		if time.Now().After(deadline) {
			break
		}
	}
	// Killed while it appends: the next batch may or may not have committed.
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_ = cmd.Wait()
	if acked < 20000 {
		t.Fatalf("the helper acknowledged only up to %d before it was killed", acked)
	}

	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	head := s.NextOffset("metrics")
	if head < acked {
		t.Fatalf("head %d is below the acknowledged %d: a synced batch was lost", head, acked)
	}
	if covered := s.sigFrom["metrics"]; covered != 1 {
		t.Fatalf("coverage starts at %d after the kill, want 1", covered)
	}
	all, _, err := s.ReadRecords("metrics", 1, int(head), nil)
	if err != nil {
		t.Fatal(err)
	}
	for k := 0; k < 7; k++ {
		signal := fmt.Sprintf("s%d", k)
		var want []uint64
		for _, r := range all {
			if r.SignalID == signal {
				want = append(want, r.Offset)
			}
		}
		got, _, err := s.ReadSignals(context.Background(), "metrics", 1, len(all), 0, 0, []string{signal}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d records through the index, %d in the stream", signal, len(got), len(want))
		}
		for i := range got {
			if got[i].Offset != want[i] {
				t.Fatalf("%s: record %d at %d through the index, %d in the stream", signal, i, got[i].Offset, want[i])
			}
		}
	}
	entries, err := s.KVScan("")
	if err != nil {
		t.Fatal(err)
	}
	if n := countIndexKeys(t, s); n != len(entries) {
		t.Fatalf("contract index has %d keys for %d KV entries", n, len(entries))
	}
	indexed, _, err := s.KVScanPage("", "", 1000, []string{"_Signal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(indexed) != len(entries) {
		t.Fatalf("the contract index lists %d of %d entries", len(indexed), len(entries))
	}
}

// BenchmarkNarrowCatchUpAfterUncleanRestart is a new consumer of one signal
// catching up a stream of 500 signals page by page (pages of 1000, the /fetch
// scan budget) after the node restarted uncleanly. kept is this version: the
// coverage survives. restarted is what versions before the indexed-head mark
// did after a crash: the coverage restarted at the head, so the read scanned
// the whole stream.
//
//	go test ./internal/store -run '^$' -bench NarrowCatchUp -benchtime 3x
func BenchmarkNarrowCatchUpAfterUncleanRestart(b *testing.B) {
	const records, signals = 500_000, 500
	dir := b.TempDir()
	s, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	recs := make([]Record, 0, 10_000)
	for i := 0; i < records; i++ {
		recs = append(recs, metricRecord(fmt.Sprintf("sig%03d", i%signals), int64(i)+1))
		if len(recs) == cap(recs) {
			if _, _, err := s.Append("metrics", recs); err != nil {
				b.Fatal(err)
			}
			recs = recs[:0]
		}
	}
	if err := s.db.Close(); err != nil { // unclean
		b.Fatal(err)
	}
	if s, err = Open(dir); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	head := s.NextOffset("metrics")
	keep := func(_, signalID string) bool { return signalID == "sig007" }
	for _, tc := range []struct {
		name    string
		covered uint64
	}{{"kept", s.sigFrom["metrics"]}, {"restarted", head}} {
		b.Run(tc.name, func(b *testing.B) {
			s.state.Lock()
			s.sigFrom["metrics"] = tc.covered
			s.state.Unlock()
			pages, matched := 0, 0
			for i := 0; i < b.N; i++ {
				for from := uint64(1); from < head; pages++ {
					got, next, err := s.ReadSignals(context.Background(), "metrics", from, 1000, 1000, 0, []string{"sig007"}, keep, nil)
					if err != nil {
						b.Fatal(err)
					}
					matched += len(got)
					from = next
				}
			}
			b.ReportMetric(float64(pages)/float64(b.N), "pages/catch-up")
			b.ReportMetric(float64(matched)/float64(b.N), "records/catch-up")
		})
	}
}

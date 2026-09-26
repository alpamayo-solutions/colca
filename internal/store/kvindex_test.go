package store

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func kvRec(contract, path string, payload string) Record {
	return Record{
		Topic: "colca/v1/" + contract + "/n1/" + path, Payload: []byte(payload), TS: 1,
		KVPath: path, KVNode: "n1",
	}
}

func topicsOf(entries []KVEntry) []string {
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = e.Topic
	}
	return out
}

// A contract-filtered scan returns what the unindexed walk returns, in the same
// order and with tokens that page the same way, for one and for several contracts.
func TestIndexedScanMatchesTheWalk(t *testing.T) {
	s := mustOpen(t)
	var recs []Record
	for i := 0; i < 30; i++ {
		path := fmt.Sprintf("line/m%02d", i)
		recs = append(recs, kvRec("_Metric", path, `{"v":1}`))
		if i%3 == 0 {
			recs = append(recs, kvRec("_Signal", path, `{"id":"s"}`))
		}
		if i%5 == 0 {
			recs = append(recs, kvRec("_SystemElement", path, `{"id":"e"}`))
		}
	}
	if _, _, err := s.Append("entities", recs); err != nil {
		t.Fatal(err)
	}

	walk := func(contracts ...string) []string {
		want := map[string]bool{}
		for _, c := range contracts {
			want[c] = true
		}
		var out []string
		for _, e := range mustKVScan(t, s, "line/") {
			if kvContractMatches(e.Topic, want) {
				out = append(out, e.Topic)
			}
		}
		return out
	}
	page := func(contracts ...string) []string {
		var out []string
		after := ""
		for {
			entries, next, err := s.KVScanPage("line/", after, 4, contracts)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, topicsOf(entries)...)
			if next == "" {
				return out
			}
			after = next
		}
	}
	for _, contracts := range [][]string{{"_Signal"}, {"_SystemElement", "_Signal"}, {"_Signal", "_Signal"}, {"_Group"}} {
		got, want := page(contracts...), walk(contracts...)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("contracts %v: indexed %v, walk %v", contracts, got, want)
		}
	}
	if n := len(page("_Signal")); n != 10 {
		t.Fatalf("_Signal entries = %d, want 10", n)
	}
}

// Every way an entry leaves KV takes its index key with it: a tombstone, an
// identity's retirement and eviction.
func TestTheIndexFollowsDeletes(t *testing.T) {
	s := mustOpen(t)
	if _, _, err := s.Append("entities", []Record{
		kvRec("_Signal", "a", `{"id":"a"}`),
		kvRec("_Signal", "b", `{"id":"b"}`),
		kvRec("_Signal", "c", `{"id":"c"}`),
	}); err != nil {
		t.Fatal(err)
	}
	tomb := kvRec("_Signal", "a", ``)
	tomb.Delete = true
	if _, _, err := s.Append("entities", []Record{tomb}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegistryDelete("u1", "entities", kvRec("_Signal", "b", ``)); err != nil {
		t.Fatal(err)
	}
	signals, _, err := s.KVScanPage("", "", 10, []string{"_Signal"})
	if err != nil || len(signals) != 1 || signals[0].Path != "c" {
		t.Fatalf("after tombstone and retirement: %v %v", topicsOf(signals), err)
	}
	if n, err := s.EvictKV(func(string) bool { return true }); err != nil || n != 1 {
		t.Fatalf("evicted %d, %v", n, err)
	}
	if left := countIndexKeys(t, s); left != 0 {
		t.Fatalf("%d index keys left after every entry went", left)
	}
}

// A store written without the index (a version before it) is indexed when it
// opens, and an index key whose entry is gone is dropped.
func TestOpenReconcilesTheIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Append("entities", []Record{
		kvRec("_Signal", "a", `{"id":"a"}`),
		kvRec("_SystemElement", "a", `{"id":"e"}`),
	}); err != nil {
		t.Fatal(err)
	}
	// What an older version leaves: an entry without an index key, and an index
	// key for an entry it deleted.
	b := s.db.NewBatch()
	if err := b.Delete(kvIndexKey("a", "n1", "colca/v1/_Signal/n1/a"), nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Set(kvIndexKey("gone", "n1", "colca/v1/_Signal/n1/gone"), nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	signals, _, err := s.KVScanPage("", "", 10, []string{"_Signal"})
	if err != nil || len(signals) != 1 || signals[0].Path != "a" {
		t.Fatalf("after reopening: %v %v", topicsOf(signals), err)
	}
	if n := countIndexKeys(t, s); n != 2 {
		t.Fatalf("index keys = %d, want 2", n)
	}
}

func countIndexKeys(t *testing.T, s *Store) int {
	t.Helper()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("x\x00"), UpperBound: []byte("x\x01")})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n
}

// depth keeps entries at most that many segments below the prefix, pages like any
// scan, and combines with a contract filter.
func TestDepthLimitedScan(t *testing.T) {
	s := mustOpen(t)
	if _, _, err := s.Append("entities", []Record{
		kvRec("_SystemElement", "plant", `{}`),
		kvRec("_SystemElement", "plant/l1", `{}`),
		kvRec("_SystemElement", "plant/l1/m1", `{}`),
		kvRec("_Signal", "plant/l1/m1/speed", `{}`),
		kvRec("_Metric", "plant/l1/m1/speed", `{}`),
		kvRec("_SystemElement", "plant/l2", `{}`),
		kvRec("_Signal", "plant/l2/temp", `{}`),
		kvRec("_SystemElement", "plant0", `{}`),
	}); err != nil {
		t.Fatal(err)
	}
	paths := func(prefix string, depth int, contracts ...string) []string {
		var out []string
		after := ""
		for {
			entries, next, err := s.KVScanPageDepth(prefix, after, 1, contracts, depth)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				out = append(out, e.Path+":"+e.Topic[len("colca/v1/"):len("colca/v1/")+4])
			}
			if next == "" {
				return out
			}
			after = next
		}
	}
	cases := []struct {
		prefix    string
		depth     int
		contracts []string
		want      string
	}{
		{"plant/", 1, nil, "[plant/l1:_Sys plant/l2:_Sys]"},
		{"plant", 1, nil, "[plant:_Sys plant/l1:_Sys plant/l2:_Sys plant0:_Sys]"},
		{"plant/", 2, nil, "[plant/l1:_Sys plant/l1/m1:_Sys plant/l2:_Sys plant/l2/temp:_Sig]"},
		{"plant/l1/m1", 1, nil, "[plant/l1/m1:_Sys plant/l1/m1/speed:_Met plant/l1/m1/speed:_Sig]"},
		{"plant/", 2, []string{"_Signal"}, "[plant/l2/temp:_Sig]"},
		{"", 1, nil, "[plant:_Sys plant0:_Sys]"},
	}
	for _, c := range cases {
		if got := fmt.Sprint(paths(c.prefix, c.depth, c.contracts...)); got != c.want {
			t.Errorf("prefix %q depth %d %v = %s, want %s", c.prefix, c.depth, c.contracts, got, c.want)
		}
	}
}

// A watcher's channel closes on the next append to its stream, not on another
// stream's, and the offsets are the ones the append left.
func TestStreamChangesWakeOnlyForTheirStream(t *testing.T) {
	s := mustOpen(t)
	waits, next := s.StreamChanges([]string{"entities", "metrics", "nope"})
	if _, ok := waits["nope"]; ok || next["entities"] != 1 {
		t.Fatalf("waits %v next %v", waits, next)
	}
	if _, _, err := s.Append("metrics", []Record{{Topic: "colca/v1/_Metric/n1/a", Payload: []byte(`{}`), TS: 1}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waits["entities"]:
		t.Fatal("entities woke on a metrics append")
	default:
	}
	select {
	case <-waits["metrics"]:
	default:
		t.Fatal("metrics did not wake on its own append")
	}
	if _, now := s.StreamChanges([]string{"metrics"}); now["metrics"] != 2 {
		t.Fatalf("metrics next = %d, want 2", now["metrics"])
	}
}

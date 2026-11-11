package store

import (
	"fmt"
	"testing"
)

const sampleTopic = "colca/v1/_Metric/n1/line1/temp"

func sampleRec(v int, at string) Record {
	return Record{
		Topic: sampleTopic, Payload: []byte(fmt.Sprintf(`{"signal_id":"s1","value":%d,"timestamp":%s}`, v, at)),
		TS: 1, KVPath: "line1/temp", KVNode: "n1",
	}
}

// heldValue is the payload the topic's KV entry holds, failing when there is none.
func heldValue(t *testing.T, s *Store) string {
	t.Helper()
	e, ok, err := s.KVGet("line1/temp", "n1", sampleTopic)
	if err != nil || !ok {
		t.Fatalf("KV entry for %s: ok=%v err=%v, want one", sampleTopic, ok, err)
	}
	return string(e.Payload)
}

func streamLen(t *testing.T, s *Store) int {
	t.Helper()
	recs, _, err := s.Read("metrics", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	return len(recs)
}

func appendOne(t *testing.T, s *Store, r Record) Record {
	t.Helper()
	recs := []Record{r}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	return recs[0]
}

func TestOlderSampleJoinsTheStreamButKeepsTheNewerCurrentValue(t *testing.T) {
	s := mustOpen(t)
	newer := sampleRec(2, "1700000100.5")
	if got := appendOne(t, s, newer); got.Stale {
		t.Fatal("the first sample was marked stale")
	}
	if got := appendOne(t, s, sampleRec(1, "1700000099")); !got.Stale {
		t.Fatal("an older sample after a newer one was not marked stale")
	}
	if got := heldValue(t, s); got != string(newer.Payload) {
		t.Fatalf("KV holds %s after an older sample, want the newer %s", got, newer.Payload)
	}
	if n := streamLen(t, s); n != 2 {
		t.Fatalf("stream has %d records, want 2: the older sample is history", n)
	}
}

func TestEqualAndNewerSamplesReplaceTheCurrentValue(t *testing.T) {
	s := mustOpen(t)
	appendOne(t, s, sampleRec(1, "1700000100"))
	corrected := sampleRec(9, "1700000100")
	if got := appendOne(t, s, corrected); got.Stale {
		t.Fatal("a sample at the same timestamp was marked stale")
	}
	if got := heldValue(t, s); got != string(corrected.Payload) {
		t.Fatalf("KV holds %s, want the correction at the same timestamp %s", got, corrected.Payload)
	}
	newer := sampleRec(3, "1700000100.001")
	appendOne(t, s, newer)
	if got := heldValue(t, s); got != string(newer.Payload) {
		t.Fatalf("KV holds %s, want the newer %s", got, newer.Payload)
	}
}

// Records of one batch are not in the database yet when the next one is
// decided, so the batch's own writes count too.
func TestOlderSampleInTheSameBatchKeepsTheNewer(t *testing.T) {
	s := mustOpen(t)
	recs := []Record{sampleRec(2, "1700000200"), sampleRec(1, "1700000100")}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	if recs[0].Stale || !recs[1].Stale {
		t.Fatalf("stale flags %v %v, want false true", recs[0].Stale, recs[1].Stale)
	}
	if got := heldValue(t, s); got != string(recs[0].Payload) {
		t.Fatalf("KV holds %s, want %s", got, recs[0].Payload)
	}
}

// Without a payload timestamp the ingest time is the sample time, as in the
// historian.
func TestSampleWithoutTimestampIsOrderedByIngestTime(t *testing.T) {
	s := mustOpen(t)
	at := func(v int, ts int64) Record {
		r := sampleRec(v, "null")
		r.TS = ts
		return r
	}
	appendOne(t, s, at(1, 2000))
	if got := appendOne(t, s, at(2, 1000)); !got.Stale {
		t.Fatal("a sample ingested earlier was not stale")
	}
	newer := sampleRec(3, "3") // 3 s = 3000 ms
	appendOne(t, s, newer)
	if got := heldValue(t, s); got != string(newer.Payload) {
		t.Fatalf("KV holds %s, want %s", got, newer.Payload)
	}
}

func TestATombstoneClearsTheHeldSampleTime(t *testing.T) {
	s := mustOpen(t)
	appendOne(t, s, sampleRec(1, "1700000100"))
	tomb := Record{Topic: sampleTopic, TS: 1, KVPath: "line1/temp", KVNode: "n1", Delete: true}
	older := sampleRec(0, "1700000000")
	recs := []Record{tomb, older}
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	if recs[1].Stale {
		t.Fatal("a sample after a tombstone in the same batch was stale")
	}
	if got := heldValue(t, s); got != string(older.Payload) {
		t.Fatalf("KV holds %s, want %s", got, older.Payload)
	}
}

// An entry written before the sample time was kept has none: the next sample
// replaces it, whatever its time.
func TestAnEntryWithoutSampleTimeIsReplaced(t *testing.T) {
	s := mustOpen(t)
	b := s.db.NewBatch()
	if err := setKV(b, "line1/temp", "n1", sampleTopic, []byte(`{"t":"`+sampleTopic+`","p":"e30=","ts":9999999999999,"o":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(nil); err != nil {
		t.Fatal(err)
	}
	old := sampleRec(1, "1")
	if got := appendOne(t, s, old); got.Stale {
		t.Fatal("a sample over an entry without sample time was stale")
	}
	if got := heldValue(t, s); got != string(old.Payload) {
		t.Fatalf("KV holds %s, want %s", got, old.Payload)
	}
}

func TestReplicatedOlderSampleKeepsTheParentsNewerValue(t *testing.T) {
	s := mustOpen(t)
	repl := func(off uint64, r Record) ReplRecord {
		return ReplRecord{ChildOffset: off, Topic: r.Topic, Payload: r.Payload, TS: r.TS, KVPath: r.KVPath, KVNode: r.KVNode}
	}
	newer, older, older2 := sampleRec(2, "1700000200"), sampleRec(1, "1700000100"), sampleRec(0, "1700000050")
	got, _, err := s.ApplyReplicated("c1", "metrics", []ReplRecord{repl(1, newer), repl(2, older)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Stale || !got[1].Stale {
		t.Fatalf("applied %+v, want two records, the second stale", got)
	}
	// A later push decides against the committed entry.
	got, _, err = s.ApplyReplicated("c1", "metrics", []ReplRecord{repl(3, older2)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Stale {
		t.Fatalf("applied %+v, want one stale record", got)
	}
	if v := heldValue(t, s); v != string(newer.Payload) {
		t.Fatalf("parent KV holds %s, want the newest %s", v, newer.Payload)
	}
	if n := streamLen(t, s); n != 3 {
		t.Fatalf("parent stream has %d records, want 3", n)
	}
}

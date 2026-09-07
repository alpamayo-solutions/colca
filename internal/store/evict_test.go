package store

import "testing"

// One pass examines at most maxScan records; the sweep still reaches every
// doomed record because each pass resumes where the last one stopped. A cap
// without a resume position would rescan the same clean prefix forever and
// never reach a doomed record behind it — the failure this pins.
func TestEvictRecordsIsBoundedPerPassAndCompleteAcrossPasses(t *testing.T) {
	s := defStore(t)
	var recs []Record
	for i := 0; i < 5; i++ {
		recs = append(recs, Record{Topic: "keep", Payload: []byte("k"), TS: 1})
	}
	recs = append(recs, Record{Topic: "doomed", Payload: []byte("d"), TS: 1})
	recs = append(recs, Record{Topic: "keep", Payload: []byte("k"), TS: 1})
	if _, _, err := s.Append("entities", recs); err != nil {
		t.Fatal(err)
	}
	doomed := func(topic string) bool { return topic == "doomed" }

	first, err := s.EvictRecords("entities", 0, 3, doomed)
	if err != nil {
		t.Fatal(err)
	}
	if first.Done || first.Removed != 0 || first.Resume != 4 {
		t.Fatalf("first pass = %+v, want 3 clean records examined, nothing removed, resume at offset 4", first)
	}
	second, err := s.EvictRecords("entities", first.Resume, 3, doomed)
	if err != nil {
		t.Fatal(err)
	}
	if second.Done || second.Removed != 1 || second.Resume != 7 || second.Bytes == 0 {
		t.Fatalf("second pass = %+v, want the doomed record at offset 6 removed and resume at 7", second)
	}
	third, err := s.EvictRecords("entities", second.Resume, 3, doomed)
	if err != nil {
		t.Fatal(err)
	}
	if !third.Done || third.Removed != 0 || third.Resume != 8 {
		t.Fatalf("third pass = %+v, want the head reached with nothing left to remove", third)
	}

	out, _, err := s.Read("entities", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 6 {
		t.Fatalf("%d records survive, want the 6 kept ones", len(out))
	}
	for _, r := range out {
		if r.Topic != "keep" {
			t.Fatalf("a doomed record survived at offset %d", r.Offset)
		}
	}
	if s.LWM("entities") != 1 {
		t.Fatalf("LWM = %d, want 1: eviction never moves the prefix mark", s.LWM("entities"))
	}
}

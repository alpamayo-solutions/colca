package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// randText is a string that is plain, or one that carries what JSON escapes
// and what could pass for the end of a record: quotes, backslashes, HTML
// characters, `,"sid":"`, control characters, non-ASCII.
func randText(rng *rand.Rand) string {
	parts := []string{"a", "line-1", "/", "01HZZ", `"`, `\`, `,"sid":"`, `"}`, "<&>", "\n", "ü", " ", "sid", "}"}
	var b strings.Builder
	for range rng.IntN(6) {
		if rng.IntN(3) == 0 {
			b.WriteString(parts[rng.IntN(len(parts))])
		} else {
			b.WriteString(parts[rng.IntN(4)])
		}
	}
	return b.String()
}

// TestPeekHeadAgreesWithDecode: whenever peekHead reads a stored record's
// topic and signal id, they are the ones a full decode returns, whatever the
// record's other fields hold; and it reads every record whose topic and signal
// id need no escaping, so the scans keep their fast path.
func TestPeekHeadAgreesWithDecode(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11)) //nolint:gosec // a reproducible sample
	plain := 0
	for i := range 20000 {
		e := recEnc{
			SourceLocalOnly: rng.IntN(2) == 0,
			Topic:           randText(rng),
			Payload:         []byte(randText(rng)),
			TS:              rng.Int64(),
			WrittenBy:       randText(rng), OriginOffset: rng.Uint64N(3),
			ActorID: randText(rng), ActorLabel: randText(rng), ActorKind: randText(rng),
			ActorClient: randText(rng), Door: randText(rng),
			SignalID: randText(rng),
		}
		for range rng.IntN(3) {
			e.ActorGroups = append(e.ActorGroups, randText(rng))
		}
		if i%2 == 0 {
			// Half are shaped like real records: a plain topic and signal id.
			e.Topic = fmt.Sprintf("colca/v1/_Metric/n/line-%d/s%d", i%7, i)
			e.SignalID = []string{"", fmt.Sprintf("01HZZ%05d", i)}[i%4/2]
		}
		val, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		var decoded recEnc
		if err := json.Unmarshal(val, &decoded); err != nil {
			t.Fatal(err)
		}
		topic, signalID, ok := peekHead(val)
		if ok && (topic != decoded.Topic || signalID != decoded.SignalID) {
			t.Fatalf("peekHead(%s) = %q, %q; decoded %q, %q", val, topic, signalID, decoded.Topic, decoded.SignalID)
		}
		if i%2 == 0 {
			if !ok {
				t.Fatalf("peekHead(%s) did not read a plain record", val)
			}
			plain++
		}
	}
	if plain == 0 {
		t.Fatal("no plain record was peeked")
	}
}

// filteredStore is a metrics stream of mixed records: metrics stored with
// their signal id, metrics without one in the payload, records of other
// contracts, topics JSON escapes. prune > 0 prunes a prefix (a gap for a
// reader below it); uncovered moves the signal index's coverage up to that
// offset, as an unclean restart leaves it.
func filteredStore(t *testing.T, rng *rand.Rand, n int, prune, uncovered uint64) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	write := func(recs []Record) {
		if _, _, err := s.Append("metrics", recs); err != nil {
			t.Fatal(err)
		}
	}
	var recs []Record
	for i := range n {
		sig := fmt.Sprintf("01HSIG%02d", rng.IntN(30))
		line := fmt.Sprintf("line-%d", rng.IntN(4))
		var r Record
		switch rng.IntN(20) {
		case 0:
			r = Record{Topic: "colca/v1/_Signal/n/" + line + "/" + sig, Payload: []byte(`{"signal_id":"` + sig + `"}`)}
		case 1:
			r = Record{Topic: "colca/v1/_Metric/n/" + line + "/nosig", Payload: []byte(`{"value":1}`)}
		case 2:
			r = Record{Topic: "colca/v1/_Metric/n/" + line + "/a&b/" + sig, Payload: fmt.Appendf(nil, `{"signal_id":%q,"value":2}`, sig)}
		default:
			r = Record{Topic: "colca/v1/_Metric/n/" + line + "/" + sig, Payload: fmt.Appendf(nil, `{"signal_id":%q,"value":%d}`, sig, i)}
		}
		r.TS = int64(i)
		r.ActorID = randText(rng)
		recs = append(recs, r)
		if len(recs) == 97 || i == n-1 {
			write(recs)
			recs = nil
		}
	}
	if prune > 0 {
		if _, err := s.Prune("metrics", prune, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if uncovered > 0 {
		s.state.Lock()
		s.sigFrom["metrics"] = uncovered
		s.state.Unlock()
	}
	return s
}

// payloadSignal is the signal of a record the way the fetch handler decides it
// for a record stored without one: from its _Metric payload.
func payloadSignal(r StoredRecord) string {
	if r.SignalID != "" {
		return r.SignalID
	}
	if !strings.HasPrefix(r.Topic, "colca/v1/_Metric/") {
		return ""
	}
	var m struct {
		SignalID string `json:"signal_id"`
	}
	_ = json.Unmarshal(r.Payload, &m)
	return m.SignalID
}

type page struct {
	from, next uint64
	offsets    []uint64
}

// readAll pages through the stream from `from` to the head with read and
// returns every page.
func readAll(t *testing.T, head, from uint64, read func(from uint64) ([]StoredRecord, uint64, error)) (pages []page, recs []StoredRecord) {
	t.Helper()
	for from < head {
		got, next, err := read(from)
		if err != nil {
			t.Fatal(err)
		}
		if next <= from {
			t.Fatalf("page at %d did not move on: next %d", from, next)
		}
		p := page{from: from, next: next}
		for _, r := range got {
			if r.Offset < from || r.Offset >= next {
				t.Fatalf("record %d outside its page [%d, %d)", r.Offset, from, next)
			}
			p.offsets = append(p.offsets, r.Offset)
		}
		pages, recs = append(pages, p), append(recs, got...)
		from = next
	}
	return pages, recs
}

// TestFilteredReadsMatchUnfilteredThenFiltered: a read that skips records by
// their head returns exactly what reading every record and filtering them
// returns, in the same order and with the same page boundaries as the same read
// without a head filter, on randomized streams, with a pruned prefix, and below
// and above the signal index's coverage.
func TestFilteredReadsMatchUnfilteredThenFiltered(t *testing.T) {
	for seed := range uint64(12) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99)) //nolint:gosec // a reproducible sample
			n := 1500 + rng.IntN(1500)
			var prune, uncovered uint64
			if seed%3 == 1 {
				prune = uint64(1 + rng.IntN(n/2)) //nolint:gosec // positive
			}
			if seed%2 == 1 {
				uncovered = uint64(1 + rng.IntN(n)) //nolint:gosec // positive
			}
			s := filteredStore(t, rng, n, prune, uncovered)
			head := s.NextOffset("metrics")
			all, _, err := s.ReadRecords("metrics", 1, n+1, nil)
			if err != nil {
				t.Fatal(err)
			}

			wanted := map[string]bool{}
			var signals []string
			for i := range 30 {
				if rng.IntN(4) == 0 {
					id := fmt.Sprintf("01HSIG%02d", i)
					wanted[id] = true
					signals = append(signals, id)
				}
			}
			line := fmt.Sprintf("/line-%d/", rng.IntN(4))
			head1 := func(topic, signalID string) bool {
				return (signalID == "" || wanted[signalID]) && !strings.Contains(topic, line)
			}
			// The full filter applies the head and decides what the head cannot:
			// the payload's signal, and a field only the decoded record has.
			full := func(r StoredRecord) bool {
				return head1(r.Topic, r.SignalID) && wanted[payloadSignal(r)] && r.TS%5 != 0
			}
			var want []uint64
			for _, r := range all {
				if full(r) {
					want = append(want, r.Offset)
				}
			}
			offsets := func(recs []StoredRecord) []uint64 {
				out := make([]uint64, len(recs))
				for i, r := range recs {
					out[i] = r.Offset
				}
				return out
			}
			for _, from := range []uint64{1, s.LWM("metrics"), uint64(1 + rng.IntN(n))} { //nolint:gosec // positive
				limit, maxScan := 1+rng.IntN(40), rng.IntN(300)
				maxBytes := uint64(rng.IntN(4000)) //nolint:gosec // positive
				var tail []uint64
				for _, o := range want {
					if o >= from {
						tail = append(tail, o)
					}
				}
				type variant struct {
					name string
					read func(HeadFilter) func(uint64) ([]StoredRecord, uint64, error)
				}
				for _, v := range []variant{
					{"stream", func(h HeadFilter) func(uint64) ([]StoredRecord, uint64, error) {
						return func(from uint64) ([]StoredRecord, uint64, error) {
							return s.ReadRecordsBounded(context.Background(), "metrics", from, limit, maxScan, maxBytes, h, full)
						}
					}},
					{"signals", func(h HeadFilter) func(uint64) ([]StoredRecord, uint64, error) {
						return func(from uint64) ([]StoredRecord, uint64, error) {
							return s.ReadSignals(context.Background(), "metrics", from, limit, maxScan, maxBytes, signals, h, full)
						}
					}},
				} {
					withPages, withRecs := readAll(t, head, from, v.read(head1))
					withoutPages, withoutRecs := readAll(t, head, from, v.read(nil))
					if got := offsets(withRecs); !slices.Equal(got, tail) {
						t.Fatalf("%s from %d: head-filtered read = %v, want %v", v.name, from, got, tail)
					}
					if !slices.EqualFunc(withPages, withoutPages, func(a, b page) bool {
						return a.from == b.from && a.next == b.next && slices.Equal(a.offsets, b.offsets)
					}) {
						t.Fatalf("%s from %d: pages with the head filter %v, without %v", v.name, from, withPages, withoutPages)
					}
					for i, r := range withRecs {
						if w := withoutRecs[i]; r.Topic != w.Topic || string(r.Payload) != string(w.Payload) || r.ActorID != w.ActorID {
							t.Fatalf("%s: record %d differs: %+v vs %+v", v.name, r.Offset, r, w)
						}
					}
				}
			}
		})
	}
}

// TestHeadFilterSkipsWithoutDecoding: a scan hands the full filter only the
// records the head filter kept (and those whose head needs a decode), so the
// records a narrow read crosses are never decoded.
func TestHeadFilterSkipsWithoutDecoding(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var recs []Record
	for i := range 1000 {
		recs = append(recs, metricRecord(fmt.Sprintf("s%d", i%10), int64(i)))
	}
	recs = append(recs, Record{Topic: "colca/v1/_Metric/n/a&b/s1", Payload: []byte(`{"signal_id":"s1"}`)})
	if _, _, err := s.Append("metrics", recs); err != nil {
		t.Fatal(err)
	}
	decoded := 0
	got, next, err := s.ReadRecordsBounded(context.Background(), "metrics", 1, 1000, 0, 0,
		func(_, signalID string) bool { return signalID == "s1" },
		func(r StoredRecord) bool { decoded++; return r.SignalID == "s1" })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 101 || next != 1002 {
		t.Fatalf("read %d records, next %d; want 101, 1002", len(got), next)
	}
	if decoded != 101 {
		t.Fatalf("the full filter saw %d records, want the 100 kept by the head and the escaped one", decoded)
	}
}

// TestPeekRecordAgreesWithDecode: whenever peekRecord reads a stored record's
// head, it is the one a full decode gives, payload emptiness and a negative or
// huge timestamp included; and it reads every record of the plain shape.
func TestPeekRecordAgreesWithDecode(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5)) //nolint:gosec // a reproducible sample
	plain := 0
	for i := range 20000 {
		e := recEnc{
			SourceLocalOnly: rng.IntN(2) == 0,
			Topic:           randText(rng),
			TS:              []int64{rng.Int64(), -rng.Int64(), 0, 1}[rng.IntN(4)],
			WrittenBy:       randText(rng), OriginOffset: rng.Uint64N(3),
			ActorID: randText(rng), ActorLabel: randText(rng), ActorKind: randText(rng),
			ActorClient: randText(rng), Door: randText(rng),
			SignalID: randText(rng),
		}
		switch rng.IntN(3) {
		case 0: // nil: encoded as null
		case 1:
			e.Payload = []byte{}
		default:
			e.Payload = []byte(randText(rng) + "x")
		}
		if i%2 == 0 {
			e.Topic = fmt.Sprintf("colca/v1/_Metric/n/line-%d/s%d", i%7, i)
			e.SignalID = []string{"", fmt.Sprintf("01HZZ%05d", i)}[i%4/2]
		}
		val, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		var d recEnc
		if err := json.Unmarshal(val, &d); err != nil {
			t.Fatal(err)
		}
		want := recHead{topic: d.Topic, signalID: d.SignalID, ts: d.TS, emptyPayload: len(d.Payload) == 0}
		h, ok := peekRecord(val)
		if ok && h != want {
			t.Fatalf("peekRecord(%s) = %+v; decoded %+v", val, h, want)
		}
		if got, err := headOf(val); err != nil || got != want {
			t.Fatalf("headOf(%s) = %+v, %v; decoded %+v", val, got, err, want)
		}
		if i%2 == 0 {
			if !ok {
				t.Fatalf("peekRecord(%s) did not read a plain record", val)
			}
			plain++
		}
	}
	if plain == 0 {
		t.Fatal("no plain record was peeked")
	}
}

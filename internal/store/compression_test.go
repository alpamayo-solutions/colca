package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// The record mix of a hub, per machine-day in the 2026-10 scale test: metrics
// ≈420 B stored, logs ≈720 B, annotations ≈3.1 KB of JSON, entities ≈1 KB, in
// roughly 240 : 28 : 5 : 4 records. Payloads follow the shapes of that test's
// records; ids and values are random, as in production, so they compress as
// badly as real ones.

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func randID(r *rand.Rand) string {
	b := make([]byte, 26)
	for i := range b {
		b[i] = crockford[r.IntN(len(crockford))]
	}
	return string(b)
}

// mixGen produces the hub record mix for a few machines.
type mixGen struct {
	r        *rand.Rand
	nodes    []string
	signals  [][]string // per node
	names    []string
	tsMS     int64
	sequence int
}

func newMixGen(seed uint64, machines, signalsPerMachine int) *mixGen {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	g := &mixGen{r: r, tsMS: 1_791_099_600_000}
	for range machines {
		g.nodes = append(g.nodes, randID(r))
		var sigs []string
		for range signalsPerMachine {
			sigs = append(sigs, randID(r))
		}
		g.signals = append(g.signals, sigs)
	}
	for i := range signalsPerMachine {
		g.names = append(g.names, fmt.Sprintf("Signal%03d", i))
	}
	return g
}

// next returns the stream and record of the next record in the mix.
func (g *mixGen) next() (string, Record) {
	g.sequence++
	g.tsMS += int64(g.r.IntN(20))
	m := g.r.IntN(len(g.nodes))
	node := g.nodes[m]
	ts := float64(g.tsMS) / 1000
	switch k := g.r.IntN(277); {
	case k < 240:
		s := g.r.IntN(len(g.signals[m]))
		var value string
		switch s % 3 {
		case 0:
			value = fmt.Sprintf("%d", g.r.IntN(30000))
		case 1:
			value = fmt.Sprintf("%.4f", g.r.Float64()*4000)
		default:
			value = []string{`"bereit"`, `"fuellen"`, `"stoerung"`}[g.r.IntN(3)]
		}
		payload := fmt.Sprintf(`{"value": %s, "timestamp": %.7f, "signal_id": %q, "quality": "good"}`, value, ts, g.signals[m][s])
		return "metrics", Record{
			Topic:   "prekit/v1/_Metric/" + node + "/CraftCanFiller/" + g.names[s],
			Payload: []byte(payload), TS: g.tsMS,
		}
	case k < 268:
		payload := fmt.Sprintf(`{"extra":{"node":%q,"request_id":%q,"duration_ms":%d},"function":"handle","level":"INFO","line_no":%d,"logger_name":"hygentile-plc","message":"cycle %d closed after %d steps","module":"cycles","timestamp":"2026-10-04T07:%02d:%02d.%06dZ"}`,
			node, randID(g.r), g.r.IntN(5000), g.r.IntN(900), g.sequence, g.r.IntN(30), g.r.IntN(60), g.r.IntN(60), g.r.IntN(1_000_000))
		return "logs", Record{Topic: "prekit/v1/_Log/" + node + "/hygentile-plc/INFO", Payload: []byte(payload), TS: g.tsMS}
	case k < 273:
		var phases, signals []string
		start := 0
		for _, name := range []string{"sleeve_closing", "pressure_build_up", "filling", "lid_placement_pressure", "lid_placement", "can_lifting", "closure_pressure", "increased_can_pressure", "seaming", "can_and_sleeve_lowering", "can_removal"} {
			d := 50 + g.r.IntN(5000)
			phases = append(phases, fmt.Sprintf(`{"name": %q, "start_ms": %d, "duration_ms": %d}`, name, start, d))
			start += d
		}
		for i := range 14 {
			v := g.r.Float64() * 3000
			signals = append(signals, fmt.Sprintf(`"ist_signal_%02d": {"n": %d, "min": %.1f, "max": %.1f, "avg": %.2f, "last": %.1f}`, i, g.r.IntN(40), v, v+g.r.Float64()*50, v+g.r.Float64()*25, v+g.r.Float64()*40))
		}
		id := randID(g.r)
		payload := fmt.Sprintf(`{"annotation_id": %q, "annotation_type_id": "3R3QWKY4ZT5S3W6DH4E26PQQQ8", "time_start": %.7f, "time_end": %.7f, "value": {"schema": 1, "detector": "steps-v3", "node_id": %q, "cycle_number": %d, "duration_ms": %d, "kind": "production", "outcome": "good", "fault": false, "emergency_stop": false, "interrupted": false, "recipe": null, "recipe_known": true, "max_step": 20, "step_resolution_ms": 20, "phases": [%s], "signals": {%s}}}`,
			id, ts-14, ts, node, g.sequence, start, strings.Join(phases, ", "), strings.Join(signals, ", "))
		return "annotations", Record{Topic: "prekit/v1/_Annotation/" + node + "/cycles/cycle/" + id, Payload: []byte(payload), TS: g.tsMS}
	default:
		s := g.r.IntN(len(g.signals[m]))
		payload := fmt.Sprintf(`{"config": {}, "created_at": "2026-10-03T10:05:19.276605Z", "data_tag": %q, "data_type": "float", "description": "Abfuelldruck (setpoint) %d", "has_contract": false, "id": %q, "index_type": "none", "is_logged": true, "is_published": true, "max_value": %d, "metadata": {"source": "plc", "address": "DB%d.DBD%d"}, "min_value": %d, "name": %q, "precision": 0, "replication_policy": "replicate_to_parents", "semantic_type_id": null, "system_element_id": %q, "unit": "mbar", "updated_at": "2026-10-03T10:05:19.276616Z"}`,
			randID(g.r), g.sequence, g.signals[m][s], g.r.IntN(5000), g.r.IntN(100), g.r.IntN(400), g.r.IntN(500), strings.ToLower(g.names[s]), randID(g.r))
		return "entities", Record{
			Topic: "prekit/v1/_Signal/" + node + "/CraftCanFiller/" + g.names[s], Payload: []byte(payload), TS: g.tsMS,
			KVPath: "CraftCanFiller/" + g.names[s], KVNode: node,
		}
	}
}

// writeMix appends n records of the mix in batches of 500 per stream.
func writeMix(tb testing.TB, s *Store, g *mixGen, n int) {
	tb.Helper()
	pending := map[string][]Record{}
	flush := func(stream string) {
		if _, _, err := s.Append(stream, pending[stream]); err != nil {
			tb.Fatal(err)
		}
		pending[stream] = nil
	}
	for range n {
		stream, rec := g.next()
		pending[stream] = append(pending[stream], rec)
		if len(pending[stream]) == 500 {
			flush(stream)
		}
	}
	for stream, recs := range pending {
		if len(recs) > 0 {
			flush(stream)
		}
	}
}

// settle flushes the memtable and compacts the whole keyspace, so every table
// on disk is written with the store's current compression.
func settle(tb testing.TB, s *Store) {
	tb.Helper()
	if err := s.db.Flush(); err != nil {
		tb.Fatal(err)
	}
	if err := s.db.Compact(context.Background(), []byte{0}, []byte{0xff, 0xff}, false); err != nil {
		tb.Fatal(err)
	}
}

// tableBytes is the size of the live tables: the store at rest, without the
// WAL and without obsolete tables Pebble has not deleted yet.
func tableBytes(s *Store) int64 { return s.db.Metrics().Total().TablesSize }

// tableCompressions lists the data-block compression of every live table.
func tableCompressions(t *testing.T, s *Store) []string {
	t.Helper()
	levels, err := s.db.SSTables(pebble.WithProperties())
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, level := range levels {
		for _, tbl := range level {
			out = append(out, tbl.Properties.CompressionName)
		}
	}
	return out
}

// readEverything returns every record of every stream, for comparing stores.
func readEverything(t *testing.T, s *Store) map[string][]StoredRecord {
	t.Helper()
	out := map[string][]StoredRecord{}
	for _, stream := range streams {
		from := s.LWM(stream)
		for {
			recs, next, err := s.ReadRecords(stream, from, 1000, nil)
			if err != nil {
				t.Fatal(err)
			}
			out[stream] = append(out[stream], recs...)
			if len(recs) == 0 || next <= from {
				break
			}
			from = next
		}
	}
	return out
}

func sameRecords(t *testing.T, got, want map[string][]StoredRecord) {
	t.Helper()
	for _, stream := range streams {
		g, w := got[stream], want[stream]
		if len(g) != len(w) {
			t.Fatalf("%s: %d records, want %d", stream, len(g), len(w))
		}
		for i := range w {
			if g[i].Offset != w[i].Offset || g[i].Topic != w[i].Topic || string(g[i].Payload) != string(w[i].Payload) || g[i].TS != w[i].TS {
				t.Fatalf("%s record %d differs: %+v, want %+v", stream, i, g[i], w[i])
			}
		}
	}
}

// zstd tables are written as zstd and read back unchanged, also by a store
// opened with the default compression.
func TestCompressionZstdRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenWithOptions(dir, Options{Compression: "zstd"})
	if err != nil {
		t.Fatal(err)
	}
	writeMix(t, s, newMixGen(1, 3, 20), 3000)
	settle(t, s)
	for _, c := range tableCompressions(t, s) {
		if !strings.Contains(strings.ToLower(c), "zstd") {
			t.Fatalf("table compression %q, want zstd", c)
		}
	}
	want := readEverything(t, s)
	kv := mustKVScan(t, s, "")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sameRecords(t, readEverything(t, s), want)
	if got := mustKVScan(t, s, ""); len(got) != len(kv) {
		t.Fatalf("KV entries after reopen = %d, want %d", len(got), len(kv))
	}
}

// Switching an existing store to zstd: it opens and reads as before, and
// compaction rewrites the old snappy tables as zstd (Pebble may do that for
// small stores already while opening). Counters, offsets and records carry
// over.
func TestCompressionSwitchOnAnExistingStore(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := newMixGen(2, 3, 20)
	writeMix(t, s, g, 2000)
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, c := range tableCompressions(t, s) {
		if !strings.Contains(strings.ToLower(c), "snappy") {
			t.Fatalf("default table compression %q, want snappy", c)
		}
	}
	nextBefore, bytesBefore := s.NextOffset("metrics"), s.StreamBytes("metrics")
	old := readEverything(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenWithOptions(dir, Options{Compression: "zstd"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.NextOffset("metrics") != nextBefore || s.StreamBytes("metrics") != bytesBefore {
		t.Fatalf("counters changed on reopen: next %d/%d bytes %d/%d", s.NextOffset("metrics"), nextBefore, s.StreamBytes("metrics"), bytesBefore)
	}
	// The old records read as before, whichever kind their table is by now.
	sameRecords(t, readEverything(t, s), old)
	writeMix(t, s, g, 2000)
	want := readEverything(t, s)
	settle(t, s)
	for _, c := range tableCompressions(t, s) {
		if !strings.Contains(strings.ToLower(c), "zstd") {
			t.Fatalf("after compaction a table is still %q", c)
		}
	}
	sameRecords(t, readEverything(t, s), want)
}

func TestCompressionUnknownIsRefused(t *testing.T) {
	if _, err := OpenWithOptions(t.TempDir(), Options{Compression: "lz4"}); err == nil {
		t.Fatal("an unknown compression opened a store")
	}
}

// BenchmarkCompressionAtRest writes the hub record mix with each compression
// and reports the size at rest after a full compaction, per record and as a
// ratio of the logical bytes the store counts (b/{stream}). ns/op is the
// write, flush and compaction time for the whole mix, the CPU the setting
// costs. Run with -benchtime=1x; the mix is fixed so the sizes are comparable.
func BenchmarkCompressionAtRest(b *testing.B) {
	const records = 20_000
	for _, c := range []string{"snappy", "zstd"} {
		b.Run(c, func(b *testing.B) {
			var disk, logical int64
			for range b.N {
				b.StopTimer()
				dir := b.TempDir()
				s, err := OpenWithOptions(dir, Options{Compression: c})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				writeMix(b, s, newMixGen(42, 10, 100), records)
				settle(b, s)
				b.StopTimer()
				logical = 0
				for _, stream := range streams {
					logical += int64(s.StreamBytes(stream))
				}
				disk = tableBytes(s)
				if err := s.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(disk)/records, "disk-B/rec")
			b.ReportMetric(float64(logical)/records, "logical-B/rec")
			b.ReportMetric(float64(logical)/float64(disk), "ratio")
		})
	}
}

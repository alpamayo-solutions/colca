package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// plantStream is a metrics stream shaped like a plant hub's: plantSignals
// signals publishing plantRate records a second between them, for
// COLCA_BENCH_HOURS hours (1 by default; the plant's 12.5 h backlog is 12).
// COLCA_BENCH_DIR names where it is built (the system temp dir by default).
// Built once per run and shared by the benchmarks that read it.
const (
	plantSignals = 4000
	plantRate    = 150
	plantWanted  = 235
)

var plant struct {
	once    sync.Once
	s       *Store
	ids     []string
	wanted  []string
	dir     string
	records int
	err     error
}

func plantHours() float64 {
	if v, err := strconv.ParseFloat(os.Getenv("COLCA_BENCH_HOURS"), 64); err == nil && v > 0 {
		return v
	}
	return 1
}

// plantRecord is a replicated _Metric as an edge sends it: a topic with the
// node and an ISA-95 path, a payload with the signal id, value, quality and
// time, and the writer's identity.
func plantRecord(node, signalID string, i int, ts int64) Record {
	return Record{
		Topic:     fmt.Sprintf("colca/v1/_Metric/%s/site/hall-%d/line-%d/machine-%d/%s", node, i%3, i%17, i%101, signalID),
		Payload:   fmt.Appendf(nil, `{"signal_id":%q,"value":%d.%03d,"quality":"good","ts":%d}`, signalID, i%997, i%1000, ts),
		TS:        ts,
		WrittenBy: "colca-service:" + node,
		ActorID:   node, ActorLabel: "edge " + node, ActorKind: "node",
		OriginOffset: uint64(i + 1), //nolint:gosec // i is positive
	}
}

func plantStore(b *testing.B) *Store {
	b.Helper()
	plant.once.Do(func() {
		plant.dir, plant.err = os.MkdirTemp(os.Getenv("COLCA_BENCH_DIR"), "colca-plant-bench-")
		if plant.err != nil {
			return
		}
		s, err := Open(plant.dir)
		if err != nil {
			plant.err = err
			return
		}
		plant.s = s
		plant.ids = make([]string, plantSignals)
		for i := range plant.ids {
			plant.ids[i] = fmt.Sprintf("01HPLANT0000000000000%05d", i)
		}
		rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a benchmark's fixed sample
		for _, i := range rng.Perm(plantSignals)[:plantWanted] {
			plant.wanted = append(plant.wanted, plant.ids[i])
		}
		plant.records = int(plantHours() * 3600 * plantRate)
		const batch = 5000
		recs := make([]Record, 0, batch)
		for i := range plant.records {
			// Signals publish at different rates: a few fast ones, many slow.
			sig := plant.ids[int(float64(plantSignals)*rng.Float64()*rng.Float64())]
			recs = append(recs, plantRecord("01HEDGE00000000000000000"+strconv.Itoa(i%4), sig, i, int64(i)*1000/plantRate))
			if len(recs) == batch || i == plant.records-1 {
				if _, _, err := s.Append("metrics", recs); err != nil {
					plant.err = err
					return
				}
				recs = recs[:0]
			}
		}
		plant.err = s.db.Flush()
	})
	if plant.err != nil {
		b.Fatal(plant.err)
	}
	return plant.s
}

func TestMain(m *testing.M) {
	code := m.Run()
	if plant.s != nil {
		_ = plant.s.Close()
		_ = os.RemoveAll(plant.dir)
	}
	os.Exit(code)
}

// BenchmarkFetchFilteredBacklog is a consumer of plantWanted signals catching
// up the whole plant stream page by page, as GET /fetch?signal_id=... serves it
// (pages of 1000, the handler's scan budget). It reports pages/s, the stream
// records the cursor crossed per second (scanned-rec/s) and the records it
// returned per second (matched-rec/s).
//
//	go test ./internal/store -run '^$' -bench FetchFilteredBacklog -benchtime 1x
//	COLCA_BENCH_HOURS=12 go test ./internal/store -run '^$' -bench FetchFilteredBacklog -benchtime 1x -timeout 1h
//
// indexed reads through the signal index. unindexed is the same read on a
// stream the index does not cover yet (after an unclean shutdown, or one
// written by a version without the index): the read falls back to the stream.
// contract is a prefix or contract filter, which has no index and scans.
func BenchmarkFetchFilteredBacklog(b *testing.B) {
	s := plantStore(b)
	want := make(map[string]struct{}, len(plant.wanted))
	for _, id := range plant.wanted {
		want[id] = struct{}{}
	}
	bySignal := func(r StoredRecord) bool { _, ok := want[r.SignalID]; return ok }
	signalHead := func(_, signalID string) bool { _, ok := want[signalID]; return ok }
	byPath := func(r StoredRecord) bool { return strings.Contains(r.Topic, "/hall-1/line-3/") }
	pathHead := func(topic, _ string) bool { return strings.Contains(topic, "/hall-1/line-3/") }
	const limit, budget = 1000, 20000
	for _, tc := range []struct {
		name    string
		covered bool
		read    func(from uint64) ([]StoredRecord, uint64, error)
	}{
		{"indexed", true, func(from uint64) ([]StoredRecord, uint64, error) {
			return s.ReadSignals(context.Background(), "metrics", from, limit, budget, 8<<20, plant.wanted, signalHead, bySignal)
		}},
		{"unindexed", false, func(from uint64) ([]StoredRecord, uint64, error) {
			return s.ReadSignals(context.Background(), "metrics", from, limit, budget, 8<<20, plant.wanted, signalHead, bySignal)
		}},
		{"contract", true, func(from uint64) ([]StoredRecord, uint64, error) {
			return s.ReadRecordsBounded(context.Background(), "metrics", from, limit, budget, 8<<20, pathHead, byPath)
		}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			s.state.Lock()
			covered := s.sigFrom["metrics"]
			if !tc.covered {
				s.sigFrom["metrics"] = s.next["metrics"]
			}
			s.state.Unlock()
			defer func() { s.state.Lock(); s.sigFrom["metrics"] = covered; s.state.Unlock() }()
			head := s.NextOffset("metrics")
			var pages, matched, scanned int
			missesBefore := s.db.Metrics().BlockCache.Misses
			b.ResetTimer()
			start := time.Now()
			for range b.N {
				for from := s.LWM("metrics"); from < head; {
					recs, next, err := tc.read(from)
					if err != nil {
						b.Fatal(err)
					}
					pages++
					matched += len(recs)
					scanned += int(next - from) //nolint:gosec // next >= from
					from = next
				}
			}
			sec := time.Since(start).Seconds()
			b.ReportMetric(float64(pages)/sec, "pages/s")
			b.ReportMetric(float64(scanned)/sec, "scanned-rec/s")
			b.ReportMetric(float64(matched)/sec, "matched-rec/s")
			b.ReportMetric(float64(plant.records), "stream-rec")
			b.ReportMetric(float64(s.db.Metrics().BlockCache.Misses-missesBefore)/float64(pages), "block-misses/page")
		})
	}
}

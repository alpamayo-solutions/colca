package metrics

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// BenchmarkScrapeWithAStuckCursor is one /metrics scrape of a node whose
// metrics stream holds 100k records past their max_age behind a stuck cursor.
//
//	go test ./internal/metrics -run '^$' -bench ScrapeWithAStuckCursor
func BenchmarkScrapeWithAStuckCursor(b *testing.B) {
	s, err := store.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { s.Close() })
	base := time.Now().Add(-48 * time.Hour).UnixMilli()
	for chunk := 0; chunk < 10; chunk++ {
		recs := make([]store.Record, 10_000)
		for i := range recs {
			recs[i] = store.Record{Topic: "colca/v1/_Metric/m1/m1/temp", Payload: []byte(`{"signal_id":"01HZZ","value":1.5,"note":"a sample of ordinary size"}`), TS: base + int64(chunk*10_000+i)}
		}
		if _, _, err := s.Append("metrics", recs); err != nil {
			b.Fatal(err)
		}
	}
	if !s.CursorAck("stuck", "metrics", 2) {
		b.Fatal("seed cursor")
	}
	m := New(s, config.Retention{Streams: map[string]config.StreamRetention{
		"metrics": {MaxAge: config.Duration(time.Hour)},
	}}, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.reg.Gather(); err != nil {
			b.Fatal(err)
		}
	}
}

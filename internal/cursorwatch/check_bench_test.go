package cursorwatch

import (
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// BenchmarkCheckAfterEachFetch is a consumer that reads one signal of a busy
// stream: 50k records it does not read lie past its cursor, and it fetches
// (with the same filter) before every check.
//
//	go test ./internal/cursorwatch -run '^$' -bench CheckAfterEachFetch
func BenchmarkCheckAfterEachFetch(b *testing.B) {
	f := newFixture(b)
	f.read(b, "c/dataops-line/metrics", "metrics")
	for chunk := 0; chunk < 5; chunk++ {
		recs := make([]store.Record, 10_000)
		for i := range recs {
			recs[i] = store.Record{Topic: "colca/v1/_Metric/NODE/other", Payload: []byte(`{"signal_id":"other","value":1.5}`), TS: 1}
		}
		if _, _, err := f.st.Append("metrics", recs); err != nil {
			b.Fatal(err)
		}
	}
	remember := func() {
		f.w.Filters.Remember("c/dataops-line/metrics", "metrics", Filter{Key: "mine",
			Head: func(_, signalID string) bool { return signalID == "mine" },
			Keep: func(r store.StoredRecord) bool { return r.SignalID == "mine" }})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		remember()
		f.w.Check(f.now)
	}
}

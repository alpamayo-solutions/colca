package httpapi

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// TestFetchFilteredBacklogRate is the plant consumer through GET /fetch: a
// caller of 235 of 4000 signals drains a metrics backlog page by page, as the
// machine m1 (grants checked per record) and as the admin token. It runs only
// with COLCA_BENCH_RECORDS set and logs pages/s and stream records crossed/s.
//
//	COLCA_BENCH_RECORDS=500000 go test ./internal/httpapi -run FetchFilteredBacklogRate -v
func TestFetchFilteredBacklogRate(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("COLCA_BENCH_RECORDS"))
	if n <= 0 {
		t.Skip("COLCA_BENCH_RECORDS not set")
	}
	a := newAPI(t)
	const signals, wantedN = 4000, 235
	ids := make([]string, signals)
	for i := range ids {
		ids[i] = fmt.Sprintf("01HPLANT0000000000000%05d", i)
	}
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a fixed sample
	recs := make([]store.Record, 0, 5000)
	for i := range n {
		sig := ids[int(float64(signals)*rng.Float64()*rng.Float64())]
		ts := int64(i) * 1000 / 150
		recs = append(recs, store.Record{
			Topic:   fmt.Sprintf("colca/v1/_Metric/n-test/m1/hall-%d/line-%d/%s", i%3, i%17, sig),
			Payload: fmt.Appendf(nil, `{"signal_id":%q,"value":%d.%03d,"quality":"good","ts":%d}`, sig, i%997, i%1000, ts),
			TS:      ts, WrittenBy: "colca-service:edge", ActorID: "edge", ActorLabel: "edge", ActorKind: "node",
		})
		if len(recs) == cap(recs) || i == n-1 {
			if _, _, err := a.st.Append("metrics", recs); err != nil {
				t.Fatal(err)
			}
			recs = recs[:0]
		}
	}
	q := url.Values{"stream": {"metrics"}, "max": {"1000"}}
	for _, i := range rng.Perm(signals)[:wantedN] {
		q.Add("signal_id", ids[i])
	}
	head := a.st.NextOffset("metrics")
	for _, who := range []string{"m1", "admin"} {
		hc, tok := client(a.m1), ""
		q.Set("cursor", "m1/bench")
		if who == "admin" {
			hc, tok = client(nil), "tok"
		}
		start, pages, matched := time.Now(), 0, 0
		for from := uint64(1); from < head; pages++ {
			q.Set("from", strconv.FormatUint(from, 10))
			resp, out := req(t, hc, "GET", a.url+"/fetch?"+q.Encode(), tok, nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s fetch = %d %v", who, resp.StatusCode, out)
			}
			matched += len(out["records"].([]any))
			from = uint64(out["next"].(float64))
		}
		sec := time.Since(start).Seconds()
		t.Logf("%s: %d pages, %.1f pages/s, %.0f scanned-rec/s, %.0f matched-rec/s", who, pages, float64(pages)/sec, float64(head)/sec, float64(matched)/sec)
	}
}

package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/hostclock"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// healthzClock serves /healthz from an engine on clk and returns the clock
// block as the wire carries it.
func healthzClock(t *testing.T, cfg *config.Config, clk *clock.Clock) (block map[string]json.RawMessage, eng *engine.Engine) {
	t.Helper()
	s, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	reg, err := registry.New(s, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	eng = engine.New(s, cfg, reg, nil, nil, clk)
	reg.SetNamespace(eng.Elements())
	return readHealthzClock(t, Handler(eng, cfg, reg, nil, nil, testBlobs(t, cfg), "deadbeef", false, nil)), eng
}

func readHealthzClock(t *testing.T, h http.Handler) map[string]json.RawMessage {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz: %d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Clock map[string]json.RawMessage `json:"clock"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("/healthz: %v: %s", err, rec.Body.String())
	}
	if out.Clock == nil {
		t.Fatalf("/healthz has no clock block: %s", rec.Body.String())
	}
	return out.Clock
}

// wantJSON compares one field of the block with its exact JSON.
func wantJSON(t *testing.T, block map[string]json.RawMessage, key, want string) {
	t.Helper()
	got, ok := block[key]
	if !ok {
		t.Fatalf("clock block has no %q: %v", key, block)
	}
	if string(got) != want {
		t.Fatalf("clock.%s = %s, want %s", key, got, want)
	}
}

// The /healthz clock block is {wall_now_ms, is_root, host:{synchronized,
// estimated_error_ms}, parent_skew_ms, parent_sample_age_s} on every node. On a
// root the parent fields are null; where the kernel's verdict cannot be read the
// host fields are null. Nothing of the old block (now_ms, offset_ms,
// sync_age_seconds, beacon_interval_seconds) is left.
func TestHealthzClockBlockOnARoot(t *testing.T) {
	wall := time.UnixMilli(1_700_000_000_000)
	clk := clock.NewWithHost(true, func() time.Time { return wall }, func() hostclock.Status { return hostclock.Status{} })
	block, _ := healthzClock(t, &config.Config{ULID: "n-root"}, clk)

	if len(block) != 5 {
		t.Fatalf("clock block has %d fields, want exactly 5: %v", len(block), block)
	}
	wantJSON(t, block, "wall_now_ms", "1700000000000")
	wantJSON(t, block, "is_root", "true")
	wantJSON(t, block, "parent_skew_ms", "null")
	wantJSON(t, block, "parent_sample_age_s", "null")
	wantJSON(t, block, "host", `{"estimated_error_ms":null,"synchronized":null}`)
}

func TestHealthzClockBlockOnAChild(t *testing.T) {
	wall := time.UnixMilli(1_700_000_000_000)
	host := hostclock.Status{Reported: true, Synchronized: true, EstimatedError: 2500 * time.Microsecond, MaxError: time.Second}
	clk := clock.NewWithHost(false, func() time.Time { return wall }, func() hostclock.Status { return host })
	cfg := &config.Config{ULID: "n-child", Parent: &config.Parent{}}

	block, eng := healthzClock(t, cfg, clk)
	wantJSON(t, block, "is_root", "false")
	wantJSON(t, block, "parent_skew_ms", "null") // no response from the parent yet
	wantJSON(t, block, "parent_sample_age_s", "null")
	wantJSON(t, block, "host", `{"estimated_error_ms":2.5,"synchronized":true}`)

	// The parent's clock is 3s behind; the sample is 12s old when /healthz is read.
	eng.ObserveParentClock(clock.ParentSample{NowMS: wall.UnixMilli() - 3_000, Sent: wall, Received: wall})
	wall = wall.Add(12 * time.Second)
	host.Synchronized = false
	reg, err := registry.New(eng.Store(), cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	block = readHealthzClock(t, Handler(eng, cfg, reg, nil, nil, testBlobs(t, cfg), "deadbeef", false, nil))
	if len(block) != 5 {
		t.Fatalf("clock block has %d fields, want exactly 5: %v", len(block), block)
	}
	wantJSON(t, block, "wall_now_ms", "1700000012000")
	wantJSON(t, block, "parent_skew_ms", "-3000")
	wantJSON(t, block, "parent_sample_age_s", "12")
	wantJSON(t, block, "host", `{"estimated_error_ms":2.5,"synchronized":false}`)
}

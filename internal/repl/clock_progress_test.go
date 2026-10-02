package repl

import (
	"bytes"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
)

type clockTransport func(*http.Request) (*http.Response, error)

func (f clockTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// An annotation arriving while the last priority lane is in flight used to
// reach the parent AFTER a newly appended metric marker in the same pass.
func TestClockProgressCannotOvertakeLatePriorityEvents(t *testing.T) {
	f := newParentFixture(t)
	cs := mustStore(t, filepath.Join(t.TempDir(), "child"))
	_, eng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	cm := metrics.New(cs, config.Retention{}, nil)
	cl := mustClient(t, f.addr, f.pid.PublicHex(), f.cid)
	original := cl.http.Transport
	var once sync.Once
	var premature bool
	cl.http.Transport = clockTransport(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if bytes.Contains(body, []byte(`"stream":"logs"`)) {
			once.Do(func() {
				if _, _, err := cs.Append("annotations", []store.Record{{Topic: "colca/v1/_Annotation/n-child/m1/event", Payload: []byte(`{"id":"event"}`), TS: 1}}); err != nil {
					t.Error(err)
				}
				if _, _, err := cs.Append("metrics", []store.Record{{Topic: "colca/v1/_ClockProgress/n-child/dataops/_service", Payload: []byte(`{"run_id":"run","processed_at":10}`), TS: 2}}); err != nil {
					t.Error(err)
				}
			})
		}
		if bytes.Contains(body, []byte("_ClockProgress")) && f.ps.NextOffset("annotations") < 2 {
			premature = true
		}
		return original.RoundTrip(r)
	})
	if _, _, err := cs.Append("logs", []store.Record{{Topic: "colca/v1/_Log/n-child/service/log", Payload: []byte(`{"message":"trigger"}`), TS: 1}}); err != nil {
		t.Fatal(err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() { defer close(done); RunUplink(cl, eng, nil, cm, stop) }()
	waitFor(t, "ordered completion marker", 10*time.Second, func() bool { return f.ps.NextOffset("metrics") == 2 })
	close(stop)
	waitForClosed(t, "uplink stops", done, 5*time.Second)
	if premature {
		t.Fatal("completion marker overtook an earlier annotation")
	}
}

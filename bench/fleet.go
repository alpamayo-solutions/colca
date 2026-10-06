package bench

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The fleet scenario drives an EXISTING parent (a hub deployed as it is in
// production: colcad, colca-historian, Postgres) with many protocol children,
// each emulating one edge: a connector that scans Signals every ScanPeriod and
// reports by exception, so ChangeRatio of the Signals produce a sample per scan.
// A child pushes each scan's samples as one replication batch, one request in
// flight, as colcad's uplink does when its connector publishes a batch per poll
// cycle, and keeps one downlink long poll open.
//
// It reports, every Interval, one JSON line: samples generated and accepted,
// pushes, failures by status, push latency (the parent's durable-ingest time)
// and the backlog the children hold. Whatever else is measured (the parent's
// CPU, the historian's lag, Postgres) is sampled from outside.

// FleetParams configures RunFleet. The parent's admin token must be BenchToken.
type FleetParams struct {
	HubAPI, HubRepl string // host:port of the parent's admin API and replication door
	HubPubkeyHex    string
	HubULID         string
	KeyDir          string // child keys, kept so a rerun against the same hub reuses its enrollments

	Children    int
	FirstChild  int // name offset, so several generators can share one hub
	Signals     int
	ScanPeriod  time.Duration
	ChangeRatio float64
	Logs        float64 // log records per second per child, on the logs stream
	Duration    time.Duration
	Interval    time.Duration
	// Backlog prefills each child with this much history at its rate, as a
	// child that was offline replays it on reconnect.
	Backlog time.Duration
	Out     io.Writer
}

type fleetChild struct {
	ulid    string
	client  *repl.Client
	signals []string // signal ids
	paths   []string
	values  []float64
	rng     *rand.Rand

	mu      sync.Mutex
	pending map[string][]store.ReplRecord // per stream
	off     map[string]uint64
	wake    chan struct{}
}

type fleetStats struct {
	generated, accepted, pushes, pushFailed atomic.Int64
	logsAccepted                            atomic.Int64
	statusMu                                sync.Mutex
	status                                  map[string]int64
	latMu                                   sync.Mutex
	lat                                     []float64 // ms
	batch                                   []float64 // records per push
}

func (s *fleetStats) fail(reason string) {
	s.pushFailed.Add(1)
	s.statusMu.Lock()
	s.status[reason]++
	s.statusMu.Unlock()
}

// signalID is a stable ULID per child and signal, so a rerun keeps its ids.
func signalID(child string, i int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("signal:%s:%d", child, i)))
	var id ulid.ULID
	copy(id[:], sum[:16])
	// A valid ULID's time part starts with 0-7; keep it in this decade.
	binary.BigEndian.PutUint16(id[0:2], 0x0199)
	return id.String()
}

func (c *fleetChild) add(stream, topic string, payload []byte, tsMS int64) {
	c.mu.Lock()
	c.off[stream]++
	c.pending[stream] = append(c.pending[stream], store.ReplRecord{
		ChildOffset: c.off[stream], Topic: topic, Payload: payload, TS: tsMS, WrittenBy: "connector-1",
	})
	c.mu.Unlock()
}

func (c *fleetChild) poke() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// scan produces one connector cycle's samples at time now.
func (c *fleetChild) scan(now time.Time, ratio float64) int {
	n := 0
	for i := range c.signals {
		if c.rng.Float64() >= ratio {
			continue
		}
		c.values[i] += c.rng.NormFloat64()
		payload, _ := json.Marshal(map[string]any{
			"value": c.values[i], "timestamp": float64(now.UnixNano()) / 1e9, "signal_id": c.signals[i],
		})
		c.add("metrics", uns.Prefix()+"_Metric/"+c.ulid+"/"+c.paths[i], payload, now.UnixMilli())
		n++
	}
	return n
}

func (c *fleetChild) runUplink(ctx context.Context, st *fleetStats) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		}
		// Priority lanes first, as colcad does, then metrics.
		for _, stream := range []string{"logs", "metrics"} {
			for ctx.Err() == nil {
				c.mu.Lock()
				batch := c.pending[stream][:min(len(c.pending[stream]), 200)]
				c.mu.Unlock()
				if len(batch) == 0 {
					break
				}
				t0 := time.Now()
				_, err := c.client.Replicate(stream, batch)
				if err != nil {
					reason := "transport"
					if msg := err.Error(); strings.Contains(msg, "status") || strings.Contains(msg, "HTTP") {
						reason = msg
						if len(reason) > 60 {
							reason = reason[:60]
						}
					}
					st.fail(reason)
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
					}
					continue
				}
				ms := float64(time.Since(t0).Microseconds()) / 1000
				st.pushes.Add(1)
				if stream == "metrics" {
					st.accepted.Add(int64(len(batch)))
				} else {
					st.logsAccepted.Add(int64(len(batch)))
				}
				st.latMu.Lock()
				st.lat = append(st.lat, ms)
				st.batch = append(st.batch, float64(len(batch)))
				st.latMu.Unlock()
				c.mu.Lock()
				c.pending[stream] = c.pending[stream][len(batch):]
				c.mu.Unlock()
			}
		}
	}
}

func (c *fleetChild) runDownlink(ctx context.Context) {
	after, defAfter := uint64(1), uint64(1)
	for ctx.Err() == nil {
		next, defNext, err := c.client.Poll(ctx, after, defAfter, 200, 20*time.Second)
		if err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		after, defAfter = max(after, next), max(defAfter, defNext)
	}
}

func (c *fleetChild) backlog() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, p := range c.pending {
		n += len(p)
	}
	return n
}

// RunFleet runs the fleet scenario until Duration passes. See the comment at
// the top of this file.
func RunFleet(p FleetParams) error {
	if p.Children <= 0 || p.Signals <= 0 || p.ScanPeriod <= 0 {
		return fmt.Errorf("fleet needs children, signals and a scan period")
	}
	if p.Interval <= 0 {
		p.Interval = 10 * time.Second
	}
	if p.Out == nil {
		p.Out = os.Stdout
	}
	if err := os.MkdirAll(p.KeyDir, 0o750); err != nil {
		return err
	}
	hub := &fanoutNode{name: "hub", addrs: map[string]string{"api": p.HubAPI},
		hc: &http.Client{Timeout: 30 * time.Second, Transport: apiTransport()}}

	runID := time.Now().UnixNano()
	children := make([]*fleetChild, p.Children)
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var startErr error
	for i := range children {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			name := fmt.Sprintf("e%04d", p.FirstChild+i+1)
			childULID := "n-" + name
			id, fresh, err := identity.LoadOrGenerate(filepath.Join(p.KeyDir, childULID+".key"))
			if err == nil && fresh {
				err = enrollKey(hub, name, childULID, "node", id.PublicHex())
			} else if err == nil {
				// A reused key: enroll again, which a fresh hub needs and an old one accepts.
				if e := enrollKey(hub, name, childULID, "node", id.PublicHex()); e != nil && !strings.Contains(e.Error(), "409") {
					err = e
				}
			}
			var client *repl.Client
			if err == nil {
				client, err = repl.NewClient("https://"+p.HubRepl, p.HubPubkeyHex, id)
			}
			if err != nil {
				errMu.Lock()
				startErr = fmt.Errorf("%s: %w", name, err)
				errMu.Unlock()
				return
			}
			// A fresh incarnation per run: the generator's offsets restart at 1,
			// and a parent that kept the last run's marks would drop them.
			client.SetStoreID(fmt.Sprintf("fleet-%s-%d", childULID, runID))
			c := &fleetChild{ulid: childULID, client: client, wake: make(chan struct{}, 1),
				pending: map[string][]store.ReplRecord{}, off: map[string]uint64{},
				rng: rand.New(rand.NewPCG(uint64(i), 42))} //nolint:gosec // load shape, not security
			for s := 0; s < p.Signals; s++ {
				c.signals = append(c.signals, signalID(childULID, s))
				c.paths = append(c.paths, fmt.Sprintf("line/cell%d/sig%03d", s%8, s))
				c.values = append(c.values, float64(s))
			}
			children[i] = c
		}(i)
	}
	wg.Wait()
	if startErr != nil {
		return startErr
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.Duration)
	defer cancel()
	st := &fleetStats{status: map[string]int64{}}
	var loops sync.WaitGroup
	for i, c := range children {
		if p.Backlog > 0 {
			scans := int(p.Backlog / p.ScanPeriod)
			start := time.Now().Add(-p.Backlog)
			for k := 0; k < scans; k++ {
				st.generated.Add(int64(c.scan(start.Add(time.Duration(k)*p.ScanPeriod), p.ChangeRatio)))
			}
		}
		loops.Add(3)
		go func() { defer loops.Done(); c.runUplink(ctx, st) }()
		go func() { defer loops.Done(); c.runDownlink(ctx) }()
		go func(i int, c *fleetChild) {
			defer loops.Done()
			// Spread the children's phases over one scan period.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(i) * p.ScanPeriod / time.Duration(len(children))):
			}
			tick := time.NewTicker(p.ScanPeriod)
			defer tick.Stop()
			var logDebt float64
			c.poke()
			for {
				select {
				case <-ctx.Done():
					return
				case now := <-tick.C:
					st.generated.Add(int64(c.scan(now, p.ChangeRatio)))
					logDebt += p.Logs * p.ScanPeriod.Seconds()
					for ; logDebt >= 1; logDebt-- {
						payload, _ := json.Marshal(map[string]any{"level": "INFO", "message": "cycle finished on line 1, all stations nominal",
							"logger": "connector", "timestamp": float64(now.UnixNano()) / 1e9})
						c.add("logs", uns.Prefix()+"_Log/"+c.ulid+"/connector-1", payload, now.UnixMilli())
					}
					c.poke()
				}
			}
		}(i, c)
	}

	enc := json.NewEncoder(p.Out)
	tick := time.NewTicker(p.Interval)
	defer tick.Stop()
	last := time.Now()
	var prevGen, prevAcc, prevPush, prevFail, prevLogs int64
	report := func(now time.Time) {
		elapsed := now.Sub(last).Seconds()
		last = now
		gen, acc, push, fail, logs := st.generated.Load(), st.accepted.Load(), st.pushes.Load(), st.pushFailed.Load(), st.logsAccepted.Load()
		st.latMu.Lock()
		lat, batch := st.lat, st.batch
		st.lat, st.batch = nil, nil
		st.latMu.Unlock()
		st.statusMu.Lock()
		status := st.status
		st.status = map[string]int64{}
		st.statusMu.Unlock()
		sort.Float64s(lat)
		backlog, worst := 0, 0
		for _, c := range children {
			b := c.backlog()
			backlog += b
			worst = max(worst, b)
		}
		meanBatch := 0.0
		for _, b := range batch {
			meanBatch += b
		}
		if len(batch) > 0 {
			meanBatch /= float64(len(batch))
		}
		line := map[string]any{
			"t": now.UTC().Format(time.RFC3339), "children": p.Children,
			"generated_per_s": float64(gen-prevGen) / elapsed, "accepted_per_s": float64(acc-prevAcc) / elapsed,
			"logs_per_s":   float64(logs-prevLogs) / elapsed,
			"pushes_per_s": float64(push-prevPush) / elapsed, "failed_per_s": float64(fail-prevFail) / elapsed,
			"failures": status, "mean_batch": meanBatch,
			"push_p50_ms": Percentile(lat, 50), "push_p95_ms": Percentile(lat, 95), "push_p99_ms": Percentile(lat, 99),
			"backlog_records": backlog, "backlog_worst_child": worst,
		}
		if len(lat) > 0 {
			line["push_max_ms"] = lat[len(lat)-1]
		}
		prevGen, prevAcc, prevPush, prevFail, prevLogs = gen, acc, push, fail, logs
		_ = enc.Encode(line)
	}
	for {
		select {
		case <-ctx.Done():
			report(time.Now())
			loops.Wait()
			return nil
		case now := <-tick.C:
			report(now)
		}
	}
}

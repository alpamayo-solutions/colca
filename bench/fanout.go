package bench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The fanout scenario measures a parent's replication door against many
// children. One parent and N children run as separate processes: the parent is
// this binary's "node" command (colcad with a pprof listener), every child is
// the real colcad. Each child publishes ChildRate records per second of
// realistic size through its own admin door and replicates them upward; every
// second record of a child's stream carries its send time, and an observer on
// the parent's broker measures the edge→parent delay of those probes.
//
// The children reach the parent from 127.0.0.1, one source address, as machines
// behind one site router do.

// fanoutPayloadPad sizes the records like the Hygentile edge's average (about
// 500 bytes stored per record across metrics, logs and annotations).
var fanoutPayloadPad = strings.Repeat("x", 300)

type fanoutNode struct {
	name, dir string
	cmd       *exec.Cmd
	addrs     map[string]string
	hc        *http.Client
}

// readAddrFile waits for colcad's addr_file, which it writes once every listener
// is bound.
func readAddrFile(path string, deadline time.Time) (map[string]string, error) {
	for {
		raw, err := os.ReadFile(path)
		if err == nil {
			var addrs map[string]string
			if json.Unmarshal(raw, &addrs) == nil && addrs["api"] != "" {
				return addrs, nil
			}
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%s never appeared", path)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func startFanoutNode(bin []string, env []string, name, dir, cfg string) (*fanoutNode, error) {
	cfgPath := filepath.Join(dir, name+".yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		return nil, err
	}
	logf, err := os.Create(filepath.Join(dir, name+".log"))
	if err != nil {
		return nil, err
	}
	args := append(append([]string{}, bin[1:]...), cfgPath)
	cmd := exec.CommandContext(context.Background(), bin[0], args...) //nolint:gosec // the bench's own binaries
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	go func() { _ = cmd.Wait(); _ = logf.Close() }()
	addrs, err := readAddrFile(filepath.Join(dir, name+".addr.json"), time.Now().Add(60*time.Second))
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
	return &fanoutNode{name: name, dir: dir, cmd: cmd, addrs: addrs,
		hc: &http.Client{Timeout: 30 * time.Second, Transport: apiTransport()}}, nil
}

func (n *fanoutNode) stop() {
	if n == nil || n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
}

func (n *fanoutNode) kill() {
	if n == nil || n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = n.cmd.Process.Kill()
}

func nodeConfig(ulid, dir, keyPath, addrFile, parent string) string {
	cfg := fmt.Sprintf(`ulid: %s
data_dir: %s
key_file: %s
log_level: warn
addr_file: %s
api:
  addr: 127.0.0.1:0
  token: %s
mqtt:
  addr: 127.0.0.1:0
repl:
  addr: 127.0.0.1:0
`, ulid, filepath.Join(dir, ulid+"-data"), keyPath, addrFile, BenchToken)
	return cfg + parent
}

// protoChild speaks the replication protocol and nothing else: it pushes its
// records in batches, one request in flight, as a node's uplink does, and keeps
// one downlink long poll open. The parent cannot tell it from a colcad child,
// and a host can run hundreds of them where it runs a few hundred colcads at
// most (each colcad keeps its own store and syncs it).
type protoChild struct {
	ulid    string
	client  *repl.Client
	mu      sync.Mutex
	pending []store.ReplRecord
	off     uint64
	wake    chan struct{}
}

func (pc *protoChild) add(topic string, payload []byte) {
	pc.mu.Lock()
	pc.off++
	pc.pending = append(pc.pending, store.ReplRecord{ChildOffset: pc.off, Topic: topic, Payload: payload, TS: time.Now().UnixMilli()})
	pc.mu.Unlock()
	select {
	case pc.wake <- struct{}{}:
	default:
	}
}

func (pc *protoChild) runUplink(ctx context.Context, failed *atomic.Int64) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pc.wake:
		}
		for {
			pc.mu.Lock()
			batch := pc.pending[:min(len(pc.pending), 200)]
			pc.mu.Unlock()
			if len(batch) == 0 {
				break
			}
			if _, err := pc.client.Replicate("metrics", batch); err != nil {
				if failed.Add(1) == 1 {
					fmt.Fprintln(os.Stderr, "fanout: first failed push:", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			pc.mu.Lock()
			pc.pending = pc.pending[len(batch):]
			pc.mu.Unlock()
		}
	}
}

func (pc *protoChild) runDownlink(ctx context.Context) {
	after, defAfter := uint64(1), uint64(1)
	for ctx.Err() == nil {
		next, defNext, err := pc.client.Poll(ctx, after, defAfter, 200, 20*time.Second)
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

// FanoutParams extends Params for the fanout scenario.
type FanoutParams struct {
	Params
	Children   int
	ChildRate  float64 // records per second per child
	Warmup     time.Duration
	ProfileDir string // when set, CPU/mutex/block/goroutine/heap profiles of the parent land here
	// ChildDir holds the children's stores when set. On one host every child
	// syncs to the same disk, which a real fleet never does: a laptop SSD takes a
	// few hundred fsyncs per second in all, so N children saturate it before the
	// parent is measured. Point this at a RAM disk to give the children "their own
	// disks" and leave the parent's store on the real one.
	ChildDir string
	// Protocol runs protocol children (see protoChild) in this process instead of
	// colcad processes.
	Protocol bool
	Self     string // this binary, which runs the parent with a pprof listener
}

// RunFanout runs the fanout scenario. See the comment at the top of this file.
func RunFanout(p FanoutParams) (*Report, error) {
	if p.Children <= 0 {
		return nil, fmt.Errorf("fanout needs at least one child")
	}
	if p.ChildRate <= 0 {
		p.ChildRate = 3
	}
	dir := p.WorkDir
	hubKey := filepath.Join(dir, "hub.key")
	hubID, err := identity.Generate(hubKey)
	if err != nil {
		return nil, err
	}
	pprofFile := filepath.Join(dir, "hub.pprof")
	hub, err := startFanoutNode([]string{p.Self, "node"}, []string{"COLCA_BENCH_PPROF_FILE=" + pprofFile},
		"hub", dir, nodeConfig("n-hub", dir, hubKey, filepath.Join(dir, "hub.addr.json"), ""))
	if err != nil {
		return nil, err
	}
	defer hub.kill()
	hubAPI, hubRepl := hub.addrs["api"], hub.addrs["repl"]
	if err := waitHealthy(hub.hc, hubAPI); err != nil {
		return nil, err
	}
	pprofAddr := ""
	if raw, err := readAddrFile(pprofFile, time.Now().Add(10*time.Second)); err == nil {
		pprofAddr = raw["api"]
	}

	// Observer on the parent's broker.
	obsID, err := enrollAtHub(hub, dir, "observer", "external", "read:#")
	if err != nil {
		return nil, err
	}
	type probeKey struct {
		child string
		seq   int
	}
	var (
		latMu sync.Mutex
		lats  []float64
		// hopLats count from the moment the child confirmed it stored a probe, so
		// they leave out the child's own disk and CPU: the replication hop alone.
		hopLats  []float64
		storedAt = map[probeKey]int64{}
		recvAt   = map[probeKey]int64{}
		// The window is decided by when a probe was sent, so a backlog from the
		// warm-up neither counts nor hides, and a probe sent in the window counts
		// however late it arrives.
		winStart, winEnd atomic.Int64
		sentInWin        atomic.Int64
		seenMu           sync.Mutex
		seen             = map[string]bool{}
		received         atomic.Int64
	)
	obs, err := connect(hub.addrs["mqtt"], "fanout-obs", "observer", obsID)
	if err != nil {
		return nil, err
	}
	defer obs.Disconnect(100)
	tk := obs.Subscribe(uns.Prefix()+"_Metric/#", 1, func(_ pahomqtt.Client, m pahomqtt.Message) {
		now := time.Now().UnixNano()
		if !strings.HasSuffix(m.Topic(), "/probe") {
			return
		}
		var body struct {
			SentNS int64  `json:"sent_ns"`
			Child  string `json:"child"`
			Seq    int    `json:"seq"`
		}
		if json.Unmarshal(m.Payload(), &body) != nil || body.SentNS == 0 {
			return
		}
		seenMu.Lock()
		seen[body.Child] = true
		seenMu.Unlock()
		if inWindow(&winStart, &winEnd, body.SentNS) {
			received.Add(1)
			latMu.Lock()
			lats = append(lats, float64(now-body.SentNS)/1e6)
			k := probeKey{body.Child, body.Seq}
			if stored, ok := storedAt[k]; ok {
				hopLats = append(hopLats, float64(now-stored)/1e6)
				delete(storedAt, k)
			} else {
				recvAt[k] = now // arrived before the child's answer did
			}
			latMu.Unlock()
		}
	})
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		return nil, fmt.Errorf("observer subscribe: %w", tk.Error())
	}

	// Children, started in parallel. Each one gets a store function: the colcad
	// child stores through its admin door, the protocol child queues for its
	// uplink.
	type child struct {
		ulid  string
		store func(path string, payload []byte) error
	}
	children := make([]child, p.Children)
	var procs []*fanoutNode
	var procMu sync.Mutex
	defer func() {
		for _, c := range procs {
			c.stop()
		}
		time.Sleep(time.Second)
		for _, c := range procs {
			c.kill()
		}
	}()
	loadCtx, stopLoad := context.WithCancel(context.Background())
	var (
		loadWG    sync.WaitGroup
		published atomic.Int64
		failed    atomic.Int64
		pubMu     sync.Mutex
		pubLats   []float64 // how long a child takes to store a record: its own disk, not the parent
	)
	defer func() { stopLoad(); loadWG.Wait() }()
	parent := fmt.Sprintf("parent:\n  url: https://%s\n  pubkey: %s\n", hubRepl, hubID.PublicHex())
	childDir := dir
	if p.ChildDir != "" {
		if childDir, err = os.MkdirTemp(p.ChildDir, "colca-fanout-"); err != nil {
			return nil, err
		}
		defer func() { _ = os.RemoveAll(childDir) }() //nolint:gosec // the directory MkdirTemp just created
	}
	sem := make(chan struct{}, 16)
	var (
		wg       sync.WaitGroup
		startErr error
		errMu    sync.Mutex
	)
	for i := range children {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			name := fmt.Sprintf("c%03d", i+1)
			ulid := "n-" + name
			key := filepath.Join(childDir, ulid+".key")
			id, err := identity.Generate(key)
			if err == nil {
				err = enrollNodeAtHub(hub, name, ulid, id.PublicHex())
			}
			if err == nil && p.Protocol {
				var client *repl.Client
				if client, err = repl.NewClient("https://"+hubRepl, hubID.PublicHex(), id); err == nil {
					client.SetStoreID(ulid)
					pc := &protoChild{ulid: ulid, client: client, wake: make(chan struct{}, 1)}
					loadWG.Add(2)
					go func() { defer loadWG.Done(); pc.runUplink(loadCtx, &failed) }()
					go func() { defer loadWG.Done(); pc.runDownlink(loadCtx) }()
					children[i] = child{ulid: ulid, store: func(path string, payload []byte) error {
						pc.add(uns.Prefix()+"_Metric/"+ulid+"/"+path, payload)
						return nil
					}}
				}
			} else if err == nil {
				var c *fanoutNode
				c, err = startFanoutNode([]string{p.ColcadPath}, nil, ulid, childDir,
					nodeConfig(ulid, childDir, key, filepath.Join(childDir, ulid+".addr.json"), parent))
				if err == nil {
					procMu.Lock()
					procs = append(procs, c)
					procMu.Unlock()
					children[i] = child{ulid: ulid, store: func(path string, payload []byte) error {
						msg, _ := json.Marshal(map[string]any{"topic": uns.Prefix() + "_Metric/" + ulid + "/" + path, "payload": json.RawMessage(payload)})
						return publishOnce(c.hc, c.addrs["api"], msg)
					}}
				}
			}
			if err != nil {
				errMu.Lock()
				startErr = errors.Join(startErr, fmt.Errorf("%s: %w", name, err))
				errMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if startErr != nil {
		return nil, startErr
	}

	// Load: every child stores ChildRate records per second, alternating a probe
	// and a padded record.
	interval := time.Duration(float64(time.Second) / p.ChildRate)
	for i, c := range children {
		loadWG.Add(1)
		go func(i int, c child) {
			defer loadWG.Done()
			// Spread the children's phases over one interval.
			time.Sleep(time.Duration(i) * interval / time.Duration(len(children)))
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for seq := 0; ; seq++ {
				select {
				case <-loadCtx.Done():
					return
				case <-tick.C:
				}
				path := fmt.Sprintf("line/s%d", seq%40)
				body := map[string]any{"v": float64(seq), "value": float64(seq), "signal_id": path, "timestamp": float64(time.Now().UnixNano()) / 1e9,
					"quality": "good", "unit": "bar", "note": fanoutPayloadPad}
				sentNS := time.Now().UnixNano()
				probe := seq%2 == 0
				if probe {
					path = "probe"
					body = map[string]any{"v": float64(seq), "value": float64(seq), "signal_id": "probe", "timestamp": float64(sentNS) / 1e9,
						"sent_ns": sentNS, "child": c.ulid, "seq": seq, "note": fanoutPayloadPad}
				}
				counted := inWindow(&winStart, &winEnd, sentNS)
				payload, _ := json.Marshal(body)
				t0 := time.Now()
				err := c.store(path, payload)
				if err == nil && probe && counted {
					sentInWin.Add(1)
					k := probeKey{c.ulid, seq}
					latMu.Lock()
					if _, ok := recvAt[k]; ok {
						hopLats = append(hopLats, 0)
						delete(recvAt, k)
					} else {
						storedAt[k] = time.Now().UnixNano()
					}
					latMu.Unlock()
				}
				if counted {
					pubMu.Lock()
					pubLats = append(pubLats, float64(time.Since(t0).Microseconds())/1000)
					pubMu.Unlock()
				}
				if err != nil {
					if failed.Add(1) == 1 {
						fmt.Fprintln(os.Stderr, "fanout: first failed publish:", err)
					}
					continue
				}
				published.Add(1)
			}
		}(i, c)
	}

	// Warm up until every child has delivered a probe, or the warm-up ends.
	warmEnd := time.Now().Add(p.Warmup)
	for time.Now().Before(warmEnd) {
		seenMu.Lock()
		n := len(seen)
		seenMu.Unlock()
		if n >= p.Children {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	seenMu.Lock()
	connected := len(seen)
	seenMu.Unlock()
	// Then let the start-up backlog drain, so the window sees the steady state.
	time.Sleep(20 * time.Second)

	kind := "colcad"
	if p.Protocol {
		kind = "protocol"
	}
	r := NewReport("fanout", p.Storage, map[string]any{
		"children": p.Children, "child_rate": p.ChildRate, "duration": p.Duration.String(), "child_kind": kind,
	})
	limitedBefore := scrapeCounter(hub.hc, hubAPI, "colca_http_request_limited_total")
	cpuBefore, _ := cpuSeconds(hub.cmd.Process.Pid)
	start := time.Now()
	winStart.Store(start.UnixNano())
	published.Store(0)

	var profWG sync.WaitGroup
	if p.ProfileDir != "" && pprofAddr != "" {
		profWG.Add(1)
		go func() {
			defer profWG.Done()
			captureProfiles(pprofAddr, p.ProfileDir, p.Duration)
		}()
	}

	// Sample the parent: RSS and how long its /metrics takes to answer.
	var peakRSS uint64
	var scrapeMS []float64
	end := start.Add(p.Duration)
	for time.Now().Before(end) {
		if rss, err := RSSBytes(hub.cmd.Process.Pid); err == nil && rss > peakRSS {
			peakRSS = rss
		}
		t0 := time.Now()
		_ = scrapeCounter(hub.hc, hubAPI, "colca_http_request_limited_total")
		scrapeMS = append(scrapeMS, float64(time.Since(t0).Milliseconds()))
		time.Sleep(min(5*time.Second, time.Until(end)))
	}
	winEnd.Store(time.Now().UnixNano())
	elapsed := time.Since(start)
	publishedInWin := published.Load()
	cpuAfter, _ := cpuSeconds(hub.cmd.Process.Pid)
	limitedAfter := scrapeCounter(hub.hc, hubAPI, "colca_http_request_limited_total")
	profWG.Wait()
	// Let the window's late probes arrive; whatever is still missing then is lost.
	for grace := time.Now().Add(60 * time.Second); time.Now().Before(grace) && received.Load() < sentInWin.Load(); {
		time.Sleep(250 * time.Millisecond)
	}

	latMu.Lock()
	defer latMu.Unlock()
	sort.Float64s(lats)
	sort.Float64s(scrapeMS)
	r.Metrics["fanout_children_connected"] = float64(connected)
	r.Metrics["fanout_published_per_sec"] = float64(publishedInWin) / elapsed.Seconds()
	r.Metrics["fanout_publish_failed"] = float64(failed.Load())
	r.Metrics["fanout_probes_lost"] = float64(sentInWin.Load() - received.Load())
	pubMu.Lock()
	sort.Float64s(pubLats)
	r.Metrics["fanout_child_store_p50_ms"] = Percentile(pubLats, 50)
	r.Metrics["fanout_child_store_p95_ms"] = Percentile(pubLats, 95)
	pubMu.Unlock()
	sort.Float64s(hopLats)
	r.Metrics["fanout_hop_p50_ms"] = Percentile(hopLats, 50)
	r.Metrics["fanout_hop_p95_ms"] = Percentile(hopLats, 95)
	r.Metrics["fanout_hop_p99_ms"] = Percentile(hopLats, 99)
	r.Metrics["fanout_p50_ms"] = Percentile(lats, 50)
	r.Metrics["fanout_p95_ms"] = Percentile(lats, 95)
	r.Metrics["fanout_p99_ms"] = Percentile(lats, 99)
	if len(lats) > 0 {
		r.Metrics["fanout_max_ms"] = lats[len(lats)-1]
	}
	r.Metrics["fanout_parent_cpu_pct"] = 100 * (cpuAfter - cpuBefore) / elapsed.Seconds()
	r.Metrics["fanout_parent_rss_mb"] = float64(peakRSS) / (1 << 20)
	if len(scrapeMS) > 0 {
		r.Metrics["fanout_metrics_scrape_max_ms"] = scrapeMS[len(scrapeMS)-1]
	}
	r.Metrics["fanout_parent_limited_per_sec"] = (limitedAfter - limitedBefore) / elapsed.Seconds()
	return r, nil
}

// inWindow reports whether a probe sent at ns falls in the measurement window.
func inWindow(start, end *atomic.Int64, ns int64) bool {
	s, e := start.Load(), end.Load()
	return s != 0 && ns >= s && (e == 0 || ns < e)
}

func waitHealthy(hc *http.Client, apiAddr string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+apiAddr+"/healthz", nil)
		resp, err := hc.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("node on %s never became healthy", apiAddr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// enrollAtHub places an element at path and enrolls a fresh key bound to it.
func enrollAtHub(hub *fanoutNode, dir, path, kind string, grants ...string) (*benchIdentity, error) {
	id, err := identity.Generate(filepath.Join(dir, path+".key"))
	if err != nil {
		return nil, err
	}
	if err := enrollKey(hub, path, path, kind, id.PublicHex(), grants...); err != nil {
		return nil, err
	}
	cert, err := id.SelfSignedCert(path)
	if err != nil {
		return nil, err
	}
	return &benchIdentity{id: id, cert: cert}, nil
}

func enrollNodeAtHub(hub *fanoutNode, path, ulid, pubHex string) error {
	return enrollKey(hub, path, ulid, "node", pubHex)
}

func enrollKey(hub *fanoutNode, path, ulid, kind, pubHex string, grants ...string) error {
	element := elementIDFor(path)
	placed, _ := json.Marshal(map[string]any{
		"topic":   uns.Prefix() + "_SystemElement/n-hub/" + path,
		"payload": map[string]string{"id": element, "name": path},
	})
	if err := postAdmin(hub.hc, hub.addrs["api"], "/publish", placed); err != nil {
		return fmt.Errorf("place %s: %w", path, err)
	}
	entry, _ := json.Marshal(map[string]any{"ulid": ulid, "pubkey": pubHex, "kind": kind, "element": element, "grants": grants})
	if err := postAdmin(hub.hc, hub.addrs["api"], "/enroll", entry); err != nil {
		return fmt.Errorf("enroll %s: %w", ulid, err)
	}
	return nil
}

func publishOnce(hc *http.Client, apiAddr string, body []byte) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://"+apiAddr+"/publish", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Colca-Token", BenchToken)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("publish: HTTP %d: %s", resp.StatusCode, msg)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// scrapeCounter sums every series of a counter on a node's /metrics, or -1 when
// the scrape fails.
func scrapeCounter(hc *http.Client, apiAddr, name string) float64 {
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+apiAddr+"/metrics", nil)
	resp, err := hc.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	sum := 0.0
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name) {
			continue
		}
		f := strings.Fields(line)
		if v, err := strconv.ParseFloat(f[len(f)-1], 64); err == nil {
			sum += v
		}
	}
	return sum
}

// cpuSeconds is the user+system CPU time pid has used so far.
func cpuSeconds(pid int) (float64, error) {
	out, err := exec.CommandContext(context.Background(), "ps", "-o", "time=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	// [[dd-]hh:]mm:ss.cc
	s := strings.TrimSpace(string(out))
	days := 0.0
	if i := strings.IndexByte(s, '-'); i >= 0 {
		d, _ := strconv.ParseFloat(s[:i], 64)
		days, s = d, s[i+1:]
	}
	total := 0.0
	for _, part := range strings.Split(s, ":") {
		v, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return 0, fmt.Errorf("parse ps time %q: %w", out, err)
		}
		total = total*60 + v
	}
	return days*86400 + total, nil
}

// captureProfiles saves the parent's CPU profile over most of the window and its
// mutex, block, goroutine and heap profiles at the end.
func captureProfiles(addr, dir string, window time.Duration) {
	_ = os.MkdirAll(dir, 0o750)
	secs := max(int(window.Seconds())-5, 5)
	get := func(path, file string, timeout time.Duration) {
		hc := &http.Client{Timeout: timeout}
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+path, nil)
		resp, err := hc.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, "profile", path, err)
			return
		}
		defer resp.Body.Close()
		f, err := os.Create(filepath.Join(dir, file))
		if err != nil {
			return
		}
		defer func() { _ = f.Close() }()
		_, _ = io.Copy(f, resp.Body)
	}
	// Mutex and block profiles as deltas over the same window, so set-up does not count.
	var wg sync.WaitGroup
	for path, file := range map[string]string{
		"/debug/pprof/profile": "cpu.pprof", "/debug/pprof/mutex": "mutex.pprof", "/debug/pprof/block": "block.pprof",
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			get(fmt.Sprintf("%s?seconds=%d", path, secs), file, time.Duration(secs+60)*time.Second)
		}()
	}
	wg.Wait()
	get("/debug/pprof/goroutine", "goroutine.pprof", time.Minute)
	get("/debug/pprof/goroutine?debug=1", "goroutine.txt", time.Minute)
	get("/debug/pprof/heap", "heap.pprof", time.Minute)
}

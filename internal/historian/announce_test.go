package historian

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
)

func startLocalNode(t *testing.T) *node.Node {
	t.Helper()
	base := t.TempDir()
	keyFile := filepath.Join(base, "n.key")
	if _, err := identity.Generate(keyFile); err != nil {
		t.Fatal(err)
	}
	n, err := node.Start(&config.Config{
		ULID:      "n-hist",
		DataDir:   filepath.Join(base, "data"),
		Identity:  config.Identity{KeyFile: keyFile},
		API:       config.API{LocalAddr: "127.0.0.1:0"},
		MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	return n
}

// serviceActive reads the historian's _ServiceDetails from the node's KV;
// found is false while there is none.
func serviceActive(t *testing.T, client *door.Client) (active, found bool) {
	t.Helper()
	entries, err := client.KV(context.Background(), "", "_ServiceDetails")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		var d serviceDetails
		if err := json.Unmarshal(e.Payload, &d); err != nil {
			t.Fatal(err)
		}
		if d.Name == "historian" {
			return d.IsActive, true
		}
	}
	return false, false
}

func waitActive(t *testing.T, client *door.Client, want bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if active, found := serviceActive(t, client); found && active == want {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("historian _ServiceDetails never reached is_active=%v", want)
}

func TestAnnouncerPublishesActiveThenInactiveOnShutdown(t *testing.T) {
	n := startLocalNode(t)
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&Announcer{Door: client, MQTTURL: "tcp://" + n.MQTTLocalAddr}).Run(ctx)
	}()

	waitActive(t, client, true)
	cancel()
	<-done
	waitActive(t, client, false)
}

func TestAnnouncerDeclaresTheHistorianACoreService(t *testing.T) {
	n := startLocalNode(t)
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go (&Announcer{Door: client, MQTTURL: "tcp://" + n.MQTTLocalAddr, Version: "0.17.2"}).Run(ctx)

	waitActive(t, client, true)
	entries, err := client.KV(context.Background(), "", "_ServiceDetails")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		var d serviceDetails
		if err := json.Unmarshal(e.Payload, &d); err != nil {
			t.Fatal(err)
		}
		if d.Name == "historian" {
			if got := d.Metadata["app_class"]; got != "core" {
				t.Fatalf("metadata.app_class = %v, want core", got)
			}
			if got := d.Metadata["version"]; got != "0.17.2" {
				t.Fatalf("metadata.version = %v, want 0.17.2", got)
			}
			return
		}
	}
	t.Fatal("no historian _ServiceDetails")
}

func TestAnnouncerLastWillMarksACrashedServiceInactive(t *testing.T) {
	n := startLocalNode(t)
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}

	var mu sync.Mutex
	var conn net.Conn
	dialed := false
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if dialed {
			return nil, net.ErrClosed // stay down, so the will is the last word
		}
		dialed = true
		c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		conn = c
		return c, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go (&Announcer{Door: client, MQTTURL: "tcp://" + n.MQTTLocalAddr, Dial: dial}).Run(ctx)

	waitActive(t, client, true)
	mu.Lock()
	_ = conn.Close() // no DISCONNECT: the broker sends the will
	mu.Unlock()
	waitActive(t, client, false)
}

func TestRuntimeTelemetryDoesNotAppendRegistration(t *testing.T) {
	n := startLocalNode(t)
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	a := &Announcer{Door: client, MQTTURL: "tcp://" + n.MQTTLocalAddr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Run(ctx)
	waitActive(t, client, true)
	before := n.Store.NextOffset("entities")
	a.Report(true, "")
	a.Report(false, "database unreachable")
	a.ReportProgress(map[string]any{"run_id": "run", "processed_at": 1000.0, "ready": true, "observed_at": 10000.0})
	if after := n.Store.NextOffset("entities"); after != before {
		t.Fatalf("runtime telemetry appended entities: %d -> %d", before, after)
	}
	var output strings.Builder
	a.WriteMetrics(&output)
	if !strings.Contains(output.String(), "colca_service_healthy{service=\"historian\"} 0") || !strings.Contains(output.String(), "colca_application_processed_timestamp_seconds{service=\"historian\"} 1000") {
		t.Fatal(output.String())
	}
}

func TestTheHealthDoorFollowsTheNodesCursorLagFinding(t *testing.T) {
	base := t.TempDir()
	keyFile := filepath.Join(base, "n.key")
	if _, err := identity.Generate(keyFile); err != nil {
		t.Fatal(err)
	}
	after := config.Duration(time.Second)
	n, err := node.Start(&config.Config{
		ULID:      "n-hist",
		DataDir:   filepath.Join(base, "data"),
		Identity:  config.Identity{KeyFile: keyFile},
		API:       config.API{LocalAddr: "127.0.0.1:0"},
		MQTTLocal: config.Endpoint{Addr: "127.0.0.1:0"},
		Contracts: config.Contracts{Bundle: contractstest.GeneratedBundlePath(t)},
		Cursors:   config.Cursors{LagAlarmAfter: &after},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.Stop)
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &Announcer{Door: client, MQTTURL: "tcp://" + n.MQTTLocalAddr}
	go a.Run(ctx)
	waitActive(t, client, true)

	publish := func(value int) {
		t.Helper()
		body := map[string]any{"signal_id": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "value": value, "timestamp": float64(time.Now().UnixMilli()) / 1000}
		if err := client.Publish(ctx, "colca/v1/_Metric/n-hist/line/s1", body); err != nil {
			t.Fatal(err)
		}
	}
	readAll := func() {
		t.Helper()
		page, err := client.Fetch(ctx, "metrics", Cursor, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Records) > 0 {
			if _, err := client.Ack(ctx, "metrics", Cursor, page.Records[len(page.Records)-1].Offset); err != nil {
				t.Fatal(err)
			}
		}
	}
	waitLag := func(standing bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if (a.CursorLag() != "") == standing {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("cursor lag standing never became %v", standing)
	}

	publish(1)
	readAll() // the cursor exists from here on
	publish(2)
	waitLag(true)
	readAll()
	waitLag(false)
}

type blockingMetricsWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingMetricsWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestSlowMetricsReaderDoesNotBlockClockProgress(t *testing.T) {
	a := &Announcer{}
	w := &blockingMetricsWriter{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { a.WriteMetrics(w); close(done) }()
	defer func() { close(w.release); <-done }()
	<-w.entered
	progressDone := make(chan struct{})
	go func() {
		a.ReportProgress(map[string]any{"processed_at": 1000.0, "ready": true})
		close(progressDone)
	}()
	select {
	case <-progressDone:
	case <-time.After(time.Second):
		t.Fatal("slow metrics response blocked functional clock progress")
	}
}

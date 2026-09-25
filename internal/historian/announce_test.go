package historian

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/config"
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
		KeyFile:   keyFile,
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

// serviceStatus reads architecture_metadata.status and detail from the
// historian's _ServiceDetails.
func serviceStatus(t *testing.T, client *door.Client) (status, detail string) {
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
			status, _ = d.ArchitectureMetadata["status"].(string)
			detail, _ = d.ArchitectureMetadata["detail"].(string)
			return status, detail
		}
	}
	return "", ""
}

func waitStatus(t *testing.T, client *door.Client, want, wantDetail string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var status, detail string
	for time.Now().Before(deadline) {
		if status, detail = serviceStatus(t, client); status == want && detail == wantDetail {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("historian status = %q (%q), want %q (%q)", status, detail, want, wantDetail)
}

func TestAnnouncerPublishesItsStatusOnChangeOnly(t *testing.T) {
	n := startLocalNode(t)
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	a := &Announcer{Door: client, MQTTURL: "tcp://" + n.MQTTLocalAddr}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.Run(ctx)
	}()

	waitStatus(t, client, StatusStarting, "")
	a.Report(true, "")
	waitStatus(t, client, StatusHealthy, "")
	a.Report(false, "database unreachable")
	waitStatus(t, client, StatusUnhealthy, "database unreachable")
	a.Report(true, "")
	waitStatus(t, client, StatusHealthy, "")

	cancel()
	<-done
	waitActive(t, client, false)
	waitStatus(t, client, StatusHealthy, "") // a clean stop keeps the last status
}

func TestAnnouncerRepeatedStatusIsNotRepublished(t *testing.T) {
	n := startLocalNode(t)
	client := &door.Client{BaseURL: "http://" + n.LocalAPIAddr, Service: "historian"}
	a := &Announcer{Door: client, MQTTURL: "tcp://" + n.MQTTLocalAddr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.Run(ctx)

	a.Report(false, "first reason")
	waitStatus(t, client, StatusUnhealthy, "first reason")
	a.Report(false, "second reason")
	time.Sleep(500 * time.Millisecond)
	if status, detail := serviceStatus(t, client); status != StatusUnhealthy || detail != "first reason" {
		t.Fatalf("a repeated status was republished: %q (%q)", status, detail)
	}
}

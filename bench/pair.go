// Package bench hosts the benchmark-gate scenarios and the shared harness they
// run against: an in-process, restartable hub←edge topology (Pair) built from
// the same node.Start/config.Config primitives proven in tests/integration_test.go.
package bench

import (
	"fmt"
	"path/filepath"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
)

const (
	BenchToken    = "bench-admin-token"
	machineSecret = "bench-machine-secret"
	observerTok   = "bench-observer-secret"
)

// Pair is a running 2-level topology: hub ← mTLS ← edge, with `machines` MQTT
// clients configured at the edge and an observer at the hub. It is the standard
// fixture for every scenario except footprint (which needs the real binary).
type Pair struct {
	Hub, Edge       *node.Node
	HubCfg, EdgeCfg *config.Config
	Machines        int
}

// StartPair builds and starts a hub and an edge, mTLS-linked with the edge
// mounted at "edge1" on the hub, and returns them running. It sets up
// `machines` MQTT clients on the edge (m1..mN, each mounted at its own ulid)
// plus a mount-less observer client on the hub.
func StartPair(dir string, machines int) (*Pair, error) {
	hubKey := filepath.Join(dir, "hub.key")
	edgeKey := filepath.Join(dir, "edge.key")
	hubID, err := identity.Generate(hubKey)
	if err != nil {
		return nil, err
	}
	edgeID, err := identity.Generate(edgeKey)
	if err != nil {
		return nil, err
	}

	hubCfg := &config.Config{
		ULID: "n-hub", DataDir: filepath.Join(dir, "hub-data"), KeyFile: hubKey,
		API:      config.API{Addr: "127.0.0.1:0", Token: BenchToken},
		MQTT:     config.Endpoint{Addr: "127.0.0.1:0"},
		Repl:     config.Endpoint{Addr: "127.0.0.1:0"},
		Children: []config.Child{{ULID: "n-edge", Pubkey: edgeID.PublicHex(), Mount: "edge1"}},
		Clients:  []config.Client{{ULID: "observer", Token: observerTok}},
	}
	hub, err := node.Start(hubCfg)
	if err != nil {
		return nil, fmt.Errorf("start hub: %w", err)
	}
	// Pin the resolved repl address so StartHub after StopHub rebinds the SAME
	// port — the edge's parent URL stays valid across the restart.
	hubCfg.Repl.Addr = hub.ReplAddr

	clients := make([]config.Client, 0, machines)
	for i := 1; i <= machines; i++ {
		ulid := fmt.Sprintf("m%d", i)
		clients = append(clients, config.Client{ULID: ulid, Token: machineSecret, Mount: ulid})
	}
	edgeCfg := &config.Config{
		ULID: "n-edge", DataDir: filepath.Join(dir, "edge-data"), KeyFile: edgeKey,
		API:     config.API{Addr: "127.0.0.1:0", Token: BenchToken},
		MQTT:    config.Endpoint{Addr: "127.0.0.1:0"},
		Parent:  &config.Parent{URL: "https://" + hub.ReplAddr, Pubkey: hubID.PublicHex()},
		Clients: clients,
	}
	edge, err := node.Start(edgeCfg)
	if err != nil {
		hub.Stop()
		return nil, fmt.Errorf("start edge: %w", err)
	}
	return &Pair{Hub: hub, Edge: edge, HubCfg: hubCfg, EdgeCfg: edgeCfg, Machines: machines}, nil
}

// StopHub stops the hub only, leaving the edge running (and buffering).
func (p *Pair) StopHub() { p.Hub.Stop() }

// StartHub restarts the hub on the SAME data dir and repl address recorded at
// StartPair time, so the edge's uplink reconnects without reconfiguration.
func (p *Pair) StartHub() error {
	hub, err := node.Start(p.HubCfg)
	if err != nil {
		return fmt.Errorf("restart hub: %w", err)
	}
	p.Hub = hub
	return nil
}

// Stop tears down both nodes. Safe to call after StopHub (hub.Stop is
// idempotent).
func (p *Pair) Stop() {
	p.Edge.Stop()
	p.Hub.Stop()
}

// Machine returns a connected QoS-capable client for m{i} (1-based).
func (p *Pair) Machine(i int) (pahomqtt.Client, error) {
	ulid := fmt.Sprintf("m%d", i)
	return connect(p.Edge.MQTTAddr, ulid, ulid, machineSecret)
}

// Observer returns a read-only observer session on the HUB broker.
func (p *Pair) Observer(clientID string) (pahomqtt.Client, error) {
	return connect(p.Hub.MQTTAddr, clientID, "observer", observerTok)
}

func connect(addr, clientID, user, pass string) (pahomqtt.Client, error) {
	opts := pahomqtt.NewClientOptions().AddBroker("tcp://" + addr).
		SetClientID(clientID).SetUsername(user).SetPassword(pass).
		SetOrderMatters(false).SetConnectTimeout(5 * time.Second)
	c := pahomqtt.NewClient(opts)
	tk := c.Connect()
	if !tk.WaitTimeout(10*time.Second) || tk.Error() != nil {
		return nil, fmt.Errorf("connect %s to %s: %w", clientID, addr, tk.Error())
	}
	return c, nil
}

// NextOffset reads a stream's next offset directly from a node's store.
func NextOffset(n *node.Node, stream string) uint64 { return n.Store.NextOffset(stream) }

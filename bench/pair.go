// Package bench hosts the benchmark-gate scenarios and the shared harness they
// run against: an in-process, restartable hub←edge topology (Pair) built from
// the same node.Start/config.Config primitives proven in tests/integration_test.go.
package bench

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/node"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const BenchToken = "bench-admin-token"

// benchIdentity is one enrolled machine identity: key + TLS client cert.
type benchIdentity struct {
	id   *identity.Identity
	cert tls.Certificate
}

// Pair is a running 2-level topology: hub ← mTLS ← edge, with `machines`
// enrolled MQTT identities at the edge and a read-all observer at the hub. It
// is the standard fixture for every scenario except footprint (which needs
// the real binary).
type Pair struct {
	Hub, Edge       *node.Node
	HubCfg, EdgeCfg *config.Config
	Machines        int
	machineIDs      map[string]*benchIdentity
	observerID      *benchIdentity
}

// enroll generates a key for ulid, enrolls it (kind machine) at reg and
// returns the identity.
func enroll(dir string, reg *registry.Manager, ulid, mount string, grants ...string) (*benchIdentity, error) {
	id, err := identity.Generate(filepath.Join(dir, ulid+".key"))
	if err != nil {
		return nil, err
	}
	cert, err := id.SelfSignedCert(ulid)
	if err != nil {
		return nil, err
	}
	entry, err := json.Marshal(uns.Entry{ULID: ulid, Pubkey: id.PublicHex(), Kind: uns.KindMachine, Mount: mount, Grants: grants})
	if err != nil {
		return nil, err
	}
	if _, _, err := reg.Enroll(entry); err != nil {
		return nil, fmt.Errorf("enroll %s: %w", ulid, err)
	}
	return &benchIdentity{id: id, cert: cert}, nil
}

// StartPair builds and starts a hub and an edge, mTLS-linked with the edge
// mounted at "edge1" on the hub, and returns them running. It enrolls
// `machines` MQTT identities at the edge (m1..mN, each mounted at its own
// ulid) plus a mount-less read-all observer at the hub.
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
		API:  config.API{Addr: "127.0.0.1:0", Token: BenchToken},
		MQTT: config.Endpoint{Addr: "127.0.0.1:0"},
		Repl: config.Endpoint{Addr: "127.0.0.1:0"},
	}
	hub, err := node.Start(hubCfg)
	if err != nil {
		return nil, fmt.Errorf("start hub: %w", err)
	}
	// Pin the resolved repl address so StartHub after StopHub rebinds the SAME
	// port — the edge's parent URL stays valid across the restart.
	hubCfg.Repl.Addr = hub.ReplAddr

	p := &Pair{Hub: hub, HubCfg: hubCfg, Machines: machines, machineIDs: map[string]*benchIdentity{}}
	p.observerID, err = enroll(dir, hub.Registry, "observer", "", "read:#")
	if err != nil {
		hub.Stop()
		return nil, err
	}
	// The edge's node key must be enrolled at the hub before the edge dials in.
	childEntry, err := json.Marshal(uns.Entry{ULID: "n-edge", Pubkey: edgeID.PublicHex(), Kind: uns.KindNode, Mount: "edge1"})
	if err != nil {
		hub.Stop()
		return nil, err
	}
	if _, _, err := hub.Registry.Enroll(childEntry); err != nil {
		hub.Stop()
		return nil, fmt.Errorf("enroll edge at hub: %w", err)
	}

	edgeCfg := &config.Config{
		ULID: "n-edge", DataDir: filepath.Join(dir, "edge-data"), KeyFile: edgeKey,
		API:    config.API{Addr: "127.0.0.1:0", Token: BenchToken},
		MQTT:   config.Endpoint{Addr: "127.0.0.1:0"},
		Parent: &config.Parent{URL: "https://" + hub.ReplAddr, Pubkey: hubID.PublicHex()},
	}
	edge, err := node.Start(edgeCfg)
	if err != nil {
		hub.Stop()
		return nil, fmt.Errorf("start edge: %w", err)
	}
	p.Edge, p.EdgeCfg = edge, edgeCfg
	for i := 1; i <= machines; i++ {
		ulid := fmt.Sprintf("m%d", i)
		mid, err := enroll(dir, edge.Registry, ulid, ulid)
		if err != nil {
			edge.Stop()
			hub.Stop()
			return nil, err
		}
		p.machineIDs[ulid] = mid
	}
	return p, nil
}

// StopHub stops the hub only, leaving the edge running (and buffering).
func (p *Pair) StopHub() { p.Hub.Stop() }

// StartHub restarts the hub on the SAME data dir and repl address recorded at
// StartPair time, so the edge's uplink reconnects without reconfiguration.
// Enrollments live in the store, so they survive the restart.
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
	mid, ok := p.machineIDs[ulid]
	if !ok {
		return nil, fmt.Errorf("machine %s not enrolled (Machines=%d)", ulid, p.Machines)
	}
	return connect(p.Edge.MQTTAddr, ulid, ulid, mid)
}

// Observer returns a read-only observer session on the HUB broker.
func (p *Pair) Observer(clientID string) (pahomqtt.Client, error) {
	return connect(p.Hub.MQTTAddr, clientID, "observer", p.observerID)
}

func connect(addr, clientID, user string, bid *benchIdentity) (pahomqtt.Client, error) {
	tlsCfg := &tls.Config{
		Certificates:       []tls.Certificate{bid.cert},
		InsecureSkipVerify: true, // #nosec G402 -- pinning model, no CA (registry is the trust store)
		MinVersion:         tls.VersionTLS13,
	}
	opts := pahomqtt.NewClientOptions().AddBroker("ssl://" + addr).
		SetTLSConfig(tlsCfg).
		SetClientID(clientID).SetUsername(user).
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

// apiTransport accepts the nodes' self-signed API certs (pinning model, no CA).
func apiTransport() *http.Transport {
	return &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true, // #nosec G402 -- pinning model, no CA (registry is the trust store)
		MinVersion:         tls.VersionTLS13,
	}}
}

// Package node assembles a complete Colca node out of the component packages
// and owns its lifecycle. Start wires identity, store, embedded broker, engine,
// HTTP API, replication server and the uplink/downlink loops in one order that
// resolves the engine/broker cycle (the broker is built first with a nil engine
// and gets it late-bound via SetEngine). Stop tears everything down again so the
// SAME data dir and the SAME ports can be reused by an immediately following
// Start — restarting a node is a first-class operation, not a leak.
package node

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/httpapi"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/mqttsrv"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/store"
)

// Node is a running Colca node. The three *Addr fields carry the RESOLVED
// listener addresses (a config may ask for "127.0.0.1:0"); each is empty when
// the node has no such listener.
type Node struct {
	Cfg     *config.Config
	Store   *store.Store
	Engine  *engine.Engine
	MQTT    *mqttsrv.Server
	ReplSrv *repl.Server

	APIAddr  string // resolved HTTP API address ("" if no api configured)
	ReplAddr string // resolved replication address ("" if this node has no children)
	MQTTAddr string // resolved MQTT address ("" if no mqtt configured)

	stop     chan struct{}
	stopOnce sync.Once
	// wg tracks everything that touches the store outside of a listener the
	// server packages own: the repl loops and in-flight API handlers. Stop waits
	// for it before closing the store — Pebble panics on use after Close.
	wg      sync.WaitGroup
	httpSrv *http.Server
	apiLn   net.Listener
}

// Start builds and starts a node from cfg. On any failure after the store is
// open, everything already started is closed again before returning, so no
// listener stays bound and no Pebble directory stays locked.
func Start(cfg *config.Config) (*Node, error) {
	lvl := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		lvl = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))

	id, err := identity.Load(cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("node %s: load key %s: %w", cfg.ULID, cfg.KeyFile, err)
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("node %s: open store %s: %w", cfg.ULID, cfg.DataDir, err)
	}

	n := &Node{Cfg: cfg, Store: st, stop: make(chan struct{})}
	log := slog.Default().With("node", cfg.ULID, "comp", "node")
	// From here on every error path unwinds through Stop.
	fail := func(err error) (*Node, error) {
		n.Stop()
		return nil, err
	}

	// 1. Broker first: New binds the socket, so MQTTAddr is known before Serve
	//    and before the engine exists. The engine is late-bound below.
	if cfg.MQTT.Addr != "" {
		mq, err := mqttsrv.New(cfg, nil)
		if err != nil {
			return fail(fmt.Errorf("node %s: mqtt listen %s: %w", cfg.ULID, cfg.MQTT.Addr, err))
		}
		n.MQTT = mq
		n.MQTTAddr = mq.Addr()
	}

	// 2. Engine: delivers downlinked commands into the local broker when there
	//    is one (a node without MQTT simply persists them).
	var deliver engine.LocalDeliver
	if n.MQTT != nil {
		deliver = n.MQTT.DeliverLocal
	}
	n.Engine = engine.New(st, cfg, deliver)

	if n.MQTT != nil {
		n.MQTT.SetEngine(n.Engine)
		go func(mq *mqttsrv.Server) {
			if err := mq.Serve(); err != nil {
				log.Error("mqtt server stopped", "err", err)
			}
		}(n.MQTT)
		// Re-seed the broker's retained set from the KV projection. The two are
		// ONE contract seen from two sides (engine.retainFor retains exactly the
		// classes that project into KV), but mochi's retained store is in-memory:
		// without this replay a restarted node comes back with an intact KV view
		// and an EMPTY retained set, silently breaking the "fresh subscriber gets
		// the current state on connect" guarantee the bus makes.
		for _, en := range st.KVScan("") {
			n.MQTT.DeliverLocal(en.Topic, en.Payload, true)
		}
	}

	// 3. Local HTTP control API.
	if cfg.API.Addr != "" {
		ln, err := net.Listen("tcp", cfg.API.Addr)
		if err != nil {
			return fail(fmt.Errorf("node %s: api listen %s: %w", cfg.ULID, cfg.API.Addr, err))
		}
		n.apiLn = ln
		n.APIAddr = ln.Addr().String()
		n.httpSrv = &http.Server{Handler: n.trackInflight(httpapi.Handler(n.Engine, cfg))}
		go func(srv *http.Server, ln net.Listener) {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("api server stopped", "err", err)
			}
		}(n.httpSrv, ln)
	}

	// 4. Replication server — only meaningful for a node that has children.
	if len(cfg.Children) > 0 && cfg.Repl.Addr != "" {
		rs, err := repl.NewServer(cfg, n.Engine, id)
		if err != nil {
			return fail(fmt.Errorf("node %s: repl server: %w", cfg.ULID, err))
		}
		n.ReplSrv = rs
		addr, err := rs.Start()
		if err != nil {
			return fail(fmt.Errorf("node %s: repl listen %s: %w", cfg.ULID, cfg.Repl.Addr, err))
		}
		n.ReplAddr = addr
	}

	// 5. Uplink + downlink loops towards the parent.
	if cfg.Parent != nil {
		cl, err := repl.NewClient(cfg.Parent.URL, cfg.Parent.Pubkey, id)
		if err != nil {
			return fail(fmt.Errorf("node %s: repl client for %s: %w", cfg.ULID, cfg.Parent.URL, err))
		}
		n.wg.Add(2)
		go func() {
			defer n.wg.Done()
			repl.RunUplink(cl, n.Engine, n.stop)
		}()
		go func() {
			defer n.wg.Done()
			repl.RunDownlink(cl, n.Engine, n.stop)
		}()
	}

	log.Info("colca node started", "ulid", cfg.ULID, "api", n.APIAddr, "repl", n.ReplAddr, "mqtt", n.MQTTAddr)
	return n, nil
}

// trackInflight makes every API request visible to Stop, so the store is never
// closed underneath a running handler.
func (n *Node) trackInflight(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.wg.Add(1)
		defer n.wg.Done()
		h.ServeHTTP(w, r)
	})
}

// Stop shuts the node down and releases every resource it holds: when it
// returns, the API and repl ports are re-bindable and the data dir is
// re-openable. It is safe to call more than once (Pebble panics on a second
// Close, so the whole teardown — not just the stop channel — runs exactly once).
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		close(n.stop)
		// Close, never Shutdown: Shutdown waits for in-flight requests and keeps
		// the port bound meanwhile, which breaks an immediate restart.
		if n.httpSrv != nil {
			_ = n.httpSrv.Close()
		}
		if n.apiLn != nil {
			_ = n.apiLn.Close() // idempotent; guarantees the port is free
		}
		if n.ReplSrv != nil {
			n.ReplSrv.Stop()
		}
		// Everything that reads or writes the store must be finished before the
		// store goes away.
		n.wg.Wait()
		if n.MQTT != nil {
			_ = n.MQTT.Close()
		}
		if n.Store != nil {
			_ = n.Store.Close()
		}
	})
}

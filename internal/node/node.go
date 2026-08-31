// Package node assembles a complete Colca node out of the component packages
// and owns its lifecycle. Start wires identity, store, embedded broker, engine,
// HTTP API, replication server and the uplink/downlink loops in one order that
// resolves the engine/broker cycle (the broker is built first with a nil engine
// and gets it late-bound via SetEngine). Stop tears everything down again so the
// SAME data dir and the SAME ports can be reused by an immediately following
// Start — restarting a node is a first-class operation, not a leak.
package node

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobgc"
	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/clock"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/httpapi"
	"github.com/alpamayo-solutions/colca/internal/httpserver"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/mqttsrv"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/retention"
	"github.com/alpamayo-solutions/colca/internal/secretstore"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/internal/tokenauth"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Node is a running Colca node. The three *Addr fields carry the RESOLVED
// listener addresses (a config may ask for "127.0.0.1:0"); each is empty when
// the node has no such listener.
type Node struct {
	Cfg      *config.Config
	Store    *store.Store
	Secrets  *secretstore.Store
	Blobs    *blobstore.Store
	Engine   *engine.Engine
	MQTT     *mqttsrv.Server
	ReplSrv  *repl.Server
	Metrics  *metrics.Metrics
	Registry *registry.Manager

	APIAddr  string // resolved HTTP API address ("" if no api configured)
	ReplAddr string // resolved replication address ("" if this node has no children)
	MQTTAddr string // resolved MQTT address ("" if no mqtt configured)
	// MQTTLocalAddr and LocalAPIAddr are the resolved local-door addresses
	// ("" if not configured). Unlike the other *Addr fields these are never
	// meant to be published (local-service-trust design §4) — they exist for
	// local services' own configuration and for tests.
	MQTTLocalAddr string
	LocalAPIAddr  string
	// Human doors (human-authz design §5.1); "" when not configured.
	MQTTHumanTCPAddr string
	MQTTHumanWSAddr  string

	stop     chan struct{}
	stopOnce sync.Once
	// wg tracks everything that touches the store outside of a listener the
	// server packages own: the repl loops and in-flight API handlers. Stop waits
	// for it before closing the store — Pebble panics on use after Close.
	wg          sync.WaitGroup
	httpSrv     *http.Server
	apiLn       net.Listener
	localAPISrv *http.Server
	localAPILn  net.Listener
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

	// First boot mints this node's identity; every later boot loads it. The key
	// must not come from the generated deployment directory — that directory is
	// tarred, signed and published as a revision (design §5).
	id, minted, err := identity.LoadOrGenerate(cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("node %s: key %s: %w", cfg.ULID, cfg.KeyFile, err)
	}
	if minted {
		// At INFO with the pubkey, because this is the moment an operator needs
		// it: nothing can enroll this node until its parent holds this key.
		slog.Info("minted this node's identity — enroll it at its parent",
			"node", cfg.ULID, "key_file", cfg.KeyFile, "pubkey", id.PublicHex())
	}
	st, err := store.Open(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("node %s: open store %s: %w", cfg.ULID, cfg.DataDir, err)
	}
	st.SetMaxRecordBytes(cfg.Limits.EffectiveMaxRecordBytes())
	var secretDB *secretstore.Store
	if cfg.SecretsDir != "" {
		secretDB, err = secretstore.Open(cfg.SecretsDir)
		if err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("node %s: open secret store %s: %w", cfg.ULID, cfg.SecretsDir, err)
		}
	}

	// clk is this node's authoritative-time state (time-sync design §2.1): a
	// node with no configured parent is the root/authority. Built once and
	// shared between Metrics (scrape-time gauges) and Engine (offset
	// read/write) — they must be the SAME instance (engine.New's doc
	// comment).
	clk := clock.New(cfg.Parent == nil, time.Now)
	n := &Node{Cfg: cfg, Store: st, Secrets: secretDB, Metrics: metrics.New(st, cfg.Retention, clk), stop: make(chan struct{})}
	log := slog.Default().With("node", cfg.ULID, "comp", "node")
	// From here on every error path unwinds through Stop.
	fail := func(err error) (*Node, error) {
		n.Stop()
		return nil, err
	}

	// The blob store lives beside Pebble under the same data directory, so a
	// node's whole durable state is one directory to back up or wipe.
	blobs, err := blobstore.Open(filepath.Join(cfg.DataDir, "blobs"), cfg.Limits.EffectiveMaxBlobBytes())
	if err != nil {
		return fail(fmt.Errorf("node %s: open blob store: %w", cfg.ULID, err))
	}
	n.Blobs = blobs

	// 1. Registry: the identity source every door consults. Loads the r/
	//    family; a corrupt persisted entry is fatal (fail-loud, like the
	//    store's own counters).
	reg, err := registry.New(st, cfg.ULID)
	if err != nil {
		return fail(fmt.Errorf("node %s: registry: %w", cfg.ULID, err))
	}
	n.Registry = reg
	// Move-drain design §3.4: Drain owns incrementing colca_drains_active
	// itself once this is wired, the same way kick/deliver are wired below.
	reg.SetMetrics(n.Metrics)

	// 1b. Token verifier: the human identity world (human-authz §2). Built
	//     before the doors that consume it; the refresh loop joins the node
	//     WaitGroup so Stop never closes the store under a JWKS persist.
	var ver *tokenauth.Verifier
	if cfg.Auth != nil {
		ver, err = tokenauth.New(tokenauth.Config{
			Issuer:   cfg.Auth.Issuer,
			Audience: cfg.Auth.Audience,
			JWKSURL:  cfg.Auth.JWKSURL,
			Refresh:  cfg.Auth.EffectiveRefresh(),
		}, st, n.Metrics)
		if err != nil {
			return fail(fmt.Errorf("node %s: tokenauth: %w", cfg.ULID, err))
		}
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			ver.Run(n.stop)
		}()
	}

	// 2. Broker: New binds the sockets, so the addrs are known before Serve
	//    and before the engine exists. The engine is late-bound below. A node
	//    with ONLY human listeners is legal (§4), and so is one with only a
	//    local door.
	if cfg.MQTT.Addr != "" || cfg.MQTTLocal.Addr != "" || cfg.MQTTHuman.TCPAddr != "" || cfg.MQTTHuman.WSAddr != "" {
		mq, err := mqttsrv.New(cfg, id, reg, ver, nil, n.Metrics, cfg.Limits.EffectiveMaxRecordBytes())
		if err != nil {
			return fail(fmt.Errorf("node %s: mqtt listen: %w", cfg.ULID, err))
		}
		n.MQTT = mq
		n.MQTTAddr = mq.Addr()
		n.MQTTLocalAddr = mq.LocalAddr()
		n.MQTTHumanTCPAddr = mq.HumanTCPAddr()
		n.MQTTHumanWSAddr = mq.HumanWSAddr()
	}

	// 3. Engine: delivers downlinked commands into the local broker when there
	//    is one (a node without MQTT simply persists them).
	var deliver engine.LocalDeliver
	var hasSubscriber engine.HasSubscriberFor
	if n.MQTT != nil {
		deliver = n.MQTT.DeliverLocal
		hasSubscriber = n.MQTT.HasSubscriberFor
	}
	n.Engine = engine.New(st, cfg, reg, deliver, n.Metrics, clk)
	// Wired separately from New (like SetExecutor/SetObserver below) so a
	// node with no broker leaves it nil and deliverCommand never counts
	// a false undelivered command for want of an answer it cannot give.
	n.Engine.SetSubscriberCheck(hasSubscriber)
	// Command execution: the engine dispatches by contract, each executor owns
	// its own verbs. _CmdAdmin drives the SAME registry writes as the
	// enrollment door — one write path inside (cmdadmin design §5). The data
	// model lives in the plugin, so the core never learns what a signal is
	// (data-model binding design §7).
	blobPort := engine.NewBlobPort(blobs)
	domain := uns.NewConfigExec(n.Engine.EntityStore(), reg, n.Engine.Elements(), blobPort,
		registry.NewULID, cfg.Plugin)
	edit := uns.NewEditExec(
		n.Engine.EntityStore(), editAttachmentWriter{registry: reg},
	)
	n.Engine.SetExecutor(engine.Executors(engine.NewAdminExecutor(reg), domain, edit))
	n.Engine.SetObserver(domain)
	// The observer only sees records from here on; the retained set persisted
	// by earlier incarnations of this node is replayed to it once, so a
	// catalogue that arrived while no observer was wired (a restart, or a
	// binding a re-declaration once wiped) still gets its lifecycle pass.
	n.Engine.ReplayRetained()
	// A node describes itself: `_Node` is "authored by the node it
	// describes" (the contract's own words), and the one fact about itself a
	// node cannot read from config is where it sits — the element its parent
	// bound it to, learned from the ancestry the downlink hands down. So the
	// record is written from the moment a position is learned, through the
	// ONE authoring path, merging into whatever an operator has since put on
	// it (a display name, a description): only the position is this hook's
	// to set, and it writes only when that changed. At the root the ancestry
	// is empty and the node is bound to nothing above itself.
	n.Engine.SetOnPosition(func(a uns.Ancestry) {
		root := ""
		if len(a) > 0 {
			root = a[len(a)-1].Element
		}
		topic := "colca/v1/_Node/" + cfg.ULID + "/_colca/nodes/" + cfg.ULID
		entity := map[string]any{}
		if raw, ok := n.Engine.EntityStore().KVGet(topic); ok && json.Unmarshal(raw, &entity) == nil {
			if held, _ := entity["root_system_element_id"].(string); held == root {
				return
			}
		}
		entity["id"] = cfg.ULID
		if name, _ := entity["name"].(string); name == "" {
			entity["name"] = cfg.NodeName()
		}
		entity["root_system_element_id"] = root
		payload, err := json.Marshal(map[string]any{
			"entities": []map[string]any{{"contract": "_Node", "entity": entity}},
		})
		if err != nil {
			log.Error("node record encode failed", "err", err)
			return
		}
		if code, msg, _ := domain.Execute("_CmdConfigure", "entity/upsert", payload); code != 200 {
			log.Error("node record not authored", "code", code, "msg", msg)
		}
	})
	// The registry resolves placements through the engine's element index
	// (id-grants design §4). Wired here rather than at construction because the
	// namespace is a projection of records the engine holds, and the registry
	// is built first — every door consults it, so it has to exist earliest.
	reg.SetNamespace(n.Engine.Elements())
	// A local service's self-registration (local-service-trust design §3.2)
	// authors the elements along a declared mount that does not exist yet, the
	// same way `colca node enroll --mount` does for a child node: through the
	// ONE authoring path in this system, domain.Execute("_CmdConfigure",
	// "element/upsert", ...) — never a second, direct write to the element
	// index. Wired here because this is the first point both dependencies
	// exist: n.Engine.Elements() (uns.Placements, via IDAt) and domain (built
	// just above). Without this, Register's declared-mount branch fails closed
	// with "mount authoring is not wired at this node" and every local CONNECT
	// carrying a mount is refused.
	reg.SetAuthoring(n.Engine.Elements(), func(path, elementID string) error {
		name := path
		if i := strings.LastIndexByte(path, '/'); i >= 0 {
			name = path[i+1:]
		}
		payload, err := json.Marshal(map[string]any{
			"elements": []map[string]any{
				{"path": path, "element": map[string]any{"id": elementID, "name": name}},
			},
		})
		if err != nil {
			return err
		}
		code, msg, _ := domain.Execute("_CmdConfigure", "element/upsert", payload)
		if code != 200 {
			return fmt.Errorf("author element at %s: %s", path, msg)
		}
		return nil
	})
	if ver != nil {
		// A human's grants come from the groups their token names, resolved
		// against the definitions this node holds (definition-stream design §8).
		ver.SetGroupIndex(n.Engine.Groups())
		ver.SetPersonalAccessTokenIndex(uns.NewPersonalAccessTokenIndex(
			n.Engine.EntityStore(),
		))
	}
	// Schema bundle (schema-bundle design §6/§7): explicit path, or the
	// baked default when present, or the builtin floor. Any configured-but-
	// bad bundle refuses to start — a broker that silently fell back to
	// weaker validation would be a validated namespace in name only.
	bundlePath := cfg.Contracts.Bundle
	if bundlePath == "" {
		if _, err := os.Stat(config.BakedBundlePath); err == nil {
			bundlePath = config.BakedBundlePath
		}
	}
	if bundlePath != "" {
		tbl, err := contracts.Load(bundlePath, cfg.Contracts.SHA256)
		if err != nil {
			return fail(fmt.Errorf("node %s: %w", cfg.ULID, err))
		}
		n.Engine.SetContracts(tbl)
		version, digest, count := tbl.Info()
		log.Info("contracts bundle loaded", "path", bundlePath, "version", version,
			"digest", digest[:12], "contracts", count)
	}
	n.Metrics.SetBundleInfo(n.Engine.BundleInfo())
	// Position in the tree (id-grants design §4): a node without a parent IS
	// the root — it sits on nothing above itself, so its chain is known-empty
	// by construction. Children learn theirs from the downlink hand-down;
	// grant translation reads the rendered prefix per Verify.
	if cfg.Parent == nil {
		n.Engine.SetAncestry(uns.Ancestry{})
	}

	if n.MQTT != nil {
		n.MQTT.SetEngine(n.Engine)
		// Revocation / re-enroll kicks the live session immediately (auth §7),
		// and registry changes mirror onto the local bus like any entity
		// (enroll = retained _EnrolledIdentity, revoke = retained-clear).
		reg.SetKick(n.MQTT.Kick)
		reg.SetDeliver(n.MQTT.DeliverLocal)
		// Re-seed the broker's retained set from the KV projection. The two are
		// ONE contract seen from two sides (engine.retainFor retains exactly the
		// classes that project into KV), but mochi's retained store is in-memory:
		// without this replay a restarted node comes back with an intact KV view
		// and an EMPTY retained set, silently breaking the "fresh subscriber gets
		// the current state on connect" guarantee the bus makes.
		//
		// The seed MUST run before Serve: mochi's Publish/InjectPacket works
		// entirely on in-memory state, while Serve is what starts the listener
		// accept loops — so no client CONNECT (and therefore no client publish)
		// can interleave with the replay, and a fresh live value can never be
		// overwritten by this stale snapshot. Connections attempted meanwhile
		// just wait in the kernel accept backlog (the listener is already
		// bound). Cost is one in-memory publish per KV path — node.Start with
		// 10k seeded paths measures ~70ms total, ~380ms under -race
		// (TestRetainedSeedStartupCostTenThousandPaths) — so it does not
		// meaningfully delay /healthz, which opens after it.
		seeded := 0
		entries, err := st.KVScan("")
		if err != nil {
			// This IS the "fresh subscriber gets current state" guarantee the
			// comment above describes; a scan that could not complete must
			// fail startup rather than come up silently claiming an empty
			// retained set is correct.
			return fail(fmt.Errorf("node %s: reseed retained set: %w", cfg.ULID, err))
		}
		for _, en := range entries {
			n.MQTT.DeliverLocal(en.Topic, en.Payload, true)
			seeded++
		}
		n.Metrics.SetReseedCount(seeded)
		go func(mq *mqttsrv.Server) {
			if err := mq.Serve(); err != nil {
				log.Error("mqtt server stopped", "err", err)
			}
		}(n.MQTT)

		// Periodic time-sync beacon (design §2.2): the per-subscribe publish
		// is wired inside mqttsrv's hook (OnSubscribed, as amended [delta] —
		// session-establishment was deterministically racy and was replaced,
		// not supplemented); this is the OTHER trigger, every
		// time_sync.beacon_interval regardless of subscription activity.
		// Joins n.wg exactly like the repl loops and the pruner: Stop must
		// wait for it before MQTT.Close() runs.
		n.wg.Add(1)
		go func(mq *mqttsrv.Server) {
			defer n.wg.Done()
			mq.RunBeacon(cfg.TimeSync.EffectiveBeaconInterval(), n.stop)
		}(n.MQTT)
	}

	// 4. Local HTTPS control API: TLS with the node's own key; machine callers
	//    present their pinned client key, admin tooling uses the token (§6.3).
	if cfg.API.Addr != "" {
		tlsCfg, err := httpapi.TLSConfig(id, cfg.ULID, cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return fail(fmt.Errorf("node %s: api tls: %w", cfg.ULID, err))
		}
		ln, err := net.Listen("tcp", cfg.API.Addr)
		if err != nil {
			return fail(fmt.Errorf("node %s: api listen %s: %w", cfg.ULID, cfg.API.Addr, err))
		}
		n.apiLn = ln
		n.APIAddr = ln.Addr().String()
		n.httpSrv = httpserver.New(n.trackInflight(httpapi.Handler(n.Engine, cfg, reg, ver, n.Metrics, n.Blobs, id.PublicHex(), false, n.Secrets)))
		go func(srv *http.Server, ln net.Listener) {
			if err := srv.Serve(tls.NewListener(ln, tlsCfg)); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("api server stopped", "err", err)
			}
		}(n.httpSrv, ln)
	}

	// 4b. The local HTTP door: plaintext, no TLS, no admin routes
	//     (local-service-trust design §4) — reachability from inside the
	//     deployment's own network IS the credential, exactly like the local
	//     MQTT door above. /healthz and /metrics are served here too, so
	//     Prometheus (itself a local service) scrapes over plain HTTP and
	//     never needs the insecure_skip_verify a self-signed door required.
	if cfg.API.LocalAddr != "" {
		ln, err := net.Listen("tcp", cfg.API.LocalAddr)
		if err != nil {
			return fail(fmt.Errorf("node %s: local api listen %s: %w", cfg.ULID, cfg.API.LocalAddr, err))
		}
		n.localAPILn = ln
		n.LocalAPIAddr = ln.Addr().String()
		n.localAPISrv = httpserver.New(n.trackInflight(httpapi.Handler(n.Engine, cfg, reg, ver, n.Metrics, n.Blobs, id.PublicHex(), true, n.Secrets)))
		go func(srv *http.Server, ln net.Listener) {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("local api server stopped", "err", err)
			}
		}(n.localAPISrv, ln)
	}

	// 5. Replication server — children are enrolled at runtime (kind "node"),
	//    so the listener exists whenever a repl address is configured.
	if cfg.Repl.Addr != "" {
		rs, err := repl.NewServer(cfg, n.Engine, id, reg, n.Blobs, n.Metrics)
		if err != nil {
			return fail(fmt.Errorf("node %s: repl server: %w", cfg.ULID, err))
		}
		n.ReplSrv = rs
		addr, err := rs.Start()
		if err != nil {
			return fail(fmt.Errorf("node %s: repl listen %s: %w", cfg.ULID, cfg.Repl.Addr, err))
		}
		n.ReplAddr = addr

		// Move-drain periodic sweep (design §3.2): the second completion
		// trigger alongside every /downlink poll — covers a draining child
		// that never polls again, and re-evaluates any drain that persisted
		// through this restart. Only meaningful where kind=node children can
		// be enrolled at all, i.e. wherever the repl door exists. Joins the
		// same WaitGroup as the repl loops and the pruner: Stop must wait for
		// an in-flight sweep before closing the store.
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			rs.RunDrainTicker(n.stop)
		}()
	}

	// 6. Uplink + downlink loops towards the parent.
	if cfg.Parent != nil {
		cl, err := repl.NewClient(cfg.Parent.URL, cfg.Parent.Pubkey, id, cfg.Limits.EffectiveMaxRecordBytes())
		if err != nil {
			return fail(fmt.Errorf("node %s: repl client for %s: %w", cfg.ULID, cfg.Parent.URL, err))
		}
		// A child's pull can now recurse through this node to its own parent.
		if n.ReplSrv != nil {
			n.ReplSrv.SetUpstream(cl)
		}
		// The executor can now fetch a blob a provisioning command names but
		// this node does not hold yet (resources design §3, §7.1).
		blobPort.SetFetcher(cl)
		n.wg.Add(2)
		go func() {
			defer n.wg.Done()
			repl.RunUplink(cl, n.Engine, n.Blobs, n.Metrics, n.stop)
		}()
		go func() {
			defer n.wg.Done()
			repl.RunDownlink(cl, n.Engine, n.Metrics, n.stop)
		}()
	}

	// 7. Retention pruner. Joins the same WaitGroup as the repl loops: a prune
	//    batch or refresh append in flight must finish before Stop closes the
	//    store. With retention.interval: 0 (explicit disable) Run returns
	//    immediately; the default (absent) config prunes on the §3.1 defaults.
	pruner := retention.NewPruner(st, n.Engine, cfg.Retention, n.Metrics, cfg.ULID)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		pruner.Run(n.stop)
	}()

	// 8. Blob sweeper (resources design §8). Joins the same WaitGroup as the
	//    pruner: a sweep in flight must finish before Stop closes the blob
	//    store. With blob_gc.interval: 0 (explicit disable) Run returns
	//    immediately; the default (absent) config sweeps on the §8 defaults.
	sweeper := blobgc.NewSweeper(blobs, n.Engine, cfg.BlobGC, n.Metrics, cfg.ULID)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		sweeper.Run(n.stop)
	}()

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
		if n.localAPISrv != nil {
			_ = n.localAPISrv.Close()
		}
		if n.localAPILn != nil {
			_ = n.localAPILn.Close() // idempotent; guarantees the port is free
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
		if n.Secrets != nil {
			_ = n.Secrets.Close()
		}
	})
}

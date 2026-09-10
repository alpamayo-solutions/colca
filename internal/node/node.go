// Package node assembles a Colca node from its components and owns its
// lifecycle. Start wires everything in dependency order; the broker is built
// before the engine and receives it through SetEngine. Stop releases
// everything, so the same data directory and ports can be reused by the next
// Start.
package node

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/door"
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
	"github.com/alpamayo-solutions/colca/internal/nodelog"
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
	// MQTTLocalAddr and LocalAPIAddr are the local doors, "" if not configured.
	// They are never published.
	MQTTLocalAddr string
	LocalAPIAddr  string
	// Token doors for people; "" when not configured.
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
	// logPublisher writes this node's own log into its store from a goroutine wg
	// does not track, so Stop must stop it before closing the store.
	logPublisher *door.LogPublisher
}

// Start builds and starts a node from cfg. On any failure after the store is
// open, everything already started is closed again before returning, so no
// listener stays bound and no Pebble directory stays locked.
func Start(cfg *config.Config) (*Node, error) {
	// The topic root is process-wide: every topic this node builds or accepts
	// starts with it.
	if err := uns.SetRoot(cfg.EffectiveTopicRoot()); err != nil {
		return nil, fmt.Errorf("node %s: %w", cfg.ULID, err)
	}
	lvl := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		lvl = slog.LevelDebug
	}
	// The node publishes its own log into its tree. The handler is installed before
	// anything else so startup lines are kept; they queue until Attach.
	logSink := &nodelog.Sink{}
	// The tree gets INFO and above even when the console is at debug: debug lines
	// come per append and per batch and do not belong on a replicated stream. It
	// also keeps the engine's per-append line from being published, which would
	// feed back into itself.
	publishLevel := lvl
	if publishLevel < slog.LevelInfo {
		publishLevel = slog.LevelInfo
	}
	logPublisher := door.NewLogPublisher(
		slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}),
		logSink,
		door.LogPublisherOptions{MinLevel: publishLevel, Skip: nodelog.SkipsItsOwnPublishing},
	)
	// SetDefault also routes the standard log package here, and those records only
	// carry a caller when the log flags ask for one; _Log requires it. SetDefault
	// clears the flags afterwards, but reads them first.
	log.SetFlags(log.Lshortfile)
	slog.SetDefault(slog.New(logPublisher).With("service", nodelog.ServiceName))

	// First boot mints this node's identity; every later boot loads it. The key
	// must not come from anything distributed to install the node.
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

	// clk is this node's authoritative time; a node without a parent is the
	// authority. Metrics and Engine must share this instance.
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

	// 1. Registry: every door consults it. A corrupt entry fails startup.
	reg, err := registry.New(st, cfg.ULID)
	if err != nil {
		return fail(fmt.Errorf("node %s: registry: %w", cfg.ULID, err))
	}
	n.Registry = reg
	// The registry updates colca_drains_active itself.
	reg.SetMetrics(n.Metrics)

	// 1b. Token verifier for people, built before the doors that use it. Its
	//     refresh loop joins wg so Stop never closes the store mid-write.
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

	// 2. Broker: New binds the sockets, so addresses are known before Serve. The
	//    engine is set later. Token doors or a local door alone are fine.
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
	// Without a broker there is nothing to check, and deliverCommand must not count
	// undelivered commands.
	n.Engine.SetSubscriberCheck(hasSubscriber)
	// Each executor owns its contract's verbs. _CmdAdmin uses the same registry
	// writes as the enrollment door, and the data model lives in the plugin, so the
	// core never learns what a signal is.
	blobPort := engine.NewBlobPort(blobs)
	domain := uns.NewConfigExec(n.Engine.EntityStore(), reg, n.Engine.Elements(), blobPort,
		registry.NewULID, cfg.Plugin)
	edit := uns.NewEditExec(
		n.Engine.EntityStore(), reg, editAttachmentWriter{registry: reg},
	)
	edit.SetScope(n.Engine.Scope()) // a person's grants resolve against this node's elements
	// The configure executor's blob port: a resource must never point at bytes
	// this node cannot produce.
	edit.SetBlobs(blobPort)
	n.Engine.SetExecutor(engine.Executors(engine.NewAdminExecutor(reg), domain, edit))
	n.Engine.SetObserver(domain)
	// The observer only sees new records, so the retained state from earlier runs
	// is replayed to it once.
	n.Engine.ReplayRetained()
	// A node authors its own _Node record. The one thing it cannot take from config
	// is its position, which it learns from the downlink. The record goes through
	// the normal authoring path, merges with what operators set, and is rewritten
	// only when the position or the interfaces change.
	var nodeRecordMu sync.Mutex
	var positionMu sync.RWMutex
	var lastPosition uns.Ancestry
	positionKnown := false
	authorNodeRecord := func(a uns.Ancestry) {
		nodeRecordMu.Lock()
		defer nodeRecordMu.Unlock()
		root := ""
		if len(a) > 0 {
			root = a[len(a)-1].Element
		}
		topic := uns.Prefix() + "_Node/" + cfg.ULID + "/_colca/nodes/" + cfg.ULID
		entity := map[string]any{}
		interfaces := networkInventory(time.Now())
		metrics := nodeHealthMetrics()
		if raw, ok := n.Engine.EntityStore().KVGet(topic); ok && json.Unmarshal(raw, &entity) == nil {
			if held, _ := entity["root_system_element_id"].(string); held == root &&
				sameNetworkInventory(entity["network_interfaces"], interfaces) {
				if heldMetrics, ok := entity["health_metrics"].([]any); ok && len(heldMetrics) > 0 {
					return
				}
			}
		}
		entity["health_metrics"] = metrics
		entity["network_interfaces"] = interfaces
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
		if code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "entity/upsert", payload); code != 200 {
			log.Error("node record not authored", "code", code, "msg", msg)
		}
	}
	n.Engine.SetOnPosition(func(a uns.Ancestry) {
		positionMu.Lock()
		lastPosition = append(uns.Ancestry(nil), a...)
		positionKnown = true
		positionMu.Unlock()
		authorNodeRecord(a)
	})
	// Author once at startup when the position is already known: a persisted
	// ancestry, or no parent at all (the root's empty chain is a position).
	// SetOnPosition does not fire for a position loaded before it was registered.
	// A new child waits for the downlink and writes once.
	if ancestry, known := n.Engine.Ancestry(); known || cfg.Parent == nil {
		positionMu.Lock()
		lastPosition = append(uns.Ancestry(nil), ancestry...)
		positionKnown = true
		positionMu.Unlock()
		authorNodeRecord(ancestry)
	}
	// Interfaces can change without the node moving. Refresh periodically, but
	// author only when the structural inventory changed; observed_at alone
	// never creates stream traffic.
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-n.stop:
				return
			case <-ticker.C:
				positionMu.RLock()
				known := positionKnown
				position := append(uns.Ancestry(nil), lastPosition...)
				positionMu.RUnlock()
				if known {
					authorNodeRecord(position)
				}
			}
		}
	}()
	// The registry resolves placements through the engine's element index. It is
	// set here because the registry has to exist before the engine.
	reg.SetNamespace(n.Engine.Elements())
	// A local service registering with a mount that does not exist yet gets its
	// elements authored through the same path as everything else. Without this,
	// such registrations are refused.
	reg.SetAuthoring(func(path string) (string, error) {
		payload, err := json.Marshal(map[string]string{"path": path})
		if err != nil {
			return "", err
		}
		code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "element/author", payload)
		if code != 200 {
			return "", fmt.Errorf("author element at %s: %s", path, msg)
		}
		return msg, nil
	})
	if ver != nil {
		// People's grants come from their token's groups, resolved against the
		// definitions this node holds.
		ver.SetGroupIndex(n.Engine.Groups())
		ver.SetPersonalAccessTokenIndex(uns.NewPersonalAccessTokenIndex(
			n.Engine.EntityStore(),
		))
	}
	// Contracts bundle: the configured path, the baked one if present, or the
	// built-in rules. A configured bundle that fails to load stops startup rather
	// than silently validating less.
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
	// A node without a parent is the root, so its chain is known to be empty.
	// Children learn theirs from the downlink.
	if cfg.Parent == nil {
		n.Engine.SetAncestry(uns.Ancestry{})
	}

	if n.MQTT != nil {
		n.MQTT.SetEngine(n.Engine)
		// Revocation kicks the live session, and registry changes appear on the local
		// bus like any entity.
		reg.SetKick(n.MQTT.Kick)
		reg.SetDeliver(n.MQTT.DeliverLocal)
		// Reseed the broker's retained set from KV: mochi keeps it in memory, so a
		// restarted node would otherwise give new subscribers no state. This runs
		// before Serve, so no client publish can race it and be overwritten by the
		// snapshot. 10k paths take about 70ms.
		seeded := 0
		entries, err := st.KVScan("")
		if err != nil {
			// Without the full scan the retained set would be silently incomplete.
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

		// The periodic time beacon; mqttsrv also sends one on each subscribe. It joins
		// wg so Stop waits for it before closing MQTT.
		n.wg.Add(1)
		go func(mq *mqttsrv.Server) {
			defer n.wg.Done()
			mq.RunBeacon(cfg.TimeSync.EffectiveBeaconInterval(), n.stop)
		}(n.MQTT)
	}

	// The uplink client is built before the HTTP doors so /healthz can report its
	// status from the start. Its loops start in step 6.
	var replClient *repl.Client
	if cfg.Parent != nil {
		replClient, err = repl.NewClient(cfg.Parent.URL, cfg.Parent.Pubkey, id, cfg.Limits.EffectiveMaxRecordBytes())
		if err != nil {
			return fail(fmt.Errorf("node %s: repl client for %s: %w", cfg.ULID, cfg.Parent.URL, err))
		}
	}

	// 4. HTTPS API with the node's key: machines present their pinned key, admin
	//    tooling uses the token.
	if cfg.API.Addr != "" {
		tlsCfg, err := httpapi.TLSConfig(id, cfg.ULID, cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return fail(fmt.Errorf("node %s: api tls: %w", cfg.ULID, err))
		}
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.API.Addr)
		if err != nil {
			return fail(fmt.Errorf("node %s: api listen %s: %w", cfg.ULID, cfg.API.Addr, err))
		}
		n.apiLn = ln
		n.APIAddr = ln.Addr().String()
		n.httpSrv = httpserver.New(n.trackInflight(httpapi.Handler(n.Engine, cfg, reg, ver, n.Metrics, n.Blobs, id.PublicHex(), false, replClient, n.Secrets)))
		go func(srv *http.Server, ln net.Listener) {
			if err := srv.Serve(tls.NewListener(ln, tlsCfg)); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("api server stopped", "err", err)
			}
		}(n.httpSrv, ln)
	}

	// 4b. The plaintext local HTTP door, without admin routes. /healthz and
	//     /metrics are served here too, so Prometheus scrapes over plain HTTP.
	if cfg.API.LocalAddr != "" {
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", cfg.API.LocalAddr)
		if err != nil {
			return fail(fmt.Errorf("node %s: local api listen %s: %w", cfg.ULID, cfg.API.LocalAddr, err))
		}
		n.localAPILn = ln
		n.LocalAPIAddr = ln.Addr().String()
		n.localAPISrv = httpserver.New(n.trackInflight(httpapi.Handler(n.Engine, cfg, reg, ver, n.Metrics, n.Blobs, id.PublicHex(), true, replClient, n.Secrets)))
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
		// Stop closes repl connections instead of draining them, so the repl door needs
		// in-flight tracking or a handler could outlive the store.
		rs.SetInflightTracker(n.trackInflight)
		addr, err := rs.Start()
		if err != nil {
			return fail(fmt.Errorf("node %s: repl listen %s: %w", cfg.ULID, cfg.Repl.Addr, err))
		}
		n.ReplAddr = addr

		// The drain sweep finishes drains for children that no longer poll, including
		// drains that survived a restart. It joins wg like the others.
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			rs.RunDrainTicker(n.stop)
		}()
	}

	// 6. Uplink and downlink loops towards the parent.
	if cfg.Parent != nil {
		cl := replClient
		// A child's pull can now recurse through this node to its own parent.
		if n.ReplSrv != nil {
			n.ReplSrv.SetUpstream(cl)
		}
		// The executor can fetch a blob a command names but this node lacks.
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

	// 7. Retention pruner. It joins wg so a prune in flight finishes before the
	//    store closes. interval: 0 disables it.
	pruner := retention.NewPruner(st, n.Engine, cfg.Retention, n.Metrics, cfg.ULID)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		pruner.Run(n.stop)
	}()

	// 8. Blob sweeper, joined to wg for the same reason. interval: 0 disables it.
	sweeper := blobgc.NewSweeper(blobs, n.Engine, cfg.BlobGC, n.Metrics, cfg.ULID)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		sweeper.Run(n.stop)
	}()

	// The node's own log is delivered only now: the engine is fully configured (a
	// publisher appending earlier would race with that), and the bundle that
	// defines _Log is loaded. Earlier lines wait in the queue, in order.
	logSink.Attach(n.Engine, cfg.ULID)
	n.logPublisher = logPublisher
	logPublisher.Start(context.Background())

	log.Info("colca node started", "ulid", cfg.ULID, "api", n.APIAddr, "repl", n.ReplAddr, "mqtt", n.MQTTAddr)
	if cfg.AddrFile != "" {
		if err := n.writeAddrFile(cfg.AddrFile); err != nil {
			// The supervisor is waiting for this file, so fail the start.
			n.Stop()
			return nil, fmt.Errorf("node %s: write addr_file %s: %w", cfg.ULID, cfg.AddrFile, err)
		}
	}
	return n, nil
}

// writeAddrFile publishes the resolved door addresses for a supervisor that
// configured them as `:0` (config.AddrFile). Atomic: a reader never sees a
// partial file, and a file that exists is complete.
func (n *Node) writeAddrFile(path string) error {
	body, err := json.Marshal(map[string]string{
		"api":        n.APIAddr,
		"api_local":  n.LocalAPIAddr,
		"mqtt":       n.MQTTAddr,
		"mqtt_local": n.MQTTLocalAddr,
		"repl":       n.ReplAddr,
	})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// trackInflight makes every request at every door — API, local API, and
// replication — visible to Stop, so the store is never closed underneath a
// running handler.
func (n *Node) trackInflight(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.wg.Add(1)
		defer n.wg.Done()
		h.ServeHTTP(w, r)
	})
}

// Stop shuts the node down and releases everything: afterwards the ports can be
// bound and the data directory opened again. It is safe to call more than once.
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
		// The node's log drain also writes to the store but is not in wg. Stop it
		// before closing the store; later lines still reach stderr.
		if n.logPublisher != nil {
			n.logPublisher.Stop()
		}
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

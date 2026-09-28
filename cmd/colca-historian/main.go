// Command colca-historian follows a node's metrics stream with a cursor and
// writes measurements into the TimescaleDB table historian_metric.
//
// It runs beside colcad, never inside it: history has its own database, failure
// modes and restarts, and a node keeps ingesting whether or not it runs.
//
// Configuration is environment only:
//
//	COLCA_URL         node's local API base URL          (default http://colca)
//	COLCA_SERVICE     service name for the local door    (default historian)
//	COLCA_MQTT_URL    node's local MQTT door, for the service record (default tcp://colca:1883)
//	COLCA_TOPIC_ROOT  topic root of the tree             (default colca)
//	DATABASE_URL      Postgres/Timescale DSN             (required)
//	DB_MAX_CONNS      pool size                          (default 4)
//	FETCH_MAX         records per page                   (default 500)
//	HTTP_ADDR         /healthz + /metrics                (default :9091)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/alpamayo-solutions/colca/clockwork"
	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/historian"
	"github.com/alpamayo-solutions/colca/internal/httpserver"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// version is set by release builds with -ldflags "-X main.version=...".
var version string

type config struct {
	colcaURL      string
	colcaService  string
	colcaMQTTURL  string
	dsn           string
	maxConns      int32
	fetchMax      int
	httpAddr      string
	retentionDays int
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "import" {
		os.Exit(runImport(os.Args[2:]))
	}
	os.Exit(run())
}

func run() int {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	// The log records below are published under the root, so it must be set first.
	if err := uns.SetRootFromEnv(); err != nil {
		log.Error("refusing to start", "err", err)
		return 2
	}

	cfg, err := load()
	if err != nil {
		log.Error("configuration", "err", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The same records also go to the tree's logs stream. Installed once the node's
	// address is known and before work starts.
	logDoor := &door.Client{BaseURL: cfg.colcaURL, Service: cfg.colcaService}
	publisher := door.NewLogPublisher(log.Handler(), logDoor, door.LogPublisherOptions{
		MinLevel: slog.LevelInfo,
	})
	publisher.Start(ctx)
	log = slog.New(publisher).With("service", "colca-historian")
	slog.SetDefault(log)

	pool, err := historian.Open(ctx, cfg.dsn, cfg.maxConns)
	if err != nil {
		log.Error("database", "err", err)
		return 2
	}
	defer pool.Close()

	deps, err := clockwork.Dependencies(os.Getenv("FACTORY_STEP_DEPENDENCIES"), os.Getenv("FACTORY_CLOCK_TOPIC"))
	if err != nil {
		log.Error("clock configuration", "err", err)
		return 2
	}
	source := env("APPLICATION_TIME_SOURCE", "local")
	if source != "local" && source != "mqtt" {
		log.Error("invalid APPLICATION_TIME_SOURCE")
		return 2
	}
	sink := &historian.Sink{Pool: pool, Strict: deps != nil}
	// Postgres may still be starting, so retry for a bounded time instead of relying
	// on a restart policy.
	if err := ensureSchema(ctx, sink, cfg.retentionDays, log, 90*time.Second); err != nil {
		log.Error("schema", "err", err)
		return 2
	}

	// The node says when the stream grew; nothing is read on a timer. The
	// first hint of every watch connection names the stream, so a reconnect
	// drains whatever arrived while it was away.
	watcher := &door.Client{BaseURL: cfg.colcaURL, Service: cfg.colcaService}
	var changes door.Signal
	link := &watchLink{}
	go watcher.WatchForever(ctx, []string{"metrics"}, 100*time.Millisecond, time.Second,
		func(door.Hint) { link.up(); changes.Notify() },
		func(err error) { link.down(time.Now()); log.Warn("watching metrics failed, reconnecting", "err", err) })

	bridge := &historian.Bridge{
		Door: &door.Client{
			BaseURL: cfg.colcaURL,
			Service: cfg.colcaService,
		},
		Store:  sink,
		Strict: deps != nil,
		Log:    log,
		Max:    cfg.fetchMax,
	}

	bridge.BatchInterval = time.Duration(intEnv("BATCH_INTERVAL_MS", 100)) * time.Millisecond
	if bridge.BatchInterval < 0 || bridge.BatchInterval > 30*time.Second {
		log.Error("BATCH_INTERVAL_MS must be in [0,30000]")
		return 2
	}
	bridge.Changes = changes.Changes

	announcer := &historian.Announcer{
		Door:    &door.Client{BaseURL: cfg.colcaURL, Service: cfg.colcaService},
		MQTTURL: cfg.colcaMQTTURL,
		Version: version,
		Log:     log,
	}
	if deps != nil {
		self, err := logDoor.Self(ctx)
		if err != nil {
			log.Error("clock identity", "err", err)
			return 2
		}
		watch := &clockwork.Subscription{Node: self.Node, Topic: os.Getenv("FACTORY_CLOCK_TOPIC"), Dependencies: deps}
		announcer.OnConnect, announcer.OnDisconnect = watch.Attach, watch.Reset
		gate := &clockwork.Gate{Asynchronous: os.Getenv("FACTORY_ASYNC_CONSUMER") == "true", Door: logDoor, Name: cfg.colcaService, Topic: watch.Topic, Dependencies: deps, State: watch.State, Fresh: watch.Fresh, ReportDetails: announcer.ReportClock}
		if err := gate.Register(ctx); err != nil {
			log.Error("clock registration", "err", err)
			return 2
		}
		var lastDrain time.Time
		gate.Drain = func(ctx context.Context, _ float64) (bool, error) {
			if gate.Asynchronous && time.Since(lastDrain) < bridge.BatchInterval {
				return false, nil
			}
			lastDrain = time.Now()
			// Capture after upstream completion; continuous arrivals must not
			// prevent this committed boundary from being reported.
			err := door.DrainToHead(ctx, logDoor, "metrics", historian.Cursor, func(ctx context.Context) (int64, error) {
				_, err := bridge.Once(ctx)
				return bridge.Acknowledged, err
			})
			return err == nil, err
		}
		var changed <-chan struct{}
		bridge.Coordinate = func(ctx context.Context) (bool, error) {
			changed = watch.Changes()
			return gate.Once(ctx, watch.RealNow(source))
		}
		bridge.WaitCoordinate = func(ctx context.Context) {
			delay := gate.WaitDelay(watch.RealNow(source))
			if gate.Asynchronous {
				if batch := time.Until(lastDrain.Add(bridge.BatchInterval)); batch > 0 && batch < delay {
					delay = batch
				}
			}
			clockwork.Wait(ctx, changed, delay)
		}
	}

	go serveObservability(cfg.httpAddr, bridge, announcer, link, log)

	bridge.Health = announcer.Report
	announced := make(chan struct{})
	go func() {
		defer close(announced)
		announcer.Run(ctx)
	}()

	log.Info("historian following", "node", cfg.colcaURL, "consumer", historian.Consumer)
	err = bridge.Run(ctx)
	stop()
	<-announced // the record says inactive before the process goes
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("historian stopped", "err", err)
		return 1
	}
	log.Info("historian stopped")

	return 0
}

// ensureSchema retries until the database answers or the window closes.
func ensureSchema(ctx context.Context, sink *historian.Sink, retentionDays int, log *slog.Logger,
	within time.Duration) error {
	deadline := time.Now().Add(within)
	for attempt := 1; ; attempt++ {
		err := sink.EnsureSchema(ctx, retentionDays)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		log.Warn("database not ready yet, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func load() (config, error) {
	cfg := config{
		colcaURL:     env("COLCA_URL", "http://colca"),
		colcaService: env("COLCA_SERVICE", "historian"),
		colcaMQTTURL: env("COLCA_MQTT_URL", "tcp://colca:1883"),
		dsn:          os.Getenv("DATABASE_URL"),
		httpAddr:     env("HTTP_ADDR", ":9091"),
	}
	if cfg.dsn == "" {
		return cfg, errors.New("DATABASE_URL is required — the historian writes to Timescale")
	}
	maxConns := intEnv("DB_MAX_CONNS", 4)
	if maxConns < 1 || maxConns > math.MaxInt32 {
		return cfg, fmt.Errorf("DB_MAX_CONNS must be between 1 and %d, got %d", math.MaxInt32, maxConns)
	}
	cfg.maxConns = int32(maxConns)
	cfg.fetchMax = intEnv("FETCH_MAX", 500)
	cfg.retentionDays = intEnv("HISTORIAN_RETENTION_DAYS", 0)
	if cfg.retentionDays < 0 {
		return cfg, errors.New("HISTORIAN_RETENTION_DAYS must be zero (unlimited) or positive")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

// serveObservability exposes liveness and the historian's counters. A gap is a
// hole in history that cannot be filled, so it belongs on a dashboard.
// watchGrace is how long the watch on the metrics stream may stay down before
// the health door fails: without it no wake arrives, and nothing reads on a
// timer.
const watchGrace = time.Minute

// watchLink remembers since when the watch has been down; zero while it is up.
type watchLink struct {
	mu        sync.Mutex
	downSince time.Time
}

func (l *watchLink) up() {
	l.mu.Lock()
	l.downSince = time.Time{}
	l.mu.Unlock()
}

func (l *watchLink) down(now time.Time) {
	l.mu.Lock()
	if l.downSince.IsZero() {
		l.downSince = now
	}
	l.mu.Unlock()
}

// downFor is how long the watch has been down, 0 while it is up.
func (l *watchLink) downFor(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.downSince.IsZero() {
		return 0
	}
	return now.Sub(l.downSince)
}

func serveObservability(addr string, bridge *historian.Bridge, announcer *historian.Announcer, link *watchLink,
	log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		unhealthy := map[string]any{}
		// The node's cursor watchdog says when records wait unread on this cursor.
		if lag := announcer.CursorLag(); lag != "" {
			unhealthy["cursor_lag"] = lag
		}
		if down := link.downFor(time.Now()); down > watchGrace {
			unhealthy["watch_down_s"] = int(down.Seconds())
		}
		if len(unhealthy) > 0 {
			unhealthy["ok"], unhealthy["consumer"] = false, historian.Consumer
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(unhealthy)
			return
		}
		_, _ = fmt.Fprintf(w, `{"ok":true,"consumer":%q}`, historian.Consumer)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w,
			"# HELP colca_historian_stream_gaps_total Pruned ranges this bridge could not historise.\n"+
				"# TYPE colca_historian_stream_gaps_total counter\n"+
				"colca_historian_stream_gaps_total %d\n", bridge.Gaps())
		_, _ = fmt.Fprintf(w,
			"# HELP colca_historian_rows_rejected_total Rows the schema permanently refused and set aside, by reason — a non-zero value means a publisher is sending data this table's schema cannot hold; the rest of that page still historised and the cursor still advanced past it.\n"+
				"# TYPE colca_historian_rows_rejected_total counter\n")
		for _, reason := range historian.PoisonReasons() {
			_, _ = fmt.Fprintf(w, "colca_historian_rows_rejected_total{reason=%q} %d\n", reason, bridge.Rejected(reason))
		}
	})
	server := httpserver.NewAt(addr, mux)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("observability server", "addr", addr, "err", err)
	}
}

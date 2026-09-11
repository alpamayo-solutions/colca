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
//	COLCA_TOPIC_ROOT  topic root of the tree             (default colca)
//	DATABASE_URL      Postgres/Timescale DSN             (required)
//	DB_MAX_CONNS      pool size                          (default 4)
//	FETCH_MAX         records per page                   (default 500)
//	IDLE_SLEEP_MS     pause when the stream is quiet     (default 500)
//	HTTP_ADDR         /healthz + /metrics                (default :9091)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/internal/historian"
	"github.com/alpamayo-solutions/colca/internal/httpserver"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

type config struct {
	colcaURL      string
	colcaService  string
	dsn           string
	maxConns      int32
	fetchMax      int
	idleSleep     time.Duration
	httpAddr      string
	retentionDays int
}

func main() {
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

	sink := &historian.Sink{Pool: pool}
	// Postgres may still be starting, so retry for a bounded time instead of relying
	// on a restart policy.
	if err := ensureSchema(ctx, sink, cfg.retentionDays, log, 90*time.Second); err != nil {
		log.Error("schema", "err", err)
		return 2
	}

	bridge := &historian.Bridge{
		Door: &door.Client{
			BaseURL: cfg.colcaURL,
			Service: cfg.colcaService,
		},
		Store:     sink,
		Log:       log,
		Max:       cfg.fetchMax,
		IdleSleep: cfg.idleSleep,
	}

	go serveObservability(cfg.httpAddr, bridge, log)

	log.Info("historian following", "node", cfg.colcaURL, "consumer", historian.Consumer)
	if err := bridge.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
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
	cfg.idleSleep = time.Duration(intEnv("IDLE_SLEEP_MS", 500)) * time.Millisecond
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
func serveObservability(addr string, bridge *historian.Bridge, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
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

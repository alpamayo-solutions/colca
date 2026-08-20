// Command colca-historian carries measurements from a node's `metrics` stream
// into the Timescale hypertables the edit and Grafana read.
//
// It is the last piece of the pipeline the colca cutover deleted: metrics have
// been ingested, stored and replicated since, but nothing has written them into
// `historian_metric`, so every history view has been empty. The table and its
// unique index on (signal_id, timestamp) are unchanged — this fills the same
// table the Kafka writer did, from a cursor instead of a topic.
//
// A sibling of colcad, never part of it: historisation is a rate path with its
// own database connection, its own failure modes and its own restart cadence,
// and a node must stay ingesting whether or not anything is writing history.
//
// Go rather than Python (projector design §9) because this is a rate
// path — two columns and a marker — not a schema path. The cache projector,
// whose sink IS the Django schema, stays in the api image.
//
// Configuration is environment only:
//
//	COLCA_URL         node's local API base URL          (default http://colca)
//	COLCA_SERVICE     service name for the local door    (default historian)
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
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/alpamayo-solutions/colca/internal/door"
	"github.com/alpamayo-solutions/colca/internal/historian"
)

type config struct {
	colcaURL     string
	colcaService string
	dsn          string
	maxConns     int32
	fetchMax     int
	idleSleep    time.Duration
	httpAddr     string
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg, err := load()
	if err != nil {
		log.Error("configuration", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := historian.Open(ctx, cfg.dsn, cfg.maxConns)
	if err != nil {
		log.Error("database", "err", err)
		os.Exit(2)
	}
	defer pool.Close()

	sink := &historian.Sink{Pool: pool}
	// The database may still be starting — Postgres accepts unix-socket
	// connections during initdb while TCP is not listening yet, so "is it up"
	// has no single moment. Retry for a bounded window instead of dying and
	// relying on a restart policy that a test harness may not have.
	if err := ensureSchema(ctx, sink, log, 90*time.Second); err != nil {
		log.Error("schema", "err", err)
		os.Exit(2)
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
		os.Exit(1)
	}
	log.Info("historian stopped")
}

// ensureSchema retries until the database answers or the window closes.
func ensureSchema(ctx context.Context, sink *historian.Sink, log *slog.Logger,
	within time.Duration) error {
	deadline := time.Now().Add(within)
	for attempt := 1; ; attempt++ {
		err := sink.EnsureSchema(ctx)
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
	cfg.maxConns = int32(intEnv("DB_MAX_CONNS", 4))
	cfg.fetchMax = intEnv("FETCH_MAX", 500)
	cfg.idleSleep = time.Duration(intEnv("IDLE_SLEEP_MS", 500)) * time.Millisecond
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

// serveObservability exposes liveness and the one number that matters
// operationally: how many pruned-record incidents this bridge has seen. A gap
// means history has a hole nothing can fill, so it belongs on a dashboard
// rather than only in a log line.
func serveObservability(addr string, bridge *historian.Bridge, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"ok":true,"consumer":%q}`, historian.Consumer)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w,
			"# HELP colca_historian_stream_gaps_total Pruned ranges this bridge could not historise.\n"+
				"# TYPE colca_historian_stream_gaps_total counter\n"+
				"colca_historian_stream_gaps_total %d\n", bridge.Gaps)
	})
	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("observability server", "addr", addr, "err", err)
	}
}

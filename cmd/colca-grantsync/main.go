// Command colca-grantsync carries authorization between Keycloak and a colca
// tree.
//
// Keycloak is where a grant is authored: system elements are created and retired
// at runtime, so a checked-in file cannot name one that did not exist when it
// was written, and an operator must be able to grant access to a machine
// somebody just commissioned. This service closes the loop in both directions —
// it registers every element as an authz resource (so there is something to
// assign against), and compiles the resulting permissions into `_Group`
// definitions written at the ROOT node, from where they descend to every node on
// their own.
//
// It is a sibling of colcad, never part of it: Keycloak stays out of the node
// core and out of the message path, and a separate process is what keeps that
// true. Being in the same module is what earns it the grammar — it validates
// every grant with the parser the nodes themselves run.
//
// Stateless. Both stores are durable and every cycle reads them whole, so it
// owns no database, two instances racing produce the same writes, and losing it
// loses nothing but freshness.
//
// Configuration is environment only:
//
//	COLCA_URL           root node's API base URL           (required, https://)
//	COLCA_TOKEN         that node's admin token            (required)
//	COLCA_ROOT_ULID     the root's ULID                    (default: read from /healthz)
//	COLCA_CA_FILE       CA bundle trusted for that node    (default: system roots)
//	COLCA_INSECURE      skip node TLS verification         (default false; dev only)
//	KC_URL              Keycloak base URL incl. /auth      (required)
//	KC_REALM            realm                              (default colca)
//	KC_CLIENT_ID        service account to read/write as   (default colca-authz)
//	KC_CLIENT_SECRET    its secret                         (required)
//	KC_AUTHZ_CLIENT     clientId owning the resource server (default colca-authz)
//	OWNER               deployment key for the managed marker (required)
//	SYNC_INTERVAL_MS    poll interval                      (default 30000)
//	SYNC_ONCE           run one cycle and exit             (default false)
//	SYNC_DRY_RUN        report, write nothing              (default false)
//	HTTP_ADDR           /healthz + /metrics                (default :9090)
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alpamayo-solutions/colca/internal/grantsync"
)

type config struct {
	colcaURL      string
	colcaToken    string
	rootULID      string
	kcURL         string
	kcRealm       string
	kcClientID    string
	kcSecret      string
	kcAuthzClient string
	caFile        string
	insecure      bool
	owner         string
	interval      time.Duration
	once          bool
	dryRun        bool
	httpAddr      string
}

// loadConfig reads the environment, naming EVERY missing variable at once. A
// service that starts with half its configuration and dies on the first cycle
// is harder to diagnose than one that refuses to start and says why.
func loadConfig(getenv func(string) string) (config, error) {
	c := config{
		colcaURL:      strings.TrimRight(getenv("COLCA_URL"), "/"),
		colcaToken:    getenv("COLCA_TOKEN"),
		rootULID:      getenv("COLCA_ROOT_ULID"),
		caFile:        getenv("COLCA_CA_FILE"),
		insecure:      truthy(getenv("COLCA_INSECURE")),
		kcURL:         strings.TrimRight(getenv("KC_URL"), "/"),
		kcRealm:       or(getenv("KC_REALM"), "colca"),
		kcClientID:    or(getenv("KC_CLIENT_ID"), "colca-authz"),
		kcSecret:      getenv("KC_CLIENT_SECRET"),
		kcAuthzClient: or(getenv("KC_AUTHZ_CLIENT"), "colca-authz"),
		owner:         getenv("OWNER"),
		once:          truthy(getenv("SYNC_ONCE")),
		dryRun:        truthy(getenv("SYNC_DRY_RUN")),
		httpAddr:      or(getenv("HTTP_ADDR"), ":9090"),
	}

	var missing []string
	for name, value := range map[string]string{
		"COLCA_URL":        c.colcaURL,
		"COLCA_TOKEN":      c.colcaToken,
		"KC_URL":           c.kcURL,
		"KC_CLIENT_SECRET": c.kcSecret,
		"OWNER":            c.owner,
	} {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sortStrings(missing)
		return config{}, fmt.Errorf("missing required environment: %s", strings.Join(missing, ", "))
	}

	ms := 30000
	if raw := getenv("SYNC_INTERVAL_MS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return config{}, fmt.Errorf("SYNC_INTERVAL_MS must be a positive integer, got %q", raw)
		}
		ms = parsed
	}
	c.interval = time.Duration(ms) * time.Millisecond
	return c, nil
}

func or(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Error("refusing to start", "error", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	nodeHTTP, err := nodeTransport(cfg)
	if err != nil {
		log.Error("refusing to start", "error", err)
		os.Exit(2)
	}
	node := &grantsync.NodeClient{BaseURL: cfg.colcaURL, Token: cfg.colcaToken, HTTP: nodeHTTP}
	rootULID := cfg.rootULID
	if rootULID == "" {
		rootULID, err = node.ULID(ctx)
		if err != nil {
			log.Error("could not learn the root node's ULID; set COLCA_ROOT_ULID to skip the lookup",
				"error", err)
			os.Exit(1)
		}
	}

	registry := prometheus.NewRegistry()
	syncer := &grantsync.Syncer{
		Node: node,
		KC: &grantsync.Keycloak{
			BaseURL: cfg.kcURL, Realm: cfg.kcRealm,
			ClientID: cfg.kcClientID, ClientSecret: cfg.kcSecret,
			AuthzClient: cfg.kcAuthzClient,
		},
		Owner:    cfg.owner,
		RootULID: rootULID,
		DryRun:   cfg.dryRun,
		Log:      log,
		Metrics:  grantsync.NewMetrics(registry),
	}

	if cfg.once {
		report, err := syncer.Once(ctx)
		if err != nil {
			log.Error("sync cycle failed, nothing written", "error", err)
			os.Exit(1)
		}
		fmt.Println(report.Summary())
		return
	}

	go serve(ctx, cfg.httpAddr, registry, log)
	log.Info("syncing grants", "interval", cfg.interval, "root", rootULID,
		"realm", cfg.kcRealm, "dry_run", cfg.dryRun)
	syncer.Run(ctx, cfg.interval)
	log.Info("stopped")
}

// nodeTransport builds the client used against the colca node.
//
// A node's API is mTLS-fronted and presents a certificate from the deployment's
// own PKI, so trusting it means naming that CA. COLCA_INSECURE exists for the
// demo world, where the CA is generated per run and there is nothing to pin —
// it is a deliberate, named opt-out rather than a default, because a service
// that skips verification silently is one nobody notices skipping it.
func nodeTransport(cfg config) (*http.Client, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	switch {
	case cfg.insecure:
		tlsConfig.InsecureSkipVerify = true
	case cfg.caFile != "":
		pem, err := os.ReadFile(cfg.caFile)
		if err != nil {
			return nil, fmt.Errorf("COLCA_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("COLCA_CA_FILE %s holds no usable certificate", cfg.caFile)
		}
		tlsConfig.RootCAs = pool
	}
	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}, nil
}

func serve(ctx context.Context, addr string, reg *prometheus.Registry, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		// Liveness only. Readiness would have to mean "Keycloak and the node are
		// both reachable", and a service whose job is to survive their outages
		// must not report itself unhealthy because of one.
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("metrics server stopped", "error", err)
	}
}

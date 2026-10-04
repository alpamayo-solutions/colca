package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // loopback-only listener of a benchmark process
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// runProfiledNode runs a node from cfgPath exactly as colcad does, plus a pprof
// listener on a loopback port that it writes to $COLCA_BENCH_PPROF_FILE.
func runProfiledNode(cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	runtime.SetMutexProfileFraction(10)
	runtime.SetBlockProfileRate(int(10 * time.Microsecond))
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: http.DefaultServeMux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	n, err := node.Start(cfg)
	if err != nil {
		return err
	}
	if file := os.Getenv("COLCA_BENCH_PPROF_FILE"); file != "" {
		raw, _ := json.Marshal(map[string]string{"api": ln.Addr().String()})
		if err := os.WriteFile(file, raw, 0o600); err != nil { //nolint:gosec // the path the fanout scenario chose for this process
			return fmt.Errorf("write pprof address: %w", err)
		}
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	n.Stop()
	return srv.Close()
}

// Command colcad runs a single Colca node from a YAML config file until it is
// interrupted (SIGINT/SIGTERM), then shuts it down cleanly.
package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/memlimit"
	"github.com/alpamayo-solutions/colca/internal/node"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: colcad <config.yaml>")
	fmt.Fprintln(os.Stderr, "       colcad identity [-json] <config.yaml>")
	fmt.Fprintln(os.Stderr, "       colcad tpm-identity [-json] [-device /dev/tpmrm0]")
	fmt.Fprintln(os.Stderr, "       colcad --version")
}

// version is set by release builds with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--version" || os.Args[1] == "-version") {
		fmt.Println("colcad", version)
		return
	}
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "identity":
			os.Exit(identityCmd(os.Args[2:], os.Stdout, os.Stderr))
		case "tpm-identity":
			os.Exit(tpmIdentityCmd(os.Args[2:], os.Stdout, os.Stderr))
		}
	}
	if len(os.Args) != 2 {
		usage()
		os.Exit(2)
	}
	cfg, err := config.Load(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	limit := memlimit.Apply()
	node.Version = version
	n, err := node.Start(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	if msg, warn := memlimit.Report(limit); warn {
		slog.Warn(msg)
	} else {
		slog.Info(msg, "bytes", limit)
	}
	// The embedded SDK waits for this lifecycle event, not for file polling.
	if cfg.AddrFile != "" {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"event": "colca.ready"})
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	n.Stop()
}

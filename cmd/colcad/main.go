// Command colcad runs a single Colca node from a YAML config file until it is
// interrupted (SIGINT/SIGTERM), then shuts it down cleanly.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/node"
)

// version is set by release builds with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--version" || os.Args[1] == "-version") {
		fmt.Println("colcad", version)
		return
	}
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: colcad <config.yaml>")
		fmt.Fprintln(os.Stderr, "       colcad --version")
		os.Exit(2)
	}
	cfg, err := config.Load(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	n, err := node.Start(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	n.Stop()
}

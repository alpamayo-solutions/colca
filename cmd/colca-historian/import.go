package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alpamayo-solutions/colca/internal/historian"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func runImport(args []string) int {
	if err := importFile(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func importFile(args []string) error {
	flags := flag.NewFlagSet("colca-historian import", flag.ContinueOnError)
	path := flags.String("file", "", "JSONL records with topic and _Metric payload")
	digest := flags.String("sha256", "", "expected SHA-256 of the immutable file")
	beforeText := flags.String("before", "", "exclusive RFC3339 timestamp ceiling")
	batchSize := flags.Int("batch-size", 1000, "transaction size, 1–5000 records")
	dryRun := flags.Bool("dry-run", false, "validate the complete file without database access")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *path == "" || len(*digest) != 64 || *batchSize < 1 || *batchSize > 5000 {
		return fmt.Errorf("require --file, --sha256, --before and batch-size in [1,5000]")
	}
	if _, err := hex.DecodeString(*digest); err != nil {
		return fmt.Errorf("invalid SHA-256")
	}
	before, err := time.Parse(time.RFC3339, *beforeText)
	if err != nil {
		return fmt.Errorf("--before: %w", err)
	}
	if err := uns.SetRootFromEnv(); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	input, err := os.Open(*path) // #nosec G304 -- explicit operator-supplied import file.
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	// A private spool keeps validation and writes on the exact same bytes even
	// if another process replaces or changes the original file during import.
	spool, err := os.CreateTemp("", "colca-historian-import-*")
	if err != nil {
		return err
	}
	defer func() { _ = spool.Close(); _ = os.Remove(spool.Name()) }()
	hash := sha256.New()
	count, err := historian.ScanImport(ctx, io.TeeReader(input, io.MultiWriter(spool, hash)), before, nil)
	if err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != *digest {
		return fmt.Errorf("import file SHA-256 mismatch; nothing written")
	}
	if !*dryRun {
		dsn := os.Getenv("DATABASE_URL")
		if dsn == "" {
			return fmt.Errorf("DATABASE_URL is required for import")
		}
		pool, err := historian.Open(ctx, dsn, 2)
		if err != nil {
			return fmt.Errorf("cannot connect to historian database")
		}
		defer pool.Close()
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			return err
		}
		sink := &historian.Sink{Pool: pool, Strict: true}
		if _, err := historian.Import(ctx, spool, before, *digest, *batchSize, sink); err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"sha256": *digest, "records": count, "dry_run": *dryRun})
}

// Command colca-bench runs the Colca benchmark scenarios and
// writes uniform JSON run records. Recording is always on: every run appends
// one JSONL line per scenario to bench/results/<host>.jsonl, commit-stamped
// via bench.Stamp. It is the benchmark gate:
//
//	colca-bench ingest --machines 4 --duration 30s --storage emmc
//	colca-bench all --storage laptop-nvme
//	colca-bench check --thresholds bench/thresholds.json
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/bench"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// defaultRecordPath returns bench/results/<short-hostname>.jsonl — the
// always-on run-record location shared by scenario runs and `check`.
func defaultRecordPath() string {
	hn, _ := os.Hostname()
	if i := strings.IndexByte(hn, '.'); i >= 0 {
		hn = hn[:i]
	}
	if hn == "" {
		hn = "unknown-host"
	}
	return "bench/results/" + hn + ".jsonl"
}

// scenarioOrder is the full scenario set colca-bench will eventually run for
// "all", in a fixed order. "all" filters this down to whatever is currently
// registered in runners, so later tasks add scenarios purely by adding a
// runners entry — this list and the "all" logic never need to change.
var scenarioOrder = []string{"ingest", "live", "catchup", "cardinality", "footprint"}

func main() {
	if err := uns.SetRootFromEnv(); err != nil {
		fmt.Fprintln(os.Stderr, "colca-bench:", err)
		os.Exit(2)
	}
	if len(os.Args) < 2 {
		usage()
	}
	scenario := os.Args[1]
	fs := flag.NewFlagSet(scenario, flag.ExitOnError)
	machines := fs.Int("machines", 4, "concurrent MQTT publishers")
	rate := fs.Int("rate", 10, "per-machine publish rate in Hz (live)")
	duration := fs.Duration("duration", 30*time.Second, "measurement window")
	records := fs.Int("records", 20000, "records to buffer offline (catchup)")
	paths := fs.Int("paths", 10000, "distinct signal paths (cardinality)")
	colcad := fs.String("colcad", "bin/colcad", "colcad binary (footprint)")
	storage := fs.String("storage", os.Getenv("COLCA_BENCH_STORAGE"), "storage note for the report")
	out := fs.String("out", defaultRecordPath(), "run-record JSONL file to append reports to")
	results := fs.String("results", defaultRecordPath(), "run-record JSONL file to read (check)")
	thresholds := fs.String("thresholds", "bench/thresholds.json", "thresholds file (check)")
	_ = fs.Parse(os.Args[2:])

	if scenario == "check" {
		if err := bench.Check(*results, *thresholds, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "GATE FAILED:", err)
			os.Exit(1)
		}
		return
	}

	p := bench.Params{
		Machines: *machines, RateHz: *rate, Duration: *duration,
		Records: *records, Paths: *paths, ColcadPath: *colcad, Storage: *storage,
	}
	runners := map[string]func(bench.Params) (*bench.Report, error){
		"ingest":      bench.RunIngest,
		"live":        bench.RunLive,
		"catchup":     bench.RunCatchup,
		"cardinality": bench.RunCardinality,
		"footprint":   bench.RunFootprint,
	}
	order := []string{scenario}
	if scenario == "all" {
		order = order[:0]
		for _, name := range scenarioOrder {
			if _, ok := runners[name]; ok {
				order = append(order, name)
			}
		}
	}
	var reports []*bench.Report
	for _, name := range order {
		run, ok := runners[name]
		if !ok {
			usage()
		}
		p.WorkDir = must(os.MkdirTemp("", "colca-bench-"+name+"-"))
		defer os.RemoveAll(p.WorkDir)
		r, err := run(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(1)
		}
		fmt.Print(r.Table())
		reports = append(reports, r)
	}
	if err := bench.AppendRecords(*out, reports); err != nil {
		fmt.Fprintln(os.Stderr, "record results:", err)
		os.Exit(1)
	}
	fmt.Printf("recorded %d report(s) → %s\n", len(reports), *out)
}

func must(s string, err error) string {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return s
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: colca-bench <ingest|live|catchup|cardinality|footprint|all|check> [flags]")
	os.Exit(2)
}

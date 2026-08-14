// Command colca-bench runs the Colca benchmark scenarios and
// writes uniform JSON reports. It is the benchmark gate:
//
//	colca-bench ingest --machines 4 --duration 30s --storage emmc --out results.json
//	colca-bench all --storage laptop-nvme --out results.json
//	colca-bench check --results results.json --thresholds bench/thresholds.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/alpamayo-solutions/colca/bench"
)

// scenarioOrder is the full scenario set colca-bench will eventually run for
// "all", in a fixed order. "all" filters this down to whatever is currently
// registered in runners, so later tasks add scenarios purely by adding a
// runners entry — this list and the "all" logic never need to change.
var scenarioOrder = []string{"ingest", "live", "catchup", "cardinality", "footprint"}

func main() {
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
	out := fs.String("out", "", "append reports to this JSON array file")
	_ = fs.String("results", "", "results file (check)")
	_ = fs.String("thresholds", "bench/thresholds.json", "thresholds file (check)")
	_ = fs.Parse(os.Args[2:])

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
	if *out != "" {
		if err := appendReports(*out, reports); err != nil {
			fmt.Fprintln(os.Stderr, "write results:", err)
			os.Exit(1)
		}
	}
}

func appendReports(path string, add []*bench.Report) error {
	var all []*bench.Report
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &all); err != nil {
			return fmt.Errorf("existing %s is not a report array: %w", path, err)
		}
	}
	all = append(all, add...)
	raw, err := json.MarshalIndent(all, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
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

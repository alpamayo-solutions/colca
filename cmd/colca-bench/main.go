// Command colca-bench runs the benchmark scenarios and appends one commit-stamped
// JSON line per scenario to bench/results/<host>.jsonl:
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

// scenarioOrder is the order "all" runs the registered scenarios in.
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
	if scenario == "node" {
		// The fanout scenario's parent: colcad with a pprof listener.
		if len(os.Args) != 3 {
			usage()
		}
		if err := runProfiledNode(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, "colca-bench node:", err)
			os.Exit(1)
		}
		return
	}
	if scenario == "fleet" {
		runFleet(os.Args[2:])
		return
	}
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
	children := fs.Int("children", 50, "child nodes replicating into one parent (fanout)")
	childRate := fs.Float64("child-rate", 3, "records per second per child (fanout)")
	warmup := fs.Duration("warmup", 90*time.Second, "longest wait for every child's first record (fanout)")
	profileDir := fs.String("profile-dir", "", "save the parent's pprof profiles here (fanout)")
	childDir := fs.String("child-dir", "", "put the children's stores here, e.g. on a RAM disk (fanout)")
	protocol := fs.Bool("protocol-children", false, "run protocol-only children in-process instead of colcad processes (fanout)")
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
		"fanout": func(p bench.Params) (*bench.Report, error) {
			self, err := os.Executable()
			if err != nil {
				return nil, err
			}
			return bench.RunFanout(bench.FanoutParams{Params: p, Children: *children, ChildRate: *childRate,
				Warmup: *warmup, ProfileDir: *profileDir, ChildDir: *childDir, Protocol: *protocol, Self: self})
		},
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
		r, err := runScenario(name, run, p)
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

// runScenario runs one scenario in a work directory of its own and removes it afterwards.
func runScenario(name string, run func(bench.Params) (*bench.Report, error), p bench.Params) (*bench.Report, error) {
	dir, err := os.MkdirTemp("", "colca-bench-"+name+"-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }() //nolint:gosec // the directory MkdirTemp just created
	p.WorkDir = dir
	return run(p)
}

// runFleet drives an existing hub; see bench/fleet.go.
func runFleet(args []string) {
	fs := flag.NewFlagSet("fleet", flag.ExitOnError)
	p := bench.FleetParams{}
	fs.StringVar(&p.HubAPI, "hub-api", "hub:443", "the parent's admin API, host:port")
	fs.StringVar(&p.HubRepl, "hub-repl", "hub:9443", "the parent's replication door, host:port")
	fs.StringVar(&p.HubPubkeyHex, "hub-pubkey", "", "the parent's public key, hex")
	fs.StringVar(&p.KeyDir, "key-dir", "fleet-keys", "where the children's keys are kept")
	fs.IntVar(&p.Children, "children", 100, "emulated edges")
	fs.IntVar(&p.FirstChild, "first-child", 0, "offset of the first child's name")
	fs.IntVar(&p.Signals, "signals", 14, "signals per edge")
	fs.DurationVar(&p.ScanPeriod, "scan", time.Second, "connector scan period")
	fs.Float64Var(&p.ChangeRatio, "change", 0.2, "share of signals that changed per scan (report by exception)")
	fs.Float64Var(&p.Logs, "logs", 0, "log records per second per edge")
	fs.DurationVar(&p.Duration, "duration", 5*time.Minute, "how long to run")
	fs.DurationVar(&p.Interval, "interval", 10*time.Second, "report interval")
	fs.DurationVar(&p.Backlog, "backlog", 0, "history each child replays at start")
	_ = fs.Parse(args)
	if err := bench.RunFleet(p); err != nil {
		fmt.Fprintln(os.Stderr, "fleet:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: colca-bench <ingest|live|catchup|cardinality|footprint|fanout|fleet|all|check> [flags]")
	os.Exit(2)
}

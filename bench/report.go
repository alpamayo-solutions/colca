// Package bench contains the benchmark scenarios. They run real topologies
// (keys, mTLS replication, Pebble directories, paho machines) and emit a uniform
// Report that colca-bench records and gates.
package bench

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"
)

type Host struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	NumCPU    int    `json:"num_cpu"`
	Hostname  string `json:"hostname"`
	GoVersion string `json:"go_version"`
	// Storage is a free-text operator note ("emmc-eg300", "laptop-nvme") set
	// via --storage / COLCA_BENCH_STORAGE. Numbers without it are meaningless.
	Storage string `json:"storage"`
}

type Report struct {
	Scenario  string `json:"scenario"`
	StartedAt string `json:"started_at"`
	// Service, GitCommit and GitDirty record what produced this run; Stamp sets
	// them.
	Service   string             `json:"service"`
	GitCommit string             `json:"git_commit"`
	GitDirty  bool               `json:"git_dirty"`
	Host      Host               `json:"host"`
	Params    map[string]any     `json:"params"`
	Metrics   map[string]float64 `json:"metrics"`
}

func NewReport(scenario, storage string, params map[string]any) *Report {
	hn, _ := os.Hostname()
	r := &Report{
		Scenario:  scenario,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		Host: Host{
			OS: runtime.GOOS, Arch: runtime.GOARCH, NumCPU: runtime.NumCPU(),
			Hostname: hn, GoVersion: runtime.Version(), Storage: storage,
		},
		Params:  params,
		Metrics: map[string]float64{},
	}
	Stamp(r)
	return r
}

// Table renders the report for a human: params first, then metrics sorted by name.
func (r *Report) Table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "── %s  (%s/%s, %d cpu, storage=%s)\n",
		r.Scenario, r.Host.OS, r.Host.Arch, r.Host.NumCPU, r.Host.Storage)
	for k, v := range r.Params {
		fmt.Fprintf(&b, "   param  %-28s %v\n", k, v)
	}
	keys := make([]string, 0, len(r.Metrics))
	for k := range r.Metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "   metric %-28s %.1f\n", k, r.Metrics[k])
	}
	return b.String()
}

// Percentile returns the p-th percentile (nearest-rank) of an ASCENDING-sorted
// slice; 0 for an empty slice.
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	rank := int(p/100*float64(len(sorted))+0.5) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

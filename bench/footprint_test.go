package bench

import (
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRunFootprintSmall(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "colcad")
	build := exec.Command("go", "build", "-o", bin, "github.com/alpamayo-solutions/colca/cmd/colcad")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build colcad: %v\n%s", err, out)
	}
	r, err := RunFootprint(Params{ColcadPath: bin, Machines: 1, Duration: 2 * time.Second, WorkDir: dir, Storage: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if r.Metrics["footprint_idle_mb"] < 1 {
		t.Fatalf("idle RSS %v MB — implausible", r.Metrics["footprint_idle_mb"])
	}
	if r.Metrics["footprint_loaded_mb"] < r.Metrics["footprint_idle_mb"] {
		t.Fatalf("loaded < idle: %+v", r.Metrics)
	}
}

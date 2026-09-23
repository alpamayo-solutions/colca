package registry_test

import (
	"encoding/json"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/internal/contracts/contractstest"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Under the real generated bundle the element walk must write records the
// node's own schema accepts. parent_id is a ULID or absent there, so a walk
// that wrote "" had every element it authored refused, and each catalogue tag
// naming an element the seed had not placed lost its binding.
func TestTheElementWalkWritesRecordsTheRealBundleAccepts(t *testing.T) {
	tbl, err := contracts.Load(contractstest.GeneratedBundlePath(t), "")
	if err != nil {
		t.Fatalf("real generated bundle failed to load: %v", err)
	}
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg, err := registry.New(st, "n1")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{ULID: "n1", DataDir: t.TempDir()}
	eng := engine.New(st, cfg, reg, func(string, []byte, bool) {}, nil, nil)
	eng.SetContracts(tbl)
	domain := uns.NewConfigExec(eng.EntityStore(), reg, eng.Elements(), nil, registry.NewULID, cfg.Plugin)
	author := func(path string) string {
		t.Helper()
		payload, err := json.Marshal(map[string]string{"path": path})
		if err != nil {
			t.Fatal(err)
		}
		code, msg, _ := domain.Execute(uns.CommandContext{}, "_CmdConfigure", "element/author", payload)
		if code != 200 {
			t.Fatalf("element/author %s: %d %s", path, code, msg)
		}
		return msg
	}

	// A fresh chain: every segment is authored, the first without a parent.
	author("line2/cell1/m1")
	// Under an element the node already holds.
	line1 := author("line1")
	author("line1/m9")

	parentOf := func(path string) string {
		t.Helper()
		raw, ok := eng.EntityStore().KVGet("colca/v1/_SystemElement/n1/" + path)
		if !ok {
			t.Fatalf("no element at %s", path)
		}
		var e struct {
			ParentID string `json:"parent_id"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatal(err)
		}
		return e.ParentID
	}
	if got := parentOf("line1/m9"); got != line1 {
		t.Fatalf("line1/m9 parent_id = %q, want line1's %q", got, line1)
	}
}

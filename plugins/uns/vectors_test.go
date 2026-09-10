package uns

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// vectorPath holds the golden topic-transformation vectors, checked by both
// franzmq's Python suite and this suite. A failure means either a protocol
// change (update the vectors and both suites) or a regression.
const vectorPath = "../../contracts/src/colca_data_contracts/vectors/topic_transformations.json"

type vectorFile struct {
	Parse []struct {
		Topic    string `json:"topic"`
		OK       bool   `json:"ok"`
		Prefix   string `json:"prefix"`
		Version  string `json:"version"`
		Contract string `json:"contract"`
		NodeID   string `json:"node_id"`
		Path     string `json:"path"`
	} `json:"parse"`
	MountInsert []struct {
		Topic string `json:"topic"`
		Mount string `json:"mount"`
		Out   string `json:"out"`
	} `json:"mount_insert"`
	MountStrip []struct {
		Topic string `json:"topic"`
		Mount string `json:"mount"`
		OK    bool   `json:"ok"`
		Out   string `json:"out"`
	} `json:"mount_strip"`
	IdentityRule []struct {
		Topic           string `json:"topic"`
		AuthenticatedAs string `json:"authenticated_as"`
		OK              bool   `json:"ok"`
	} `json:"identity_rule"`
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(vectorPath))
	if err != nil {
		t.Fatalf("golden vectors missing (schema-bundle design §2): %v", err)
	}
	var v vectorFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("golden vectors unparseable: %v", err)
	}
	if len(v.Parse) == 0 || len(v.MountInsert) == 0 || len(v.MountStrip) == 0 {
		t.Fatal("golden vectors empty — the dataset must cover parse, mount_insert and mount_strip")
	}
	return v
}

func TestGoldenVectorsParse(t *testing.T) {
	for _, c := range loadVectors(t).Parse {
		p, err := Parse(c.Topic)
		if !c.OK {
			if err == nil {
				t.Errorf("Parse(%q) accepted, vectors say reject", c.Topic)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q) rejected (%v), vectors say ok", c.Topic, err)
			continue
		}
		if p.Prefix != c.Prefix || p.Version != c.Version || p.Contract != c.Contract || p.NodeID != c.NodeID || p.Path != c.Path {
			t.Errorf("Parse(%q) = %+v, vectors want prefix=%q version=%q contract=%q node_id=%q path=%q",
				c.Topic, p, c.Prefix, c.Version, c.Contract, c.NodeID, c.Path)
		}
	}
}

func TestGoldenVectorsMountInsert(t *testing.T) {
	for _, c := range loadVectors(t).MountInsert {
		if got := MountInsert(c.Topic, c.Mount); got != c.Out {
			t.Errorf("MountInsert(%q, %q) = %q, vectors want %q", c.Topic, c.Mount, got, c.Out)
		}
	}
}

func TestGoldenVectorsMountStrip(t *testing.T) {
	for _, c := range loadVectors(t).MountStrip {
		got, ok := MountStrip(c.Topic, c.Mount)
		if ok != c.OK {
			t.Errorf("MountStrip(%q, %q) ok=%v, vectors want %v", c.Topic, c.Mount, ok, c.OK)
			continue
		}
		if ok && got != c.Out {
			t.Errorf("MountStrip(%q, %q) = %q, vectors want %q", c.Topic, c.Mount, got, c.Out)
		}
	}
}

// The identity rule lives in the engine; its topic half is checked here: the
// verdict must match "node id equals the authenticated identity" for
// non-command contracts, and commands are exempt.
func TestGoldenVectorsIdentityRule(t *testing.T) {
	for _, c := range loadVectors(t).IdentityRule {
		p, err := Parse(c.Topic)
		if err != nil {
			t.Errorf("identity vector topic %q does not parse: %v", c.Topic, err)
			continue
		}
		exempt := ClassOf(p.Contract) == ClassCmd
		got := exempt || p.NodeID == c.AuthenticatedAs
		if got != c.OK {
			t.Errorf("identity rule for %q as %q: got %v, vectors want %v", c.Topic, c.AuthenticatedAs, got, c.OK)
		}
	}
}

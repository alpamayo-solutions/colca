package engine

import (
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// With a configured root the namespace moves: a record under the new root is
// stored, a topic under the default root is plain broker traffic, and the admin
// door refuses to store anything outside the root.
func TestAConfiguredRootIsTheNamespace(t *testing.T) {
	before := uns.Root()
	if err := uns.SetRoot("acme"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = uns.SetRoot(before) })
	e := newTestEngine(t, "n-edge1")

	res, err := e.IngestClient("01JSVC", "acme/v1/_Metric/n-edge1/line1/temp", []byte(`{"v":7}`))
	if err != nil || !res.Persisted {
		t.Fatalf("publish under the configured root: persisted=%v err=%v, want it stored", res.Persisted, err)
	}

	res, err = e.IngestClient("01JSVC", "colca/v1/_Metric/n-edge1/line1/temp", []byte(`{"v":7}`))
	if err != nil || res.Persisted {
		t.Fatalf("publish under the default root: persisted=%v err=%v, want plain broker traffic", res.Persisted, err)
	}

	if _, err := e.IngestAdmin("colca/v1/_Metric/n-edge1/line1/temp", []byte(`{"v":7}`)); err == nil {
		t.Fatal("the admin door stored a record outside the configured root")
	}
}

package repl

import (
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A node-private record, the Edit replay receipt, never leaves its node, nor
// does its tombstone, while the ordinary entities around it rise and the uplink
// cursor moves past what stayed home. The two _Signal records on the same parent
// stream show the lane was read at all, so an empty parent stream cannot pass.
// Making leavesTheNode return true turns the absence check red.
func TestANodePrivateReceiptNeverLeavesTheNodeButItsNeighboursDo(t *testing.T) {
	cs, ps, parentPub, start := uplinkPair(t)

	const receipt = "colca/v1/_EditOperation/n-child/_colca/edit/operations/op-1"
	if _, _, err := cs.Append("entities", []store.Record{
		{Topic: "colca/v1/_Signal/n-child/press/temp", Payload: []byte(`{"id":"sig-1"}`), TS: 1},
		{Topic: receipt, Payload: []byte(`{"id":"op-1","digest":"d","message":"created","result":"ok","topics":["colca/v1/_Signal/n-child/press/temp"]}`), TS: 2},
		{Topic: receipt, TS: 3}, // the receipt's tombstone: the per-node cap pruned it
		{Topic: "colca/v1/_Signal/n-child/press/speed", Payload: []byte(`{"id":"sig-2"}`), TS: 4},
	}); err != nil {
		t.Fatal(err)
	}
	head := cs.NextOffset("entities")

	start()
	// The lane is drained when the cursor reaches the child's head, not merely when
	// the parent has two records: a filter that dropped the receipt but stalled on
	// it would also leave two records.
	waitFor(t, "the child's entities uplink cursor to reach its head", 20*time.Second, func() bool {
		return cs.CursorGet(uns.UplinkCursor(parentPub), "entities") == head
	})

	// The parent's entities stream also holds what it authored itself (the child's
	// element and enrollment); level 4 names the author, so the child's records are
	// those under its ULID.
	all, _, err := ps.Read("entities", 1, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	var recs []store.StoredRecord
	var got []string
	for _, r := range all {
		if p, perr := uns.Parse(r.Topic); perr == nil && p.NodeID == "n-child" {
			recs = append(recs, r)
			got = append(got, r.Topic)
		}
	}
	want := []string{
		"colca/v1/_Signal/n-child/child1/press/temp",
		"colca/v1/_Signal/n-child/child1/press/speed",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("parent entities stream after the drain:\n  got  %q\n  want %q\n"+
			"— exactly the two signals, mount-inserted, and neither the _EditOperation "+
			"receipt nor its tombstone (uns.IsNodePrivate, applied by the uplink's leavesTheNode)",
			got, want)
	}
	for _, r := range recs {
		if len(r.Payload) == 0 {
			t.Fatalf("a tombstone reached the parent at %q — the receipt's retirement must stay home with the receipt", r.Topic)
		}
	}
}

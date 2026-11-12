// Revoking a local service retires its catalogue, so a successor under
// another name can take over its tags and produce their signals: the hand-over
// a connector migration makes.
package tests

import (
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/authtest"
)

func TestARevokedLocalServicesCatalogueHandsItsTagsOver(t *testing.T) {
	n := startCarryNode(t)
	authtest.Place(t, n.Engine, "line1")
	const tag = "01JTAG-MOTORSTROM"
	catalogue := `{"data_tags":[{"id":"` + tag + `","name":"motorstrom","data_type":"float"}]}`

	if code, out := localPublish(t, n, "conn-old", "colca/v1/_DataTags/"+carryNode+"/conn-old", catalogue); code != 200 {
		t.Fatalf("old connector's catalogue: %d %v", code, out)
	}
	old, ok := n.Registry.ByName("conn-old")
	if !ok {
		t.Fatal("conn-old did not register")
	}
	configure(t, n, "signal/autobind", map[string]any{"connector": old.ULID, "under": "line1"})
	metric := "colca/v1/_Metric/" + carryNode + "/" + signalPathFor(t, n, tag)
	if code, out := localPublish(t, n, "conn-old", metric, `{"v":1}`); code != 200 {
		t.Fatalf("old connector's value: %d %v", code, out)
	}

	api(t, n, "DELETE", "/enroll/"+old.ULID, nil)
	waitFor(t, "the old catalogue retired", 5*time.Second, func() bool {
		return retainedAt(t, n, "colca/v1/_DataTags/"+carryNode+"/conn-old") == nil
	})

	if code, out := localPublish(t, n, "conn-new", "colca/v1/_DataTags/"+carryNode+"/conn-new", catalogue); code != 200 {
		t.Fatalf("new connector's catalogue: %d %v", code, out)
	}
	if code, out := localPublish(t, n, "conn-new", metric, `{"v":2}`); code != 200 {
		t.Fatalf("the successor's value was refused: %d %v; the revoked catalogue still claims %s", code, out, tag)
	}
}

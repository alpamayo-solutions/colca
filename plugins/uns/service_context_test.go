package uns

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

const serviceContextVectorPath = "../../contracts/src/colca_data_contracts/vectors/service_context.json"

type serviceContextVector struct {
	Mount   string   `json:"mount"`
	Service string   `json:"service"`
	Context []string `json:"context"`
}

// The Python side (tests/test_local_service.py) reads the same vectors, so a
// change to either implementation must be reflected here.
func TestServiceContextMatchesTheSharedVectors(t *testing.T) {
	raw, err := os.ReadFile(serviceContextVectorPath)
	if err != nil {
		t.Fatalf("golden service_context vectors missing: %v", err)
	}
	var doc struct {
		Vectors []serviceContextVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("golden service_context vectors unparseable: %v", err)
	}
	if len(doc.Vectors) == 0 {
		t.Fatal("golden service_context vectors are empty — this test would pass proving nothing")
	}
	for _, vector := range doc.Vectors {
		got := ServiceContext(vector.Mount, vector.Service)
		if strings.Join(got, "/") != strings.Join(vector.Context, "/") {
			t.Errorf("ServiceContext(%q, %q) = %v, want %v",
				vector.Mount, vector.Service, got, vector.Context)
		}
	}
}

func TestTwoUnplacedServicesGetDifferentAddresses(t *testing.T) {
	// An unplaced service has no mount, so without its name every unplaced
	// service on a node would write its registration to the same topic.
	projector := strings.Join(ServiceContext("", "projector"), "/")
	dataops := strings.Join(ServiceContext("", "dataops"), "/")
	if projector == dataops {
		t.Fatalf("both resolved to %q — one service erases the other", projector)
	}
}

package engine

import (
	"testing"
	"time"
)

// A definition arriving from the parent reaches the record observer, so the
// domain plugin can apply it to state the node already holds (a semantic tag
// a bound signal's tag names, say).
func TestADownlinkedDefinitionReachesTheObserver(t *testing.T) {
	e := newEngine(t)
	obs := &recordingObserver{}
	e.SetObserver(obs)

	topic := "colca/v1/_SemanticTag/n-parent/01STAGHEALTH"
	payload := []byte(`{"id":"01STAGHEALTH","name":"health_indicator"}`)
	if _, err := e.IngestDownlinkDefinition(topic, payload, time.Now().UnixMilli()); err != nil {
		t.Fatalf("downlink definition: %v", err)
	}

	if len(obs.seen) != 1 || obs.seen[0] != "_SemanticTag "+topic {
		t.Fatalf("observer saw %v, want the downlinked definition once", obs.seen)
	}
}

// A definition the node authors through the admin door reaches the observer
// too; other admin records still do not.
func TestAnAdminDefinitionReachesTheObserver(t *testing.T) {
	e := newEngine(t)
	obs := &recordingObserver{}
	e.SetObserver(obs)

	topic := "colca/v1/_SemanticTag/n-edge1/01STAGHEALTH"
	if _, err := e.IngestAdmin(topic, []byte(`{"id":"01STAGHEALTH","name":"health_indicator"}`)); err != nil {
		t.Fatalf("admin definition: %v", err)
	}
	if _, err := e.IngestAdmin("colca/v1/_SystemElement/n-edge1/m2", []byte(`{"id":"el-m2","name":"m2"}`)); err != nil {
		t.Fatalf("admin element: %v", err)
	}

	if len(obs.seen) != 1 || obs.seen[0] != "_SemanticTag "+topic {
		t.Fatalf("observer saw %v, want only the definition", obs.seen)
	}
}

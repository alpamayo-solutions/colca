package engine

import (
	"errors"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

func signalOf(path, payload string) uns.StateRecord {
	return uns.StateRecord{Topic: "colca/v1/_Signal/n-edge1/press/" + path, Payload: []byte(payload)}
}

// The commit door holds the relationship rules for every executor: a link to
// a signal the node does not hold is refused and nothing is written, and the
// same link commits once its target exists.
func TestACommitWithABrokenSignalRelationshipIsRefused(t *testing.T) {
	e := newEngine(t)
	store := e.EntityStore()
	setpoint := signalOf("sp", `{"id":"01JSIG00000000000000000002","name":"sp","data_type":"float",`+
		`"relationships":[{"relationship_type":"actual","target_signal_id":"01JSIG00000000000000000001"}]}`)
	before := e.Store().NextOffset("entities")

	_, err := store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{setpoint})

	var refusal *uns.RelationshipError
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %v, want a RelationshipError", err)
	}
	if got := e.Store().NextOffset("entities"); got != before {
		t.Fatalf("entities advanced to %d from %d: a refused batch wrote", got, before)
	}
	speed := signalOf("speed", `{"id":"01JSIG00000000000000000001","name":"speed","data_type":"float"}`)
	if _, err := store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{speed, setpoint}); err != nil {
		t.Fatalf("a link to a target in the same batch was refused: %v", err)
	}
}

package engine

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// referenceOf is one _ExternalReference record of n-edge1 linking el-m1 to
// row of the equipment table.
func referenceOf(id, row string) uns.StateRecord {
	return uns.StateRecord{
		Topic: "colca/v1/_ExternalReference/n-edge1/_colca/external-references/" + id,
		Payload: []byte(fmt.Sprintf(
			`{"id":%q,"source_entity":"SystemElement","source_object_id":"el-m1",`+
				`"relationship_type":"access:equipment","external_system_id":"01JEXTERNAL00000000000000A",`+
				`"external_table":"equipment","external_row_id":%q}`, id, row)),
	}
}

func referencesHeld(e *Engine) int {
	return len(e.EntityStore().KVScan("_ExternalReference", "n-edge1"))
}

// The commit door refuses a second record for a reference the node holds,
// whichever executor composed it, and names the holder.
func TestACommitThatDuplicatesAReferenceIsRefused(t *testing.T) {
	e := newEngine(t)
	store := e.EntityStore()
	if _, err := store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{
		referenceOf("01JREF0000000000000000000A", "7"),
	}); err != nil {
		t.Fatal(err)
	}
	before := e.Store().NextOffset("entities")

	_, err := store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{
		referenceOf("01JREF0000000000000000000B", "7"),
	})

	var duplicate *uns.ReferenceHeldError
	if !errors.As(err, &duplicate) || duplicate.Holder != "01JREF0000000000000000000A" {
		t.Fatalf("err = %v, want the reference held by 01JREF…A", err)
	}
	if got := e.Store().NextOffset("entities"); got != before {
		t.Fatalf("entities advanced to %d from %d: a refused batch wrote", got, before)
	}
	if _, err := store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{
		referenceOf("01JREF0000000000000000000B", "8"),
	}); err != nil {
		t.Fatalf("another row was refused: %v", err)
	}
}

// Commands run by different executors hold different locks, so the commit
// door is where two writes of one reference meet. However they interleave,
// one commits and every other is refused.
func TestConcurrentCommitsOfOneReferenceLeaveOneRecord(t *testing.T) {
	e := newEngine(t)
	store := e.EntityStore()
	const n = 16
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{
				referenceOf(fmt.Sprintf("01JREF0000000000000000%04d", i), "7"),
			})
		}(i)
	}
	wg.Wait()

	committed := 0
	for _, err := range errs {
		var duplicate *uns.ReferenceHeldError
		switch {
		case err == nil:
			committed++
		case errors.As(err, &duplicate):
		default:
			t.Fatalf("unexpected refusal: %v", err)
		}
	}
	if held := referencesHeld(e); committed != 1 || held != 1 {
		t.Fatalf("committed %d, the node holds %d records; want exactly one of each", committed, held)
	}
}

// The admin publish door writes the same record, so it is held to the same
// rule, and may still remove a record.
func TestAnAdminPublishThatDuplicatesAReferenceIsRefused(t *testing.T) {
	e := newEngine(t)
	first, second := referenceOf("01JREF0000000000000000000A", "7"), referenceOf("01JREF0000000000000000000B", "7")
	if _, err := e.IngestAdmin(first.Topic, first.Payload); err != nil {
		t.Fatal(err)
	}

	_, err := e.IngestAdmin(second.Topic, second.Payload)

	var duplicate *uns.ReferenceHeldError
	if !errors.As(err, &duplicate) || referencesHeld(e) != 1 {
		t.Fatalf("err = %v with %d records held, want the duplicate refused", err, referencesHeld(e))
	}
	if _, err := e.IngestAdmin(first.Topic, nil); err != nil {
		t.Fatalf("retiring the holder: %v", err)
	}
	if _, err := e.IngestAdmin(second.Topic, second.Payload); err != nil {
		t.Fatalf("the freed reference was refused: %v", err)
	}
}

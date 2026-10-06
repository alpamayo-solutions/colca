package engine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const heldTag = "01JTAG0000000000000000000A"

// signalAt is one _Signal record of n-edge1, bound to tag when tag is set.
func signalAt(path, id, tag string) uns.StateRecord {
	payload := fmt.Sprintf(`{"id":%q,"name":%q}`, id, path)
	if tag != "" {
		payload = fmt.Sprintf(`{"id":%q,"name":%q,"data_tag":%q}`, id, path, tag)
	}
	return uns.StateRecord{Topic: "colca/v1/_Signal/n-edge1/" + path, Payload: []byte(payload)}
}

// The commit door refuses a batch that binds a tag another signal holds,
// whichever executor composed it, and names the holder.
func TestACommitThatBindsAHeldTagIsRefused(t *testing.T) {
	e := newEngine(t)
	store := e.EntityStore()
	if _, err := store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{
		signalAt("line1/temp", "01JSIG000000000000000000A1", heldTag),
		signalAt("line1/other", "01JSIG000000000000000000B1", ""),
	}); err != nil {
		t.Fatal(err)
	}
	before := e.Store().NextOffset("entities")

	_, err := store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{
		signalAt("line1/other", "01JSIG000000000000000000B1", heldTag),
	})

	var held *uns.TagHeldError
	if !errors.As(err, &held) || held.Holder != "01JSIG000000000000000000A1" || held.HolderPath != "line1/temp" {
		t.Fatalf("err = %v, want the tag held by 01JSIG…A1 at line1/temp", err)
	}
	if got := e.Store().NextOffset("entities"); got != before {
		t.Fatalf("entities advanced to %d from %d: a refused batch wrote", got, before)
	}
}

// Commands run by different executors hold different locks, so the commit
// door is where two binds of one tag meet. However they interleave, one
// commits and every other is refused.
func TestConcurrentCommitsBindingOneTagLeaveOneHolder(t *testing.T) {
	e := newEngine(t)
	store := e.EntityStore()
	const n = 16
	seed := make([]uns.StateRecord, n)
	for i := range seed {
		seed[i] = signalAt(fmt.Sprintf("line1/s%02d", i), fmt.Sprintf("01JSIG0000000000000000%04d", i), "")
	}
	if _, err := store.PublishBatch(uns.CommandContext{}, seed); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = store.PublishBatch(uns.CommandContext{}, []uns.StateRecord{
				signalAt(fmt.Sprintf("line1/s%02d", i), fmt.Sprintf("01JSIG0000000000000000%04d", i), heldTag),
			})
		}(i)
	}
	wg.Wait()

	committed := 0
	for _, err := range errs {
		var held *uns.TagHeldError
		switch {
		case err == nil:
			committed++
		case errors.As(err, &held):
		default:
			t.Fatalf("unexpected refusal: %v", err)
		}
	}
	holders := 0
	for _, rec := range e.EntityStore().KVScan("_Signal", "n-edge1") {
		if bytes.Contains(rec.Payload, []byte(heldTag)) {
			holders++
		}
	}
	if committed != 1 || holders != 1 {
		t.Fatalf("committed %d, %d signals hold the tag; want exactly one of each", committed, holders)
	}
}

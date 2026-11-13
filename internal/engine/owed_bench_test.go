package engine

import (
	"fmt"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// BenchmarkOwedCheck is the check a live command for machine m1 makes before
// it is published, on a commands stream that holds 100k commands for other
// machines since m1's delivery floor, and acks.
//
//	go test ./internal/engine -run '^$' -bench OwedCheck
func BenchmarkOwedCheck(b *testing.B) {
	e := newEngine(b)
	const stream = "commands"
	floor := e.store.NextOffset(stream)
	for base := 0; base < 100_000; base += 1000 {
		recs := make([]store.Record, 0, 1000)
		for i := base; i < base+1000; i++ {
			target := fmt.Sprintf("m%d", 2+i%50)
			topic := "colca/v1/_CmdParam/" + target + "/temp/set"
			if i%2 == 1 {
				topic = "colca/v1/_Ack/" + target + "/temp/set"
			}
			recs = append(recs, store.Record{Topic: topic, Payload: []byte(fmt.Sprintf(`{"correlation_id":"c%d","expires_at":9999999999999}`, i)), TS: int64(i) + 1})
		}
		if _, _, err := e.store.Append(stream, recs); err != nil {
			b.Fatal(err)
		}
	}
	offset := e.store.NextOffset(stream)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e.owedBelow(stream, floor, offset, "m1") {
			b.Fatal("owed")
		}
	}
}

// The owed check reads the target's own commands only, within a budget: more
// of them than the budget between the floor and the record holds the record
// (the replay delivers in order) instead of reading on without bound. Fewer
// expired ones are read through and owe nothing.
func TestAnOwedCheckReadsItsMachinesCommandsWithinABudget(t *testing.T) {
	e := newEngine(t)
	const stream = "commands"
	appendExpired := func(target string, n int) {
		t.Helper()
		for base := 0; base < n; base += 1000 {
			recs := make([]store.Record, 0, 1000)
			for i := base; i < min(n, base+1000); i++ {
				recs = append(recs, store.Record{Topic: "colca/v1/_CmdParam/" + target + "/temp/set",
					Payload: []byte(fmt.Sprintf(`{"correlation_id":"c%d","expires_at":1000}`, i)), TS: 1})
			}
			if _, _, err := e.store.Append(stream, recs); err != nil {
				t.Fatal(err)
			}
		}
	}
	floor := e.store.NextOffset(stream)
	appendExpired("m2", 20_000) // other machines' commands cost nothing
	appendExpired("m1", owedScanBudget-1)
	if offset := e.store.NextOffset(stream); e.owedBelow(stream, floor, offset, "m1") {
		t.Fatal("expired commands within the budget were taken as owed")
	}
	appendExpired("m1", 2)
	if offset := e.store.NextOffset(stream); !e.owedBelow(stream, floor, offset, "m1") {
		t.Fatal("a check past its budget did not hold the command")
	}
	// A live one for m1 is owed, whatever lies before it.
	e2 := newEngine(t)
	floor = e2.store.NextOffset(stream)
	if _, _, err := e2.store.Append(stream, []store.Record{{Topic: "colca/v1/_CmdParam/m1/temp/set",
		Payload: []byte(`{"correlation_id":"live","expires_at":9999999999999}`), TS: 1}}); err != nil {
		t.Fatal(err)
	}
	if !e2.owedBelow(stream, floor, e2.store.NextOffset(stream), "m1") {
		t.Fatal("a live older command was not owed")
	}
}

package repl

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
)

func appendRecords(tb testing.TB, st *store.Store, stream string, topics ...string) {
	tb.Helper()
	recs := make([]store.Record, len(topics))
	for i, topic := range topics {
		recs[i] = store.Record{Topic: topic, Payload: []byte(`{"v":1,"note":"a payload the uplink decodes only when it sends it"}`), TS: int64(i + 1)}
	}
	if _, _, err := st.Append(stream, recs); err != nil {
		tb.Fatal(err)
	}
}

// The uplink reads a physical page and returns only what rises: the lane's
// filter and the log policy step over the rest, which still moves next, and the
// logs kept home come back with their offsets so they are counted once.
func TestTheUplinkPageReturnsOnlyWhatRises(t *testing.T) {
	st := mustStore(t, filepath.Join(t.TempDir(), "data"))
	c := &Client{logs: newLogForwarding(config.ParentLogs{})}

	appendRecords(t, st, "commands",
		"colca/v1/_CmdParam/n1/line/go",
		"colca/v1/_Ack/n1/line/go",
		"colca/v1/_CmdParam/n1/line/stop",
	)
	recs, withheld, next, err := c.readUplinkPage(st, "commands", 1, pushable(ackOnly))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Offset != 2 || string(recs[0].Payload) == "" || len(withheld) != 0 || next != 4 {
		t.Fatalf("commands lane: recs %+v withheld %v next %d, want the ack at 2 with its payload and next 4", recs, withheld, next)
	}

	appendRecords(t, st, "logs",
		"colca/v1/_Log/n1/colca/DEBUG",
		"colca/v1/_Log/n1/colca/WARNING",
		"colca/v1/_Log/n1/colca/INFO",
	)
	recs, withheld, next, err = c.readUplinkPage(st, "logs", 1, pushable(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Offset != 2 || next != 4 {
		t.Fatalf("logs lane: recs %+v next %d, want the warning at 2 and next 4", recs, next)
	}
	if fmt.Sprint(withheld) != "[{1 DEBUG} {3 INFO}]" {
		t.Fatalf("withheld %v, want DEBUG at 1 and INFO at 3", withheld)
	}

	// An empty lane: next stays.
	if recs, _, next, err := c.readUplinkPage(st, "alarms", 1, pushable(nil)); err != nil || len(recs) != 0 || next != 1 {
		t.Fatalf("empty lane: recs %d next %d err %v", len(recs), next, err)
	}
}

// A page is replBatch records long, the ones kept home included, so a lane that
// keeps nearly everything home still moves a page at a time.
func TestTheUplinkPageIsBoundedByWhatItCrosses(t *testing.T) {
	st := mustStore(t, filepath.Join(t.TempDir(), "data"))
	c := &Client{logs: newLogForwarding(config.ParentLogs{})}
	topics := make([]string, replBatch+10)
	for i := range topics {
		topics[i] = "colca/v1/_CmdParam/n1/line/go"
	}
	topics[replBatch+5] = "colca/v1/_Ack/n1/line/go"
	appendRecords(t, st, "commands", topics...)

	recs, _, next, err := c.readUplinkPage(st, "commands", 1, pushable(ackOnly))
	if err != nil || len(recs) != 0 || next != replBatch+1 {
		t.Fatalf("first page: recs %d next %d err %v, want none and next %d", len(recs), next, err, replBatch+1)
	}
	recs, _, next, err = c.readUplinkPage(st, "commands", next, pushable(ackOnly))
	if err != nil || len(recs) != 1 || recs[0].Offset != replBatch+6 || next != replBatch+11 {
		t.Fatalf("second page: recs %+v next %d err %v", recs, next, err)
	}
}

// BenchmarkUplinkPage compares the uplink's read of a commands page, where only
// acks rise, and of a logs page under the default policy (WARNING and up),
// with the read it replaces: every record of the page decoded, then filtered.
//
//	go test -run '^$' -bench UplinkPage -benchmem ./internal/repl/
func BenchmarkUplinkPage(b *testing.B) {
	st, err := store.Open(filepath.Join(b.TempDir(), "data"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { st.Close() })
	c := &Client{logs: newLogForwarding(config.ParentLogs{})}
	commands := make([]string, replBatch)
	logs := make([]string, replBatch)
	levels := []string{"DEBUG", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "WARNING"}
	for i := range commands {
		commands[i] = fmt.Sprintf("colca/v1/_CmdParam/n1/site/line%d/press/go", i%20)
		if i%10 == 0 {
			commands[i] = fmt.Sprintf("colca/v1/_Ack/n1/site/line%d/press/go", i%20)
		}
		logs[i] = "colca/v1/_Log/n1/site/line/connector/" + levels[i%len(levels)]
	}
	appendRecords(b, st, "commands", commands...)
	appendRecords(b, st, "logs", logs...)

	for _, lane := range []struct {
		stream string
		filter func(string) bool
	}{{"commands", ackOnly}, {"logs", nil}} {
		allowed := pushable(lane.filter)
		b.Run(lane.stream+"/decode-all", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				recs, _, err := st.Read(lane.stream, 1, replBatch, nil)
				if err != nil {
					b.Fatal(err)
				}
				for _, r := range recs {
					if allowed(r.Topic) && lane.stream == logsStream {
						c.logs.forward(r.Topic)
					}
				}
			}
		})
		b.Run(lane.stream+"/head", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, _, _, err := c.readUplinkPage(st, lane.stream, 1, allowed); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

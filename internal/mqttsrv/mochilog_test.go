package mqttsrv

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type capturedRecord struct {
	message string
	attrs   map[string]slog.Value
}

type capturingHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler            { return h }
func (h *capturingHandler) Handle(_ context.Context, record slog.Record) error {
	captured := capturedRecord{message: record.Message, attrs: map[string]slog.Value{}}
	record.Attrs(func(attr slog.Attr) bool {
		captured.attrs[attr.Key] = attr.Value
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, captured)
	h.mu.Unlock()
	return nil
}

func mochiRecord(at time.Time, msg string, attrs ...slog.Attr) slog.Record {
	record := slog.NewRecord(at, slog.LevelWarn, msg, 0)
	record.AddAttrs(attrs...)
	return record
}

// A flood of one line — mochi's per-occurrence "client store quota reached"
// — must reach the log once per window, and the first line of the next
// window must say how many were swallowed. Without this bound a single slow
// QoS>0 subscriber writes the broker's whole log for it.
func TestARepeatedMochiLineLogsOncePerWindowWithACount(t *testing.T) {
	sink := &capturingHandler{}
	handler := newMochiLogHandler(sink)
	start := time.Now()

	for i := 0; i < 500; i++ {
		record := mochiRecord(start.Add(time.Duration(i)*time.Millisecond),
			"client store quota reached", slog.String("client", "dataops-1"))
		if err := handler.Handle(context.Background(), record); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if len(sink.records) != 1 {
		t.Fatalf("expected 1 delivered record inside the window, got %d", len(sink.records))
	}

	next := mochiRecord(start.Add(mochiLogWindow+time.Second),
		"client store quota reached", slog.String("client", "dataops-1"))
	if err := handler.Handle(context.Background(), next); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(sink.records) != 2 {
		t.Fatalf("expected the next window's first record to be delivered, got %d", len(sink.records))
	}
	suppressed, ok := sink.records[1].attrs["repeats_suppressed"]
	if !ok || suppressed.Int64() != 499 {
		t.Fatalf("expected repeats_suppressed=499 on the next window's record, got %v", sink.records[1].attrs)
	}
}

// Different clients are different facts: the bound is per line, not global.
func TestDistinctClientsAreNotSuppressedTogether(t *testing.T) {
	sink := &capturingHandler{}
	handler := newMochiLogHandler(sink)
	at := time.Now()

	for _, client := range []string{"a", "b", "c"} {
		record := mochiRecord(at, "client store quota reached", slog.String("client", client))
		if err := handler.Handle(context.Background(), record); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if len(sink.records) != 3 {
		t.Fatalf("expected one record per client, got %d", len(sink.records))
	}
}

// mochi logs some transport errors with an empty message — a WARN that is
// nothing but attrs. It gets a name, so the line can be searched for.
func TestAnEmptyMochiMessageGetsAName(t *testing.T) {
	sink := &capturingHandler{}
	handler := newMochiLogHandler(sink)

	record := mochiRecord(time.Now(), "", slog.String("listener", "human-ws"))
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(sink.records) != 1 || sink.records[0].message != "mqtt transport error" {
		t.Fatalf("expected the empty message to be named, got %+v", sink.records)
	}
}

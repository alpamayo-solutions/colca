package mqttsrv

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

type capturedRecord struct {
	level   slog.Level
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
	captured := capturedRecord{level: record.Level, message: record.Message, attrs: map[string]slog.Value{}}
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

// A flood of one line logs once per window, and the next window's first line says
// how many were swallowed.
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

type infoLevel struct{ *capturingHandler }

func (infoLevel) Enabled(_ context.Context, level slog.Level) bool { return level >= slog.LevelInfo }

// A browser tab that closes or navigates away ends its websocket with 1001, or
// with 1005 when it sends no status: a client leaving, logged at debug.
func TestAWebsocketTheBrowserClosedIsDebugNotAWarning(t *testing.T) {
	sink := &capturingHandler{}
	for _, text := range []string{"websocket: close 1001 (going away)", "websocket: close 1005 (no status)"} {
		record := mochiRecord(time.Now(), "", slog.String("listener", "human-ws"), slog.String("error", text))
		if err := newMochiLogHandler(sink).Handle(context.Background(), record); err != nil {
			t.Fatalf("handle: %v", err)
		}
	}
	if len(sink.records) != 2 {
		t.Fatalf("expected both closes at debug, got %+v", sink.records)
	}
	for _, record := range sink.records {
		if record.level != slog.LevelDebug || record.message != "mqtt client closed its websocket" {
			t.Fatalf("expected a debug line, got %+v", record)
		}
	}

	quiet := &capturingHandler{}
	closed := mochiRecord(time.Now(), "", slog.Any("error", errString("websocket: close 1001 (going away)")))
	if err := newMochiLogHandler(infoLevel{quiet}).Handle(context.Background(), closed); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(quiet.records) != 0 {
		t.Fatalf("expected nothing at info level, got %+v", quiet.records)
	}
}

// An abnormal closure is still a transport fault.
func TestAnAbnormalWebsocketCloseStaysAWarning(t *testing.T) {
	sink := &capturingHandler{}
	record := mochiRecord(time.Now(), "", slog.String("error", "websocket: close 1006 (abnormal closure): unexpected EOF"))
	if err := newMochiLogHandler(sink).Handle(context.Background(), record); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(sink.records) != 1 || sink.records[0].level != slog.LevelWarn || sink.records[0].message != "mqtt transport error" {
		t.Fatalf("expected a warning, got %+v", sink.records)
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// A refused publish is one short debug line with its topic and size, never the
// packet with its payload bytes.
func TestARefusedPublishLogsItsTopicAndSizeNotThePacket(t *testing.T) {
	sink := &capturingHandler{}
	handler := newMochiLogHandler(sink)
	pk := packets.Packet{TopicName: "colca/v1/_CmdParam/n1/line1/setDensity", Payload: make([]byte, 3000)}

	record := slog.NewRecord(time.Now(), slog.LevelError, "publish packet error", 0)
	record.AddAttrs(slog.Any("error", packets.ErrPayloadFormatInvalid), slog.String("hook", "colca"), slog.Any("packet", pk))
	if err := handler.Handle(context.Background(), record); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(sink.records) != 1 {
		t.Fatalf("records = %+v", sink.records)
	}
	got := sink.records[0]
	if got.level != slog.LevelDebug || got.attrs["topic"].String() != pk.TopicName || got.attrs["bytes"].Int64() != 3000 {
		t.Fatalf("refused publish logged as %+v", got)
	}
	if _, ok := got.attrs["packet"]; ok {
		t.Fatal("the packet was logged")
	}
}

// One sender's repeated refusals log once per window; another sender's still log.
func TestRefusalsAreLimitedPerSender(t *testing.T) {
	sink := &capturingHandler{}
	log := slog.New(newMochiLogHandler(sink))
	for range 50 {
		log.Warn("human publish rejected", "sub", "anna", "topic", "t", "bytes", 3000)
	}
	log.Warn("human publish rejected", "sub", "bert", "topic", "t", "bytes", 3000)
	if len(sink.records) != 2 {
		t.Fatalf("expected one line per sender, got %d", len(sink.records))
	}
}

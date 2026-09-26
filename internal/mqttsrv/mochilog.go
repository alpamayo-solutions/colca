package mqttsrv

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
)

// wsClosedNormally are the close codes a browser sends when a tab closes or
// navigates away (1001) or when it closes without a status (1005), and a
// normal close (1000): a client leaving, not a transport fault.
var wsClosedNormally = []string{"websocket: close 1000 ", "websocket: close 1001 ", "websocket: close 1005 "}

// closedNormally says whether the record's error is a websocket closed by its client.
func closedNormally(record slog.Record) bool {
	normal := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "error" {
			return true
		}
		text := attr.Value.String()
		for _, code := range wsClosedNormally {
			if strings.Contains(text+" ", code) {
				normal = true
			}
		}
		return false
	})
	return normal
}

// mochiLogWindow is how long one repeated mochi log line stays suppressed
// before the next occurrence is let through (carrying the suppressed count).
const mochiLogWindow = time.Minute

// mochiLogKeyLimit bounds the suppression table. Keys carry client ids, so a
// churn of short-lived clients would otherwise grow it without bound; past
// the limit the stalest entries are dropped, which only means an early
// repeat logs once more.
const mochiLogKeyLimit = 1024

// mochiLogHandler wraps mochi's logging to fix three problems: some transport errors
// are logged with an empty message, which get a name here; mochi logs every
// occurrence, so one slow subscriber can flood the log with "client store quota
// reached"; and a refused publish comes with the whole packet, payload bytes and
// all. Identical lines log once per window, and the next one reports how many
// were suppressed; a refused publish is a debug line with its topic and size,
// beside the hook's own warning. Colca's own logging goes through it only for
// those refusals, which a sender can repeat as fast as it likes.
type mochiLogHandler struct {
	inner slog.Handler
	state *mochiLogShared
}

// mochiLogShared is the suppression table and its lock, shared by every WithAttrs
// and WithGroup derivative, so one lock guards the map.
type mochiLogShared struct {
	mu   sync.Mutex
	seen map[string]*mochiLogState
}

type mochiLogState struct {
	windowStart time.Time
	suppressed  int
}

func newMochiLogHandler(inner slog.Handler) *mochiLogHandler {
	return &mochiLogHandler{
		inner: inner,
		state: &mochiLogShared{seen: map[string]*mochiLogState{}},
	}
}

func (h *mochiLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *mochiLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Suppression state is shared on purpose: attrs added via With are part
	// of the record either way, and one table keeps one bound.
	return &mochiLogHandler{inner: h.inner.WithAttrs(attrs), state: h.state}
}

func (h *mochiLogHandler) WithGroup(name string) slog.Handler {
	return &mochiLogHandler{inner: h.inner.WithGroup(name), state: h.state}
}

func (h *mochiLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level > slog.LevelDebug && closedNormally(record) {
		record.Level = slog.LevelDebug
		record.Message = "mqtt client closed its websocket"
		if !h.inner.Enabled(ctx, record.Level) {
			return nil
		}
	}
	if record.Message == "" {
		record.Message = "mqtt transport error"
	}
	if record.Message == mochiPublishError {
		record = withoutPacket(record)
		if !h.inner.Enabled(ctx, record.Level) {
			return nil
		}
	}

	key := record.Level.String() + "|" + record.Message
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "client" || attr.Key == "listener" || attr.Key == "identity" || attr.Key == "sub" {
			key += "|" + attr.Key + "=" + attr.Value.String()
		}
		return true
	})

	now := record.Time
	if now.IsZero() {
		now = time.Now()
	}

	shared := h.state
	shared.mu.Lock()
	state := shared.seen[key]
	if state != nil && now.Sub(state.windowStart) < mochiLogWindow {
		state.suppressed++
		shared.mu.Unlock()
		return nil
	}
	suppressed := 0
	if state != nil {
		suppressed = state.suppressed
	}
	if len(shared.seen) >= mochiLogKeyLimit {
		for staleKey, stale := range shared.seen {
			if now.Sub(stale.windowStart) >= mochiLogWindow {
				delete(shared.seen, staleKey)
			}
		}
	}
	shared.seen[key] = &mochiLogState{windowStart: now}
	shared.mu.Unlock()

	if suppressed > 0 {
		record.AddAttrs(slog.Int("repeats_suppressed", suppressed))
	}
	return h.inner.Handle(ctx, record)
}

// mochiPublishError is the line mochi writes for every publish a hook refused.
const mochiPublishError = "publish packet error"

// withoutPacket is a refused publish's line with the packet reduced to its topic
// and size, at debug: the hook has already said why it refused.
func withoutPacket(record slog.Record) slog.Record {
	out := slog.NewRecord(record.Time, slog.LevelDebug, record.Message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != "packet" {
			out.AddAttrs(attr)
			return true
		}
		if pk, ok := attr.Value.Any().(packets.Packet); ok {
			out.AddAttrs(slog.String("topic", pk.TopicName), slog.Int("bytes", len(pk.Payload)))
		}
		return true
	})
	return out
}

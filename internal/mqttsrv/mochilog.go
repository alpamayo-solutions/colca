package mqttsrv

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// mochiLogWindow is how long one repeated mochi log line stays suppressed
// before the next occurrence is let through (carrying the suppressed count).
const mochiLogWindow = time.Minute

// mochiLogKeyLimit bounds the suppression table. Keys carry client ids, so a
// churn of short-lived clients would otherwise grow it without bound; past
// the limit the stalest entries are dropped, which only means an early
// repeat logs once more.
const mochiLogKeyLimit = 1024

// mochiLogHandler wraps mochi's logging to fix two problems: some transport errors
// are logged with an empty message, which get a name here, and mochi logs every
// occurrence, so one slow subscriber can flood the log with "client store quota
// reached". Identical lines log once per window, and the next one reports how many
// were suppressed. Colca's own logging does not go through it.
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
	if record.Message == "" {
		record.Message = "mqtt transport error"
	}

	key := record.Level.String() + "|" + record.Message
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "client" || attr.Key == "listener" {
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

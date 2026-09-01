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

// mochiLogHandler is the handler colca hands mochi for the broker's own
// logging. It exists for two defects the raw library logger has in
// production:
//
//   - mochi logs some transport errors with an EMPTY message (a WARN that is
//     nothing but attrs) — those get a name here.
//   - mochi logs per OCCURRENCE with no bound: a single slow QoS>0
//     subscriber emits "client store quota reached" for every dropped
//     delivery, which turns the broker's log into megabytes of one line and
//     makes the log floodable by whoever runs one bad client. Identical
//     lines (same level, message, client, listener) log once per window;
//     the next occurrence after the window carries how many were suppressed.
//
// Colca's own log calls do not go through this handler — a rule the broker
// enforces on a library is not a licence to sample its own reporting.
type mochiLogHandler struct {
	inner slog.Handler
	state *mochiLogShared
}

// mochiLogShared is the suppression table and ITS lock, one allocation shared
// by every WithAttrs/WithGroup derivative. The first version copied the map
// pointer into derivatives that each carried their own zero mutex — two locks
// guarding one map, which -race caught the first time mochi logged through a
// derived logger while another goroutine logged through the base.
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

package door

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// LogPublisher is an slog.Handler that also writes each record to the tree's
// `logs` stream, so a Go service appears in the editor's log view.
//
// Every Colca service is meant to be visible there. The Python services get
// this from colca_data_contracts (over MQTT) or from the api's own handler
// (over this same door); the Go services had no path at all, so the node's
// historian, its grant convergence and its notification delivery were absent
// from the log the product shows -- a four-hour window of the deployed demo
// contained 1000 records, every one of them from dataops.
//
// It WRAPS another handler rather than replacing it: the service's existing
// console output is untouched, and publishing is added to it. A record that
// cannot be published is still on stdout.
//
// The three ways a log publisher takes a service down, and what is done here:
//
//   - Blocking: Handle never waits on the network. Records go to a buffered
//     channel that one goroutine drains.
//   - Unbounded growth: that channel is capped and drops the OLDEST record
//     when full, because during an outage the newest lines are the ones
//     describing it.
//   - Recursion: publishing makes an HTTP call, and the node serving it logs.
//     Cross-process that is not a loop, but a service publishing to its OWN
//     door would feed itself. Such a caller passes a MinLevel above what its
//     serving path logs at (see colcad's note in the design), and the
//     publisher never logs through slog itself -- a publish that fails is
//     dropped, silently, on purpose.
type LogPublisher struct {
	inner    slog.Handler
	client   *Client
	minLevel slog.Level
	records  chan logRecord
	name     string // the service name, from a With("service", ...) attr
	node     string
	position []string
	failures *failureNotice
	nodeOnce sync.Once
	started  sync.Once
}

type logRecord struct {
	logger  string
	level   string
	payload map[string]any
}

// LogPublisherOptions configures NewLogPublisher.
type LogPublisherOptions struct {
	// MinLevel is the floor for PUBLISHING. The wrapped handler keeps its own
	// level, so raising this narrows what reaches the tree without changing
	// what reaches the console.
	MinLevel slog.Level
	// Capacity is how many records may wait to be published. A buffer for a
	// hiccup, not a store -- the stream is the store.
	Capacity int
}

// DefaultLogCapacity mirrors the api's publisher, for the same reason.
const DefaultLogCapacity = 512

// failureNoticeInterval is how often a publisher that cannot reach the node
// may say so -- on stderr, never through slog. Reporting it through slog would
// hand the failure straight back to this handler, and one unreachable node
// would become a storm. On an interval because the failure is a STATE, the
// same reasoning internal/repl/linkstate.go applies to a replication lane.
// Silence would be worse: the log view would simply be missing lines with
// nothing anywhere saying why.
const failureNoticeInterval = 5 * time.Minute

// NewLogPublisher wraps inner so that records at or above MinLevel are also
// published to client's node as `_Log`.
func NewLogPublisher(inner slog.Handler, client *Client, options LogPublisherOptions) *LogPublisher {
	capacity := options.Capacity
	if capacity <= 0 {
		capacity = DefaultLogCapacity
	}
	return &LogPublisher{
		inner:    inner,
		client:   client,
		minLevel: options.MinLevel,
		records:  make(chan logRecord, capacity),
		failures: &failureNotice{},
	}
}

// Enabled defers to the wrapped handler: publishing must never SILENCE a line
// the service would otherwise have printed.
func (p *LogPublisher) Enabled(ctx context.Context, level slog.Level) bool {
	return p.inner.Enabled(ctx, level)
}

func (p *LogPublisher) Handle(ctx context.Context, record slog.Record) error {
	err := p.inner.Handle(ctx, record)
	if record.Level >= p.minLevel {
		p.offer(record)
	}
	return err
}

// WithAttrs remembers a `service` (or `logger`) attr as this handler's name.
//
// slog puts the attrs from `log.With("service", "colca-historian")` on the
// HANDLER, not on each record, so reading them off the record at Handle time
// finds nothing -- which is how every published line was first attributed to
// a generic "colca" rather than to the service that wrote it.
func (p *LogPublisher) WithAttrs(attrs []slog.Attr) slog.Handler {
	derived := p.derive(p.inner.WithAttrs(attrs))
	for _, attr := range attrs {
		if attr.Key == "service" || attr.Key == "logger" {
			derived.name = topicSegment(attr.Value.String())
		}
	}
	return derived
}

func (p *LogPublisher) WithGroup(name string) slog.Handler {
	return p.derive(p.inner.WithGroup(name))
}

// derive keeps ONE queue and one worker across every handler slog clones off
// this one. A per-clone queue would mean a per-clone goroutine, and a service
// that calls With() per request would grow one publisher per request.
func (p *LogPublisher) derive(inner slog.Handler) *LogPublisher {
	return &LogPublisher{
		inner:    inner,
		client:   p.client,
		minLevel: p.minLevel,
		records:  p.records,
		name:     p.name,
		node:     p.node,
		position: p.position,
		failures: p.failures,
		// nodeOnce/started are per-clone but guard shared state that is
		// idempotent to set; the worker is started from the ROOT publisher
		// below, which is the one Start() is called on.
	}
}

func (p *LogPublisher) offer(record slog.Record) {
	payload := map[string]any{
		"timestamp":   record.Time.UTC().Format(time.RFC3339Nano),
		"level":       record.Level.String(),
		"message":     record.Message,
		"logger_name": p.loggerName(record),
	}
	attrs := map[string]any{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.String()
		return true
	})
	if len(attrs) > 0 {
		payload["extra"] = attrs
	}
	item := logRecord{
		logger:  p.loggerName(record),
		level:   levelSegment(record.Level),
		payload: payload,
	}
	select {
	case p.records <- item:
	default:
		// Full: drop the oldest, then take this one. Both operations are
		// non-blocking, so a wedged publisher can never wedge a caller.
		select {
		case <-p.records:
		default:
		}
		select {
		case p.records <- item:
		default:
		}
	}
}

// Start begins draining the queue. It is separate from construction so a
// service can install the handler before it has a context to cancel with, and
// so a test can drive Drain deterministically instead.
func (p *LogPublisher) Start(ctx context.Context) {
	p.started.Do(func() {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case item := <-p.records:
					p.publish(ctx, item)
				}
			}
		}()
	})
}

func (p *LogPublisher) publish(ctx context.Context, item logRecord) {
	node := p.resolveNode(ctx)
	if node == "" {
		return
	}
	// The record is addressed at THIS SERVICE'S position -- its mount, then
	// its name -- because a service may write its own subtree and nothing
	// above it. Addressed at the node root instead, colcad refuses it with
	// `no write scope covers colca/v1/_Log/...`, which is what silenced every
	// placed service. The slog logger's own name stays in the payload.
	segments := append([]string{"colca", "v1", "_Log", node}, p.position...)
	segments = append(segments, item.level)
	topic := strings.Join(segments, "/")
	if err := p.client.Publish(ctx, topic, item.payload); err != nil {
		p.failures.note(err)
	}
}

// failureNotice counts records the node never received and reports the count
// on stderr at most once per failureNoticeInterval.
type failureNotice struct {
	mu      sync.Mutex
	dropped int
	last    time.Time
}

func (f *failureNotice) note(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropped++
	now := time.Now()
	if !f.last.IsZero() && now.Sub(f.last) < failureNoticeInterval {
		return
	}
	f.last = now
	fmt.Fprintf(os.Stderr,
		"[colca-log-publisher] %d log record(s) not published to the node: %v\n",
		f.dropped, err)
}

// resolveNode asks the door which node this is (topic level 4) and where this
// service sits (its mount, then its name -- the rule
// colca_data_contracts.service_topics.service_context states). Both come from
// one /self call, and neither can change without the process restarting.
func (p *LogPublisher) resolveNode(ctx context.Context) string {
	p.nodeOnce.Do(func() {
		self, err := p.client.Self(ctx)
		if err != nil {
			return
		}
		p.node = self.Node
		for _, segment := range strings.Split(self.Mount, "/") {
			if segment != "" {
				p.position = append(p.position, segment)
			}
		}
		name := self.Name
		if name == "" {
			name = p.name
		}
		p.position = append(p.position, topicSegment(name))
	})
	return p.node
}

// loggerName is the `service` attribute, so a reader can tell
// colca-historian's lines from colca-grantsync's. slog has no logger names of
// its own, and the view shows this segment as the source. A per-record attr
// wins over the handler's, so one call can attribute itself differently.
func (p *LogPublisher) loggerName(record slog.Record) string {
	name := ""
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "service" || attr.Key == "logger" {
			name = attr.Value.String()
			return false
		}
		return true
	})
	if name != "" {
		return topicSegment(name)
	}
	if p.name != "" {
		return p.name
	}
	return "colca"
}

// topicSegment keeps a name to ONE topic segment: the reader takes the
// second-to-last segment as the logger and the last as the level, so a slash
// inside either would shift both by one.
func topicSegment(name string) string {
	return strings.ReplaceAll(name, "/", ".")
}

// levelSegment maps slog's levels onto the five the topic grammar allows.
// `colca_data_contracts.logging.LOG_LEVELS` is the list, and the API refuses
// a record whose last segment is not in it -- so an unmapped level would not
// show up wrong, it would not show up at all.
func levelSegment(level slog.Level) string {
	switch {
	case level >= slog.LevelError:
		return "ERROR"
	case level >= slog.LevelWarn:
		return "WARNING"
	case level >= slog.LevelInfo:
		return "INFO"
	default:
		return "DEBUG"
	}
}

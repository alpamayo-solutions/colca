package door

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// LogSink receives finished records. A door Client is one sink; colcad has an
// in-process sink because it cannot post to its own door as a service.
type LogSink interface {
	// LogPosition answers the node's ULID and where this publisher sits --
	// its mount, then its name. Asked once.
	LogPosition(ctx context.Context) (node string, position []string, err error)
	// PublishLog writes one record.
	PublishLog(ctx context.Context, topic string, payload map[string]any) error
}

// LogPosition satisfies LogSink over the door, answering both halves from
// /self.
func (c *Client) LogPosition(ctx context.Context) (string, []string, error) {
	self, err := c.Self(ctx)
	if err != nil {
		return "", nil, err
	}
	position := []string{}
	for _, segment := range strings.Split(self.Mount, "/") {
		if segment != "" {
			position = append(position, segment)
		}
	}
	if self.Name != "" {
		position = append(position, topicSegment(self.Name))
	}
	return self.Node, position, nil
}

// PublishLog satisfies LogSink over the node's publish door.
func (c *Client) PublishLog(ctx context.Context, topic string, payload map[string]any) error {
	return c.Publish(ctx, topic, payload)
}

// LogPublisher is an slog.Handler that also writes each record to the tree's
// logs stream, so Go services show up in the log view. It wraps another
// handler: console output is unchanged, and a record that cannot be published
// still reaches it.
//
// It is built so it cannot take a service down:
//
//   - Handle never waits on the network; one goroutine drains a buffered channel.
//   - The channel is capped and drops the oldest record when full, because
//     during an outage the newest lines describe it.
//   - A service publishing to its own door would log about its own publishes.
//     Such a caller sets MinLevel above what its serving path logs, and the
//     publisher never logs through slog itself.
type LogPublisher struct {
	inner    slog.Handler
	sink     LogSink
	minLevel slog.Level
	records  chan logRecord
	skip     func(slog.Record) bool
	name     string // the service name, from a With("service", ...) attr
	node     string
	position []string
	failures *failureNotice
	nodeOnce sync.Once
	started  sync.Once

	// Stop uses cancel and done to end the goroutine Start launched. They are set
	// once in started.Do and read under mu.
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

type logRecord struct {
	logger  string
	level   string
	payload map[string]any
}

// LogPublisherOptions configures NewLogPublisher.
type LogPublisherOptions struct {
	// MinLevel is the floor for publishing. The wrapped handler keeps its own
	// level, so the console is unaffected.
	MinLevel slog.Level
	// Capacity is how many records may wait to be published. A buffer for a
	// hiccup, not a store -- the stream is the store.
	Capacity int
	// Skip refuses individual records. colcad needs it: it appends into its own
	// store, which logs every append, and the two would feed each other. Nil
	// accepts everything.
	Skip func(slog.Record) bool
}

// DefaultLogCapacity is the default queue capacity.
const DefaultLogCapacity = 512

// failureNoticeInterval is how often an unreachable node is reported on stderr,
// never through slog, which would feed the failure back into this handler.
const failureNoticeInterval = 5 * time.Minute

// NewLogPublisher wraps inner so that records at or above MinLevel are also
// published to the sink's node as `_Log`.
func NewLogPublisher(inner slog.Handler, sink LogSink, options LogPublisherOptions) *LogPublisher {
	capacity := options.Capacity
	if capacity <= 0 {
		capacity = DefaultLogCapacity
	}
	return &LogPublisher{
		inner:    usableBase(inner),
		sink:     sink,
		minLevel: options.MinLevel,
		records:  make(chan logRecord, capacity),
		skip:     options.Skip,
		failures: &failureNotice{},
	}
}

// usableBase replaces slog's built-in default handler with a stderr handler.
// That handler writes through the log package, which SetDefault routes back
// into slog, so wrapping it and installing the wrapper hangs on the first log
// call.
func usableBase(inner slog.Handler) slog.Handler {
	if inner == nil || reflect.TypeOf(inner).String() == builtinDefaultHandler {
		return slog.NewTextHandler(os.Stderr, nil)
	}
	return inner
}

// builtinDefaultHandler is the unexported type of slog's built-in default
// handler. If Go renames it, the test on usableBase catches it.
const builtinDefaultHandler = "*slog.defaultHandler"

// Enabled defers to the wrapped handler: publishing must never SILENCE a line
// the service would otherwise have printed.
func (p *LogPublisher) Enabled(ctx context.Context, level slog.Level) bool {
	return p.inner.Enabled(ctx, level)
}

// Handle passes the record to the wrapped handler and queues it for publishing.
func (p *LogPublisher) Handle(ctx context.Context, record slog.Record) error {
	err := p.inner.Handle(ctx, record)
	if record.Level >= p.minLevel && (p.skip == nil || !p.skip(record)) {
		p.offer(record)
	}
	return err
}

// WithAttrs remembers a service (or logger) attribute as this handler's name.
// slog keeps With attributes on the handler, not on each record.
func (p *LogPublisher) WithAttrs(attrs []slog.Attr) slog.Handler {
	derived := p.derive(p.inner.WithAttrs(attrs))
	for _, attr := range attrs {
		if attr.Key == "service" || attr.Key == "logger" {
			derived.name = topicSegment(attr.Value.String())
		}
	}
	return derived
}

// WithGroup returns a handler that shares this publisher's queue.
func (p *LogPublisher) WithGroup(name string) slog.Handler {
	return p.derive(p.inner.WithGroup(name))
}

// derive keeps ONE queue and one worker across every handler slog clones off
// this one. A per-clone queue would mean a per-clone goroutine, and a service
// that calls With() per request would grow one publisher per request.
func (p *LogPublisher) derive(inner slog.Handler) *LogPublisher {
	return &LogPublisher{
		inner:    inner,
		sink:     p.sink,
		minLevel: p.minLevel,
		records:  p.records,
		skip:     p.skip,
		name:     p.name,
		node:     p.node,
		position: p.position,
		failures: p.failures,
		// nodeOnce and started are per clone, but only the root publisher is started.
	}
}

func (p *LogPublisher) offer(record slog.Record) {
	module, function, line := source(record)
	// Every field is required by the _Log contract; vectors/log_payload.json in the
	// data contracts pins the list.
	payload := map[string]any{
		"timestamp":   record.Time.UTC().Format(time.RFC3339Nano),
		"level":       levelSegment(record.Level),
		"message":     record.Message,
		"logger_name": p.loggerName(record),
		"module":      module,
		"function":    function,
		"line_no":     line,
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
		ctx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		p.mu.Lock()
		p.cancel, p.done = cancel, done
		p.mu.Unlock()
		go func() {
			defer close(done)
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

// Stop ends the drain and waits for it, so nothing publishes after Stop
// returns. The drain writes through the sink into a store, and cancelling alone
// could leave a write in flight when the owner closes that store. Stop is
// idempotent and a no-op on a publisher that was never started.
func (p *LogPublisher) Stop() {
	p.mu.Lock()
	cancel, done := p.cancel, p.done
	p.mu.Unlock()
	if cancel == nil {
		return // never started: there is no drain to wait for
	}
	cancel()
	<-done
}

func (p *LogPublisher) publish(ctx context.Context, item logRecord) {
	node := p.resolveNode(ctx)
	if node == "" {
		return
	}
	// The record is addressed at this service's position, its mount and then its
	// name, because a service may only write its own subtree.
	segments := append([]string{uns.Root(), uns.Version, "_Log", node}, p.position...)
	segments = append(segments, item.level)
	topic := strings.Join(segments, "/")
	if err := p.sink.PublishLog(ctx, topic, item.payload); err != nil {
		p.failures.note(fmt.Errorf("%s (%q): %w", topic, item.payload["message"], err))
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

// resolveNode asks the door for the node's ULID and this service's position,
// once; neither changes while the process runs.
func (p *LogPublisher) resolveNode(ctx context.Context) string {
	p.nodeOnce.Do(func() {
		node, position, err := p.sink.LogPosition(ctx)
		if err != nil {
			return
		}
		p.node = node
		p.position = position
		if len(p.position) == 0 && p.name != "" {
			p.position = []string{p.name}
		}
	})
	return p.node
}

// loggerName is the service attribute, which the log view shows as the source.
// A per-record attribute wins over the handler's.
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

// source returns the file, function and line a record was written at, from its
// program counter. The _Log contract requires all three.
func source(record slog.Record) (module, function string, line int) {
	if record.PC == 0 {
		// No program counter: a hand-built record, or one bridged from the log package
		// without a location flag. The contract needs a function name, so name the gap
		// rather than have the node refuse the record.
		return "colca", "unknown", 0
	}
	frame, _ := runtime.CallersFrames([]uintptr{record.PC}).Next()
	module = strings.TrimSuffix(filepath.Base(frame.File), ".go")
	function = frame.Function
	if index := strings.LastIndex(function, "."); index >= 0 {
		function = function[index+1:]
	}
	return module, function, frame.Line
}

// topicSegment keeps a name to ONE topic segment: the reader takes the
// second-to-last segment as the logger and the last as the level, so a slash
// inside either would shift both by one.
func topicSegment(name string) string {
	return strings.ReplaceAll(name, "/", ".")
}

// levelSegment maps slog levels onto the five the topic grammar allows; a
// record with any other level would be refused.
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

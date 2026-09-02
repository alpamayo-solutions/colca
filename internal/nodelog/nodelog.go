// Package nodelog lets colcad publish its own log into the tree it owns.
//
// Every other service reaches the `logs` stream through the node's door. The
// node cannot: the door authenticates a caller by service name, and colcad is
// not one of its own services -- it IS the node. Nor should it acquire one.
// "Everything is a node" says a participant binds to an element and writes its
// own subtree; colcad is not a participant in its own registry, and inventing
// an entry for it would put a second identity on a position that already has
// one.
//
// So the last step is an in-process append instead of an HTTP call, and
// everything before it -- the record, the topic, the queue, the bound, the
// drop policy -- is the publisher every other Go service uses. The node's own
// voice is addressed like an unplaced service's: level 4 is its ULID, and its
// one position segment is `colca`.
package nodelog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/alpamayo-solutions/colca/internal/engine"
)

// ServiceName is colcad's position segment, and the name the log view shows
// against its lines.
const ServiceName = "colca"

// Sink appends a finished log record straight to the store.
//
// It is created BEFORE the engine exists, because the logger has to be
// installed before anything worth logging happens -- a node's most valuable
// lines are the ones it writes while starting up, and those are exactly the
// ones a publisher built after the engine would miss. Until Attach is called
// it reports "not ready", and the publisher's queue holds what it cannot
// deliver yet (bounded, oldest dropped, like any other outage).
type Sink struct {
	mu     sync.RWMutex
	engine *engine.Engine
	node   string
}

// Attach hands the sink the engine and the node's identity. Safe to call once
// the engine is built; records queued before this point are delivered then.
func (s *Sink) Attach(e *engine.Engine, nodeULID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engine = e
	s.node = nodeULID
}

func (s *Sink) current() (*engine.Engine, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.engine, s.node
}

// LogPosition answers the node's own identity and its one position segment.
func (s *Sink) LogPosition(_ context.Context) (string, []string, error) {
	e, node := s.current()
	if e == nil || node == "" {
		return "", nil, fmt.Errorf("nodelog: the engine is not attached yet")
	}
	return node, []string{ServiceName}, nil
}

// PublishLog appends the record as the node itself.
//
// `IngestAdminAttributed` is the right door for it: this write has no scope
// to check, because the writer owns the store. Attribution says plainly who
// wrote it, so a reader of the audit trail sees the node and not "admin".
func (s *Sink) PublishLog(_ context.Context, topic string, payload map[string]any) error {
	e, _ := s.current()
	if e == nil {
		return fmt.Errorf("nodelog: the engine is not attached yet")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = e.IngestAdminAttributed(topic, body, engine.Attribution{
		WrittenBy:  ServiceName,
		ActorID:    ServiceName,
		ActorLabel: ServiceName,
		ActorKind:  "system",
	})
	return err
}

// SkipsItsOwnPublishing reports whether a record is ABOUT publishing a log,
// and so must never itself be published.
//
// The engine logs every append at debug with the topic it appended
// (`atomic event ingest`). Publishing those would close a cycle: one log
// record appended produces one log record about the append, forever. The
// queue is bounded and non-blocking so this could never deadlock or grow, but
// on a node started with LOG_LEVEL=debug it would be a hot loop burning a core
// to say nothing.
//
// Cutting it here, at the one record that closes the cycle, is narrower than
// muting the engine or refusing debug outright: everything else the node logs
// at debug still reaches the tree.
func SkipsItsOwnPublishing(record slog.Record) bool {
	skip := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "topic" && strings.Contains(attr.Value.String(), "/_Log/") {
			skip = true
			return false
		}
		return true
	})
	return skip
}

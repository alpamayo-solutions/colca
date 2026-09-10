// Package nodelog lets colcad publish its own log into its tree. Services reach
// the logs stream through the node's door, but colcad is the node, not one of
// its services, so the last step is an in-process append. Everything else is
// the publisher every Go service uses; the node's records carry its ULID at
// level 4 and one position segment, colca.
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

// Sink appends a finished log record straight to the store. It exists before
// the engine so startup lines are captured; until Attach it reports not ready
// and the publisher's queue holds the records.
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

// PublishLog appends the record as the node itself, attributed to the node
// rather than to "admin".
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

// SkipsItsOwnPublishing reports whether a record is about publishing a log. The
// engine logs every append at debug; publishing those would log again for each
// append, a loop on a node running at debug.
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

// Package engine is the single place every write converges: the MQTT hook, the
// HTTP publish endpoint, replication apply and downlink apply all go through
// one of the Ingest* methods. Grammar, identity rule, mount rewrite, payload
// validation and the atomic persist live here and nowhere else.
package engine

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// LocalDeliver lets the engine hand downlink commands to the local MQTT broker (nil in unit tests).
type LocalDeliver func(topic string, payload []byte)

// Result describes what an ingest did. Topic is the post-rewrite topic, i.e.
// exactly what was persisted.
type Result struct {
	Persisted bool
	Stream    string
	Offset    uint64
	Topic     string // post-rewrite
}

type Engine struct {
	store   *store.Store
	cfg     *config.Config
	deliver LocalDeliver
	mounts  map[string]string // identity ulid → mount (clients + children)
	log     *slog.Logger
}

// New builds an engine. The mount map covers both children and clients: they
// share one mount namespace (config.Validate guarantees no collisions).
func New(s *store.Store, cfg *config.Config, deliver LocalDeliver) *Engine {
	m := map[string]string{}
	for _, c := range cfg.Clients {
		m[c.ULID] = c.Mount
	}
	for _, c := range cfg.Children {
		m[c.ULID] = c.Mount
	}
	return &Engine{store: s, cfg: cfg, deliver: deliver, mounts: m, log: slog.Default().With("node", cfg.ULID)}
}

func (e *Engine) Store() *store.Store { return e.store }

// MountOf resolves the mount a child or client is attached under.
func (e *Engine) MountOf(ulid string) (string, bool) {
	m, ok := e.mounts[ulid]
	return m, ok
}

// IngestClient: a directly attached MQTT client (machine/service) publishes.
// Rules: uns grammar, class must be data/entity/ack, level-4 == identity, mount
// rewrite, validate, persist. A non-UNS topic is not an error — it is normal
// broker traffic that simply is not persisted.
func (e *Engine) IngestClient(identity, topic string, payload []byte) (Result, error) {
	if !uns.IsUns(topic) {
		return Result{Persisted: false}, nil // normal broker behavior outside colca/#
	}
	p, err := uns.Parse(topic)
	if err != nil {
		return Result{}, err
	}
	class := uns.ClassOf(p.Contract)
	if class == uns.ClassCmd || class == uns.ClassNone {
		return Result{}, fmt.Errorf("client %s may not publish %s", identity, p.Contract)
	}
	if p.NodeID != identity {
		return Result{}, fmt.Errorf("identity rule: level-4 %q != authenticated identity %q", p.NodeID, identity)
	}
	if err := uns.Validate(p.Contract, payload); err != nil {
		return Result{}, err
	}
	mount, ok := e.mounts[identity]
	if !ok {
		return Result{}, fmt.Errorf("no mount registered for %s", identity)
	}
	rewritten := uns.MountInsert(topic, mount)
	rp, err := uns.Parse(rewritten)
	if err != nil {
		return Result{}, err
	}
	return e.persist(class, rp, rewritten, payload)
}

// IngestAdmin: local HTTP API with admin token — publishes in node-local
// coordinates, no rewrite, commands allowed, still validated.
func (e *Engine) IngestAdmin(topic string, payload []byte) (Result, error) {
	if !uns.IsUns(topic) {
		return Result{}, fmt.Errorf("admin publish must be colca/#")
	}
	p, err := uns.Parse(topic)
	if err != nil {
		return Result{}, err
	}
	class := uns.ClassOf(p.Contract)
	if class == uns.ClassNone {
		return Result{}, fmt.Errorf("unknown contract %s", p.Contract)
	}
	if err := uns.Validate(p.Contract, payload); err != nil {
		return Result{}, err
	}
	return e.persist(class, p, topic, payload)
}

// IngestDownlink: a command received from the parent (already mount-stripped to
// local coords). Trusted (parent authenticated), persisted to the own commands
// stream with the ORIGINAL parent timestamp — expiry must not be refreshed by a
// hop — and then delivered to the local MQTT broker.
func (e *Engine) IngestDownlink(topic string, payload []byte, ts int64) (Result, error) {
	p, err := uns.Parse(topic)
	if err != nil {
		return Result{}, err
	}
	res, err := e.persistTS(uns.ClassOf(p.Contract), p, topic, payload, ts)
	if err != nil {
		return res, err
	}
	if e.deliver != nil {
		e.deliver(topic, payload)
	}
	return res, nil
}

func (e *Engine) persist(class uns.Class, p uns.Parsed, topic string, payload []byte) (Result, error) {
	return e.persistTS(class, p, topic, payload, time.Now().UnixMilli())
}

// persistTS writes the record (plus, for data/entity, its KV projection) in one
// atomic batch. p must be the parse of topic, i.e. post-rewrite, so KVPath and
// KVNode carry the local coordinates and the originating node id.
func (e *Engine) persistTS(class uns.Class, p uns.Parsed, topic string, payload []byte, ts int64) (Result, error) {
	streamName := uns.StreamFor(class)
	rec := store.Record{Topic: topic, Payload: payload, TS: ts}
	if class == uns.ClassData || class == uns.ClassEntity {
		rec.KVPath, rec.KVNode = p.Path, p.NodeID
	}
	first, _, err := e.store.Append(streamName, []store.Record{rec})
	if err != nil {
		return Result{}, err
	}
	e.log.Debug("ingest", "stream", streamName, "offset", first, "topic", topic)
	return Result{Persisted: true, Stream: streamName, Offset: first, Topic: topic}, nil
}

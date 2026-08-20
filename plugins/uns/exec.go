package uns

// EntityStore is the node's record surface as this plugin needs it: read the
// current state of an entity, list the entities of one contract belonging to
// one identity, write a record as the node.
//
// It is declared HERE, in stdlib types only, and satisfied by an adapter on the
// core side. Go interfaces are structural, so that adapter needs no import from
// this package and this package needs none from the core — which is what keeps
// `go list -deps` on this package at "standard library only" (arch_test.go)
// while still letting domain logic touch storage.
type EntityStore interface {
	// KVGet returns the current payload stored at a topic, if any.
	KVGet(topic string) ([]byte, bool)
	// KVScan returns the current records of one contract published under one
	// identity — the level-4 ULID, not a path, so the answer is the same at
	// every level of the tree.
	KVScan(contract, nodeID string) []KVRecord
	// KVScanAll returns the current records of one contract whoever published
	// them, each at the path THIS node holds it under. The element index needs
	// this: a child's elements arrive here mount-inserted, already in this
	// node's frame, and they are as much a position here as the node's own.
	KVScanAll(contract string) []KVRecord
	// Publish writes a record as this node, in the node's own frame, and returns
	// the durable stream position produced by the write. Command acknowledgements
	// carry that position so an API can wait for its exact projected state.
	Publish(topic string, payload []byte) (StateWrite, error)
	// PublishBatch validates and commits a complete command result as one
	// atomic state transition. Either every record receives a durable stream
	// position and becomes current KV state, or none of them do.
	PublishBatch(records []StateRecord) ([]StateWrite, error)
	// NodeID is the identity this node publishes under.
	NodeID() string
}

// StateRecord is one desired state mutation produced by a domain command.
// An empty payload is the contract's tombstone when that contract permits it.
// It deliberately carries no stream, offset or owner choice: the engine
// validates the topic and derives those authoritative values at commit time.
type StateRecord struct {
	Topic   string
	Payload []byte
}

// StateWrite identifies one state record produced while executing a command.
// It is deliberately a plugin type so the domain executor remains independent
// of the engine package that implements the store adapter.
type StateWrite struct {
	Stream string `json:"stream"`
	Offset uint64 `json:"offset"`
	Topic  string `json:"topic"`
}

// EntryRef is the part of a registry entry the domain needs to compute where
// it publishes: its identity, its name, and its element. Returned by
// Bindings.Entries rather than *uns.Entry to keep the port narrow, the same
// reason EntryOf returns two fields instead of the whole entry.
type EntryRef struct{ ULID, Name, Element string }

// Bindings answers which identities bind to an element, and who one identity
// is. Declared here for the same reason as EntityStore and satisfied the same
// way — the core's registry manager fits it structurally, without either side
// importing the other.
type Bindings interface {
	// BoundTo lists the ULIDs of the identities bound to an element.
	BoundTo(elementID string) []string
	// EntryOf answers who an identity is and where it is bound: its name and
	// its element ("" for unplaced — bound to the node itself). ok is false
	// when this node has never enrolled that identity. Autobind needs both to
	// COMPUTE where that identity's catalogue sits, rather than searching for
	// records that look like they might be its (local-service-trust design
	// §6). Returning the two fields rather than *uns.Entry keeps this port
	// narrow and stops the domain depending on the entry's whole shape.
	EntryOf(ulid string) (name, element string, ok bool)
	// Entries lists the identities enrolled at this node, so the domain can
	// ask which of them, if any, a record's arrival position belongs to — the
	// lifecycle trigger's version of the same computation autobind runs
	// forward. The registry only lists; it has no notion of what a catalogue
	// topic looks like — that knowledge stays in this package (design §4/§6).
	// The list is the local registry, a handful of identities, so a scan over
	// it costs nothing; the reverted design's mistake was scanning RECORDS,
	// not identities.
	Entries() []EntryRef
}

// KVRecord is one entity record as the store currently holds it.
type KVRecord struct {
	// Topic is the full stored topic, so Parse gives back contract, identity
	// and the record's position.
	Topic string
	// Path is the record's position in this node's frame.
	Path    string
	NodeID  string
	Payload []byte
	// Offset is the entity stream version that produced this current state.
	// It is local to the node and remains the coordinate for retention/CAS.
	Offset uint64
	// OriginOffset is the owner node's entity-stream coordinate. Replication
	// keeps it unchanged across hops, so an ancestor's Edit projection and
	// the owner executing a command compare the same version.
	OriginOffset uint64
}

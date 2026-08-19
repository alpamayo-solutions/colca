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
	// Publish writes a record as this node, in the node's own frame.
	Publish(topic string, payload []byte) error
	// NodeID is the identity this node publishes under.
	NodeID() string
}

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
}

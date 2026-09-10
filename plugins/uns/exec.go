package uns

// EntityStore is the node's record store as this plugin needs it: read an
// entity, list a contract's records for one identity, write as the node. It is
// declared here in stdlib types and implemented by a core adapter, which keeps
// this package free of non-stdlib imports.
type EntityStore interface {
	// KVGet returns the current payload stored at a topic, if any.
	KVGet(topic string) ([]byte, bool)
	// KVScan returns the current records of one contract published under one
	// node id, which is the same at every level of the tree.
	KVScan(contract, nodeID string) []KVRecord
	// KVScanAll returns the current records of one contract from any publisher,
	// at the path this node holds each under. Children's elements arrive
	// mount-inserted and count as positions here too.
	KVScanAll(contract string) []KVRecord
	// PublishBatch validates and commits a command's result as one atomic
	// transition in the node's frame: every record gets a stream position and
	// becomes KV state, or none does. The returned positions go into the ack.
	// It is the only way an executor writes state, and it refuses anything that
	// is not command-authored state; annotations go through PublishEvent.
	PublishBatch(records []StateRecord) ([]StateWrite, error)
	// PublishEvent commits one event record authored by a command executor, for
	// classes that are never KV-projected (uns.IsCommandAuthoredEvent). An event
	// has no current value to compare, so there is nothing to batch it with.
	PublishEvent(record StateRecord) (StateWrite, error)
	// NodeID is the identity this node publishes under.
	NodeID() string
}

// Blobs is the domain's view of this node's file store. A _Resource is never
// authored pointing at bytes the node lacks: Has checks, Pull fetches them from
// the parent. Declared in stdlib types like EntityStore.
type Blobs interface {
	// Has reports whether this node already holds the blob with this digest.
	Has(sha string) bool
	// Pull fetches the blob from this node's parent and stores it, verifying
	// the digest before it lands. It errors when this node has no parent, when
	// no ancestor holds the blob, or when the transfer fails.
	Pull(sha string) error
}

// StateRecord is one state change a command produces. An empty payload is the
// contract's tombstone where allowed. The engine derives stream, offset and
// owner at commit time.
type StateRecord struct {
	Topic   string
	Payload []byte
}

// StateWrite identifies one state record written while executing a command.
type StateWrite struct {
	Stream string `json:"stream"`
	Offset uint64 `json:"offset"`
	Topic  string `json:"topic"`
}

// CommandContext is who is acting when a command executes. The engine fills it
// from the entry it authorized at the door, or, for a command replicated from
// an ancestor, from the group ids recorded with it, resolved against this
// node's _Group definitions. Actor is nil only for the admin door. Executors
// authorize against Actor, never against whoever carried the command.
type CommandContext struct {
	Actor *Entry
}

// EntryRef is the part of a registry entry the domain needs to compute where
// an identity publishes: its ULID, name and element.
type EntryRef struct{ ULID, Name, Element string }

// Bindings says which identities bind to an element and who an identity is.
// The core's registry manager satisfies it without either side importing the
// other.
type Bindings interface {
	// BoundTo lists the ULIDs of the identities bound to an element.
	BoundTo(elementID string) []string
	// EntryOf returns an identity's name and element ("" for unplaced); ok is
	// false if this node never enrolled it. Autobind uses both to compute
	// where the identity's catalogue is.
	EntryOf(ulid string) (name, element string, ok bool)
	// Entries lists the identities enrolled at this node, so the lifecycle
	// trigger can tell which one a record's topic belongs to. The registry
	// only lists; what a catalogue topic looks like is known here.
	Entries() []EntryRef
}

// occupantsOf lists the identities standing on an element. Retiring such an
// element would leave them able to authenticate but unable to write, so both
// element/delete and the Edit delete intent ask here. No registry or no id
// means an empty list.
func occupantsOf(bound Bindings, elementID string) []string {
	if bound == nil || elementID == "" {
		return nil
	}
	return bound.BoundTo(elementID)
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
	// OriginOffset is the owner node's entity-stream offset. Replication keeps
	// it across hops, so an ancestor and the owner compare the same version.
	OriginOffset uint64
}

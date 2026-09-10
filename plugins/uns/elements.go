package uns

import (
	"encoding/json"
	"strings"
	"sync"
)

// ElementIndex answers which local path an element is at. It is a projection of
// the _SystemElement records the node holds, whose topics are the positions, so
// a rename is correct as soon as the record changes. It includes children's
// elements, which arrive already mount-inserted into this node's frame; elements
// above the node come from its ancestry instead. It is a map, not a scan,
// because every publish needs its writer's mount resolved.
type ElementIndex struct {
	store EntityStore

	mu     sync.RWMutex
	loaded bool
	byID   map[string]string // element id → local path
	byPath map[string]string // local path → element id
}

var _ Namespace = (*ElementIndex)(nil)

// NewElementIndex returns an index over the elements in s. Observe keeps it current.
func NewElementIndex(s EntityStore) *ElementIndex {
	return &ElementIndex{store: s, byID: map[string]string{}, byPath: map[string]string{}}
}

// Observe keeps the map current as element records arrive and ignores
// everything else.
func (x *ElementIndex) Observe(contract, topic string, payload []byte) {
	if contract != "_SystemElement" {
		return
	}
	p, err := Parse(topic)
	if err != nil {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.loaded {
		return // the first read loads everything anyway
	}
	x.apply(p.Path, payload)
}

// apply folds one record into the map. The caller holds the write lock.
func (x *ElementIndex) apply(path string, payload []byte) {
	if old, ok := x.byPath[path]; ok {
		// The position changed hands or was retired; keeping the old id would
		// resolve a stale element to a live path.
		delete(x.byID, old)
		delete(x.byPath, path)
	}
	if len(payload) == 0 {
		return // tombstone: the position is retired
	}
	var e placedElement
	if json.Unmarshal(payload, &e) != nil || e.ID == "" {
		return
	}
	x.byID[e.ID] = path
	x.byPath[path] = e.ID
}

// load fills the map from the store on first use, so the index needs no place
// in the startup order.
func (x *ElementIndex) load() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.loaded {
		return
	}
	for _, rec := range x.store.KVScanAll("_SystemElement") {
		x.apply(rec.Path, rec.Payload)
	}
	x.loaded = true
}

// PathOf returns the element's local path, or false if this node does not hold
// it. Callers must fail closed on false.
func (x *ElementIndex) PathOf(elementID string) (string, bool) {
	if elementID == "" {
		return "", false
	}
	x.load()
	x.mu.RLock()
	defer x.mu.RUnlock()
	path, ok := x.byID[elementID]
	return path, ok
}

// IDAt returns which element sits at a local path.
func (x *ElementIndex) IDAt(path string) (string, bool) {
	x.load()
	x.mu.RLock()
	defer x.mu.RUnlock()
	id, ok := x.byPath[path]
	return id, ok
}

// Covers reports whether topicPath lies at or below the element's position.
// The boundary is the path separator, so `line10` is not under `line1`.
func (x *ElementIndex) Covers(elementID, topicPath string) bool {
	base, ok := x.PathOf(elementID)
	if !ok {
		return false
	}
	return topicPath == base || strings.HasPrefix(topicPath, base+"/")
}

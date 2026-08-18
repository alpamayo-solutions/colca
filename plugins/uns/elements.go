package uns

import (
	"encoding/json"
	"strings"
	"sync"
)

// ElementIndex answers the one question the namespace is asked: which of this
// node's local paths is that element?
//
// The answer lives in the `_SystemElement` records the node already holds, whose
// topics ARE the positions — so this index is a projection of them, never a
// second source of truth. A rename is correct as soon as the record changes,
// with nothing to invalidate by hand and nothing that can go stale.
//
// It indexes EVERY element record the node holds, not only the ones it
// published itself. A child's elements replicate upward with the mount inserted
// at each hop, so at an ancestor they already carry that ancestor's own local
// path — which is exactly the answer a grant naming a deep element needs there.
// The reverse never happens: records do not flow downward, so a node cannot
// resolve an element ABOVE it, and that half is what its ancestry carries.
//
// It is a maintained map rather than a scan because resolution sits on the
// ingest path: every record a client publishes needs its writer's mount, and a
// mount is an element (id-grants design §4).
type ElementIndex struct {
	store EntityStore

	mu     sync.RWMutex
	loaded bool
	byID   map[string]string // element id → local path
	byPath map[string]string // local path → element id
}

var _ Namespace = (*ElementIndex)(nil)

func NewElementIndex(s EntityStore) *ElementIndex {
	return &ElementIndex{store: s, byID: map[string]string{}, byPath: map[string]string{}}
}

// Observe keeps the map current as element records arrive. Anything that is not
// an element is ignored, so this can be wired to the same hook everything else
// uses.
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
		// The position changed hands, or was retired: the id that used to sit
		// here no longer does, and leaving it mapped would resolve a stale
		// element to a live path.
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

// load builds the map from the store on first use, so the index needs no place
// in the startup order — it fills itself the first time anything asks.
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
// it. Callers must fail closed on false: an element a node has never heard of
// grants nothing and mounts nowhere, and saying so beats guessing a path.
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

// IDAt is the reverse: which element sits at this local path. Used where a path
// is what one has — a record arriving at a door — and an identity is what one
// needs.
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

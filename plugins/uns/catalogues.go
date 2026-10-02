package uns

import (
	"encoding/json"
	"sort"
	"sync"
)

// CatalogueTopic is the _DataTags topic an identity bound at mount publishes
// its catalogue on: this node, the mount, then the identity's catalogue name
// (Entry.CatalogueName). Autobind finds a catalogue's owner by it, and the
// metric door finds a signal's producer by it.
func CatalogueTopic(nodeID, mount, name string) string {
	return Prefix() + "_DataTags/" + nodeID + "/" + joinPath(mount, name)
}

// CatalogueIndex answers which of this node's connector catalogues hold a data
// tag. A signal names its binding by tag id only, and the service whose
// catalogue holds that tag produces the signal's metrics. It is a map, not a
// scan, because every _Metric publish asks it.
type CatalogueIndex struct {
	store EntityStore

	mu      sync.RWMutex
	loaded  bool
	tagsAt  map[string][]string        // catalogue topic → the tag ids it holds
	holders map[string]map[string]bool // tag id → the catalogue topics holding it
}

// NewCatalogueIndex returns an index over the catalogues in s. Observe keeps it
// current.
func NewCatalogueIndex(s EntityStore) *CatalogueIndex {
	return &CatalogueIndex{store: s, tagsAt: map[string][]string{}, holders: map[string]map[string]bool{}}
}

// Observe folds a _DataTags record this node just stored into the index and
// ignores everything else, including catalogues replicated from children:
// their signals' metrics are the child's to admit.
func (x *CatalogueIndex) Observe(contract, topic string, payload []byte) {
	if contract != "_DataTags" {
		return
	}
	p, err := Parse(topic)
	if err != nil || p.NodeID != x.store.NodeID() {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.loaded {
		return // the first read loads everything anyway
	}
	x.apply(topic, payload)
}

// apply replaces what the index knows about one catalogue. The caller holds
// the write lock.
func (x *CatalogueIndex) apply(topic string, payload []byte) {
	for _, tag := range x.tagsAt[topic] {
		delete(x.holders[tag], topic)
		if len(x.holders[tag]) == 0 {
			delete(x.holders, tag)
		}
	}
	delete(x.tagsAt, topic)
	if len(payload) == 0 {
		return // tombstone: the catalogue was retired
	}
	var cat struct {
		DataTags []struct {
			ID string `json:"id"`
		} `json:"data_tags"`
	}
	if json.Unmarshal(payload, &cat) != nil {
		return
	}
	tags := make([]string, 0, len(cat.DataTags))
	for _, tag := range cat.DataTags {
		if tag.ID == "" {
			continue
		}
		if x.holders[tag.ID] == nil {
			x.holders[tag.ID] = map[string]bool{}
		}
		x.holders[tag.ID][topic] = true
		tags = append(tags, tag.ID)
	}
	x.tagsAt[topic] = tags
}

// load fills the index from the store on first use, so it needs no place in
// the startup order. It holds the write lock while it scans, so an Observe
// racing it waits and is applied on top.
func (x *CatalogueIndex) load() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.loaded {
		return
	}
	for _, rec := range x.store.KVScan("_DataTags", x.store.NodeID()) {
		x.apply(rec.Topic, rec.Payload)
	}
	x.loaded = true
}

// Holders returns the topics of this node's catalogues that hold the tag,
// sorted. More than one means two services claim the same tag id.
func (x *CatalogueIndex) Holders(tagID string) []string {
	x.load()
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make([]string, 0, len(x.holders[tagID]))
	for topic := range x.holders[tagID] {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}

package uns

import (
	"encoding/json"
	"fmt"
	"sort"
)

// signalBindings is the tag-to-signal binding state a node holds and owns the
// rule every binding follows: a tag binds to at most one signal, a signal holds
// at most one tag, and an existing binding is never silently overwritten.
// signal/autobind skips what it may not bind; the edit refuses with a 409. The
// reaction differs, the rule does not.
type signalBindings struct {
	signalOf map[string]string // tag id → the signal that holds it
	tagOf    map[string]string // signal id → the tag it holds
}

// bindVerdict answers whether a tag may bind to a signal given the existing
// bindings. Every value after bindFree is a reason not to.
type bindVerdict int

const (
	bindFree       bindVerdict = iota // nothing stands in the way
	bindSatisfied                     // this exact binding already stands
	bindTagHeld                       // another signal already holds the tag
	bindSignalHeld                    // this signal already holds another tag
)

func newSignalBindings() *signalBindings {
	return &signalBindings{signalOf: map[string]string{}, tagOf: map[string]string{}}
}

// propose applies the rule. An empty signalID is a signal not created yet, so
// only the tag side can refuse.
func (b *signalBindings) propose(tagID, signalID string) bindVerdict {
	if holder := b.signalOf[tagID]; holder != "" {
		if holder == signalID {
			return bindSatisfied
		}
		return bindTagHeld
	}
	if signalID != "" && b.tagOf[signalID] != "" {
		return bindSignalHeld
	}
	return bindFree
}

// bind records that signalID holds tagID. Callers record every binding they
// compose, so a set composed in one pass follows the rule internally too.
func (b *signalBindings) bind(tagID, signalID string) {
	if tagID == "" || signalID == "" {
		return
	}
	if previous := b.tagOf[signalID]; previous != "" {
		delete(b.signalOf, previous)
	}
	b.signalOf[tagID] = signalID
	b.tagOf[signalID] = tagID
}

// unbind releases tagID and whichever signal held it.
func (b *signalBindings) unbind(tagID string) {
	if holder := b.signalOf[tagID]; holder != "" {
		delete(b.tagOf, holder)
	}
	delete(b.signalOf, tagID)
}

// signalHolding is the signal that holds tagID, "" if none does.
func (b *signalBindings) signalHolding(tagID string) string { return b.signalOf[tagID] }

// tagHeldBy is the tag signalID holds, "" if it holds none.
func (b *signalBindings) tagHeldBy(signalID string) string { return b.tagOf[signalID] }

// TagHeldError refuses a write that would bind a tag another signal already
// holds. It names both signals, so the caller can say which one is in the
// way. Executors answer it with a 409.
type TagHeldError struct {
	Tag        string // the tag the write binds
	Signal     string // the signal the write binds it to
	Holder     string // the signal that holds the tag
	HolderPath string // where the holder sits at this node
}

func (e *TagHeldError) Error() string {
	return fmt.Sprintf("tag %s is already bound to signal %s at %s — a tag binds to at most one signal",
		e.Tag, e.Holder, e.HolderPath)
}

// CheckTagBindings applies the rule to a batch about to commit at one node.
// current is the node's own _Signal records, batch the records the command
// writes. A record that binds its signal to a tag the signal did not hold
// before is refused when, once the batch is applied, another signal holds the
// same tag. A binding that already stands is left alone, including two
// signals on one tag that a node held before the rule was enforced: the
// command that resolves them has to be able to commit.
//
// The engine runs it under one lock with the commit, so two commands that
// bind one tag at the same time cannot both pass.
func CheckTagBindings(current []KVRecord, batch []StateRecord) error {
	type standing struct{ id, tag, path string }
	identity := func(s boundSignal, path string) string {
		if s.ID != "" {
			return s.ID
		}
		return path
	}
	held := map[string]string{}    // signal → the tag it held before the batch
	after := map[string]standing{} // topic → the signal standing there after it
	for _, rec := range current {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) != nil {
			continue
		}
		id := identity(s, rec.Path)
		after[rec.Topic] = standing{id: id, tag: s.DataTag, path: rec.Path}
		if s.DataTag != "" {
			held[id] = s.DataTag
		}
	}
	var fresh []standing
	for _, rec := range batch {
		parsed, err := Parse(rec.Topic)
		if err != nil || parsed.Contract != "_Signal" {
			continue
		}
		if len(rec.Payload) == 0 {
			delete(after, rec.Topic)
			continue
		}
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) != nil {
			continue // not a record: the schema check refuses it
		}
		id := identity(s, parsed.Path)
		after[rec.Topic] = standing{id: id, tag: s.DataTag, path: parsed.Path}
		if s.DataTag != "" && held[id] != s.DataTag {
			fresh = append(fresh, standing{id: id, tag: s.DataTag, path: parsed.Path})
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	topics := make([]string, 0, len(after))
	for topic := range after {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for _, bound := range fresh {
		for _, topic := range topics {
			other := after[topic]
			if other.tag == bound.tag && other.id != bound.id {
				return &TagHeldError{Tag: bound.tag, Signal: bound.id, Holder: other.id, HolderPath: other.path}
			}
		}
	}
	return nil
}

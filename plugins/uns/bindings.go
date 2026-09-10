package uns

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

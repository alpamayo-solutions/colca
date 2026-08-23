package uns

// signalBindings is the tag↔signal binding state a node holds, and the one
// owner of the invariant every binding operation obeys: a tag binds to at most
// one signal, a signal holds at most one tag, and a binding that already
// stands is never silently overwritten.
//
// Two operations consult it and react differently, deliberately.
// `signal/autobind` PROVISIONS a whole catalogue: it passes over what it may
// not bind, so a replay, a lifecycle trigger and a person all converge on the
// same model. The edit CURATES one edit: it refuses with a 409 so the
// person is told which binding is in the way. The reaction is each caller's
// own business — the RULE is not, or changing it in one place would leave the
// other with the old meaning and nothing would notice.
type signalBindings struct {
	signalOf map[string]string // tag id → the signal that holds it
	tagOf    map[string]string // signal id → the tag it holds
}

// bindVerdict answers "may this tag bind to this signal, given the bindings
// that already stand?". Everything past bindFree is a reason the invariant
// says no; what to do about it is the caller's.
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

// propose is the invariant itself. An empty signalID names a signal that does
// not exist yet — autobind mints one per unbound tag — so it holds nothing and
// only the tag side can refuse it.
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
// compose, not only the ones they read, so a set composed in one pass obeys
// the invariant within itself and not merely against what was already retained.
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

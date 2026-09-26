// Package cursorwatch tells a consumer that stopped reading from one that has
// nothing to read.
//
// A consumer is woken by an MQTT message and then drains its cursor. There is
// no timed catch-up behind that, so a lost wake or a stuck loop leaves records
// waiting with nobody reading them. The watchdog finds those cursors instead of
// hiding them behind a poll: every few seconds it takes each cursor's oldest
// unread record that the consumer actually reads, and when that record is older
// than the threshold it writes a retained _Finding (reason cursor_lag) about
// the service that owns the cursor. The alarm path raises it like any other
// finding, and the service itself can subscribe to it to fail its health
// check. The finding is retired once the cursor has caught up.
//
// "Actually reads" matters. A consumer that fetches with a filter (a signal
// list, contracts, topics) is only woken for records that pass it, so records
// it filters out wait forever by design. The door remembers each cursor's last
// fetch filter (Filters) and the watchdog applies it; until a cursor fetches,
// every record counts for the gauge. Only a cursor fetched since the node
// started can raise a finding: one nobody fetches is abandoned (a previous
// buffer generation, a renamed consumer), and every live consumer fetches when
// it drains on reconnect.
package cursorwatch

import (
	"sync"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// Filter keeps the records a consumer reads.
type Filter func(store.StoredRecord) bool

type key struct{ cursor, stream string }

type remembered struct {
	filter Filter
	gen    uint64
}

// Filters holds each cursor's last fetch filter. It is memory only: after a
// restart a consumer drains on reconnect, which fetches and fills it again.
type Filters struct {
	mu  sync.Mutex
	m   map[key]remembered
	gen uint64
}

// NewFilters returns an empty set.
func NewFilters() *Filters { return &Filters{m: map[key]remembered{}} }

// Remember records the filter of a fetch on cursor. nil means every record.
func (f *Filters) Remember(cursor, stream string, filter Filter) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen++
	f.m[key{cursor, stream}] = remembered{filter: filter, gen: f.gen}
}

// get returns the cursor's filter and a generation that changes whenever a new
// filter is remembered; gen 0 means the cursor never fetched since start.
func (f *Filters) get(cursor, stream string) (Filter, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.m[key{cursor, stream}]
	return r.filter, r.gen
}

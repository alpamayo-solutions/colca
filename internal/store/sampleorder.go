package store

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// A _Metric's KV entry, and with it the retained message, holds the topic's
// newest sample, not the last record to arrive. A repair, backfill or catch-up
// publishes older samples after newer ones; they join the stream (history,
// subscribers, the historian) but leave the current value alone. A sample with
// the same time as the current one replaces it, so a corrected value
// republished at the same timestamp still lands. The rule is the same at every
// node a record reaches, local or replicated, so a parent ends up with the
// child's current value whatever order the uplink delivers.
//
// A sample's time is its payload timestamp, or, without one, the time the node
// that first stored it ingested it: the time the historian files it under. Other
// state classes carry no sample time and keep last-arrival-wins.
//
// A sample time more than maxSampleLead ahead of the ingest time counts as
// ingest time + maxSampleLead: a device whose clock runs far ahead would
// otherwise hold the current value until real time reached its stamp. The
// payload keeps the timestamp it was published with.
//
// The KV entry keeps the sample time it holds (kvEnc.SampleAt), so the check
// reads one small value and never decodes the retained payload. An entry
// written before it was kept has none, and the next sample replaces it.

// sampleOrder decides, for one write batch, whether each _Metric moves its KV
// entry. Records earlier in the batch are not in the database yet, so what the
// batch wrote is looked up first. A nested order (child) stages a part of the
// batch that may still be dropped and is merged only once that part is kept.
type sampleOrder struct {
	db      pebble.Reader
	parent  *sampleOrder
	pending map[string]pendingSample
}

// pendingSample is what a batch did to a KV entry: set it to a sample at at
// (0: unknown), or deleted it.
type pendingSample struct {
	at      int64
	deleted bool
}

func newSampleOrder(db pebble.Reader) *sampleOrder { return &sampleOrder{db: db} }

// child stages writes that are merged into o only by commit.
func (o *sampleOrder) child() *sampleOrder { return &sampleOrder{db: o.db, parent: o} }

// commit merges a child's writes into its parent.
func (o *sampleOrder) commit() {
	if o.parent == nil {
		return
	}
	for k, v := range o.pending {
		o.parent.note(k, v)
	}
}

func (o *sampleOrder) note(key string, p pendingSample) {
	if o.pending == nil {
		o.pending = map[string]pendingSample{}
	}
	o.pending[key] = p
}

// held is the sample time of the KV entry at key, as this batch leaves it so
// far; ok is false when the entry is absent or its sample time unknown.
func (o *sampleOrder) held(key []byte) (at int64, ok bool) {
	for cur := o; cur != nil; cur = cur.parent {
		if p, found := cur.pending[string(key)]; found {
			return p.at, !p.deleted && p.at != 0
		}
	}
	v, closer, err := o.db.Get(key)
	if err != nil {
		return 0, false
	}
	defer closer.Close()
	var head struct {
		SampleAt int64 `json:"sa"`
	}
	if json.Unmarshal(v, &head) != nil {
		return 0, false
	}
	return head.SampleAt, head.SampleAt != 0
}

// admit reports whether a sample at at may replace the KV entry at key, and
// notes it as written when it may. at is 0 when the sample's time is unknown;
// such a sample replaces whatever is held, as every write did before.
func (o *sampleOrder) admit(key []byte, at int64) bool {
	if o == nil {
		return true
	}
	if at != 0 {
		if cur, ok := o.held(key); ok && at < cur {
			return false
		}
	}
	o.note(string(key), pendingSample{at: at})
	return true
}

// deleted notes that the batch deleted the KV entry at key.
func (o *sampleOrder) deleted(key []byte) {
	if o != nil {
		o.note(string(key), pendingSample{deleted: true})
	}
}

// maxSampleLead is how far a sample's time may run ahead of the time it was
// ingested before it is ordered by ingest time + maxSampleLead.
const maxSampleLead = time.Minute

// orderingTime is the time a _Metric is ordered by, in unix nanoseconds:
// the payload's sample time at (0: none), capped at maxSampleLead past the
// ingest time ingestMS (unix milliseconds); the ingest time without one. 0
// when neither is known.
func orderingTime(at, ingestMS int64) int64 {
	if ingestMS == 0 {
		return at
	}
	ingest := ingestMS * int64(time.Millisecond)
	if at == 0 {
		return ingest
	}
	return min(at, ingest+int64(maxSampleLead))
}

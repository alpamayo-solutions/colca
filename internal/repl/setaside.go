package repl

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// A record the parent refuses for good must not hold its lane forever.
//
// Order within a stream never changes, so one refused record used to stop
// every record behind it until retention pruned past it. Now the uplink first
// isolates it: a refused batch is sent again at half its size, down to one
// record, so the records before the refused one rise and the refused one is
// named. Refusals are often fixed on the parent (a rolling upgrade, a
// max_record_bytes mismatch), so the isolated record is held and retried for
// setAsideGrace. Only then is it set aside: copied to the local store
// (store.SetAsideUplink), the cursor moved past it, colca_uplink_set_aside_total
// counted, an error logged, and the node's uplink_set_aside finding written.
//
// A set-aside record never reaches the parent: that is data loss there, stated
// in the log and the finding. It is not sent again by itself. The parent drops
// every child offset at or below its high-water mark, and appending the record
// again under a new offset could put an old state value over a newer one at
// the parent. What to do with it is the operator's decision; GET
// /uplink/set-aside lists the copies and DELETE clears them, which retires the
// finding once none is left.

// defaultSetAsideGrace is how long one record may be refused alone before it
// is set aside.
const defaultSetAsideGrace = 15 * time.Minute

// SetAsideAuthor writes the uplink_set_aside finding.
const SetAsideAuthor = "colca-uplink"

// SetAsideReason is the _Finding.reason of the node's set-aside records.
const SetAsideReason = "uplink_set_aside"

// laneHold is what a lane knows about the refusals it is isolating.
type laneHold struct {
	// limit is the most records the next batch may carry, 0 for no limit
	// beyond the usual ones.
	limit int
	// offset is the record the parent refuses alone, 0 for none, refused alone
	// continuously since since.
	offset uint64
	since  time.Time
}

// refused updates the hold after the parent refused batch for good. It returns
// true when the batch was the one record the hold names and its grace is over.
func (h *laneHold) refused(batch []store.ReplRecord, now time.Time, grace time.Duration) bool {
	if len(batch) > 1 {
		h.limit, h.offset = max(1, len(batch)/2), 0
		return false
	}
	h.limit = 1
	off := batch[0].ChildOffset
	if h.offset != off {
		h.offset, h.since = off, now
	}
	// A skip range is not a record: it cannot be copied, and no parent that
	// accepts this node refuses it, so it is only held.
	return batch[0].SkipFrom == 0 && now.Sub(h.since) >= grace
}

// accepted grows the batch back after the parent took one.
func (h *laneHold) accepted() {
	h.offset = 0
	if h.limit > 0 {
		h.limit *= 2
		if h.limit >= replBatch {
			h.limit = 0
		}
	}
}

func (c *Client) grace() time.Duration {
	if c.setAsideGrace > 0 {
		return c.setAsideGrace
	}
	return defaultSetAsideGrace
}

// setAside sets rec aside, unless a re-sync placed the cursors since the page
// was read. It reports whether the cursor moved past the record.
func (c *Client) setAside(eng *engine.Engine, m *metrics.Metrics, epoch uint64, stream string, rec store.StoredRecord, refusal *replError, since time.Time) bool {
	st := eng.Store()
	c.syncMu.Lock()
	if c.epoch != epoch {
		c.syncMu.Unlock()
		return false
	}
	moved, err := st.SetAsideUplink(uns.UplinkCursor(c.parentPub), store.SetAsideRecord{
		Parent: c.parentPub, Stream: stream, Offset: rec.Offset, Record: rec,
		Status: refusal.Status, Reason: truncateReason(refusal.Body),
		HeldSinceMS: since.UnixMilli(), AtMS: time.Now().UnixMilli(),
	})
	c.syncMu.Unlock()
	if err != nil {
		c.log.Error("refused uplink record not set aside — it stays held and is retried",
			"stream", stream, "offset", rec.Offset, "err", err)
		return false
	}
	if !moved {
		return false
	}
	m.UplinkSetAside(stream)
	c.log.Error("uplink record set aside: the parent refused it alone for longer than the grace, so it is NOT replicated "+
		"and the parent does not have it — the lane continues behind it; GET /uplink/set-aside lists the copy",
		"stream", stream, "offset", rec.Offset, "topic", rec.Topic, "bytes", len(rec.Payload),
		"parent", c.base, "status", refusal.Status, "meaning", replicationStatusMeaning(refusal.Status),
		"parent_said", refusal.Body, "refused_for", time.Since(since).Round(time.Second))
	if err := c.publishSetAsideFinding(eng); err != nil {
		c.log.Error("uplink_set_aside finding not written; it is written again at the next set-aside, clear or start",
			"err", err)
	}
	return true
}

// truncateReason bounds what the parent said, as kept with the copy.
func truncateReason(s string) string {
	const most = 1000
	if len(s) > most {
		return s[:most] + "…"
	}
	return s
}

// SetAsideRecords lists the records this node set aside for its parent.
func (c *Client) SetAsideRecords(st *store.Store) ([]store.SetAsideRecord, error) {
	return st.SetAsideRecords(c.parentPub)
}

// ClearSetAside removes one set-aside copy, the operator's decision that it is
// dealt with, and brings the finding up to date: retired once none is left.
func (c *Client) ClearSetAside(eng *engine.Engine, stream string, offset uint64) (bool, error) {
	found, err := eng.Store().ClearSetAside(c.parentPub, stream, offset)
	if err != nil || !found {
		return found, err
	}
	return true, c.publishSetAsideFinding(eng)
}

// SetAsideFindingTopic is where the node's uplink_set_aside finding sits.
func SetAsideFindingTopic(node string) string { return uns.NodeFindingTopic(node, SetAsideReason) }

type setAsideEntry struct {
	Stream string `json:"stream"`
	Offset uint64 `json:"offset"`
	Topic  string `json:"topic"`
	Bytes  int    `json:"bytes"`
	Status int    `json:"status"`
	Reason string `json:"parent_said"`
	AtMS   int64  `json:"set_aside_at_ms"`
}

// publishSetAsideFinding writes the finding from the stored copies, or retires
// it when there are none and it stands.
func (c *Client) publishSetAsideFinding(eng *engine.Engine) error {
	st := eng.Store()
	records, err := c.SetAsideRecords(st)
	if err != nil {
		return err
	}
	topic := SetAsideFindingTopic(eng.NodeID())
	attribution := engine.Attribution{
		WrittenBy: SetAsideAuthor, ActorID: SetAsideAuthor, ActorLabel: SetAsideAuthor, ActorKind: "system",
	}
	if len(records) == 0 {
		p, err := uns.Parse(topic)
		if err != nil {
			return err
		}
		if _, standing, err := st.KVGet(p.Path, p.NodeID, topic); err != nil || !standing {
			return err
		}
		_, err = eng.IngestAdminAttributed(topic, nil, attribution)
		return err
	}
	entries := make([]setAsideEntry, len(records))
	for i, r := range records {
		entries[i] = setAsideEntry{
			Stream: r.Stream, Offset: r.Offset, Topic: r.Record.Topic, Bytes: len(r.Record.Payload),
			Status: r.Status, Reason: r.Reason, AtMS: r.AtMS,
		}
	}
	payload, err := json.Marshal(map[string]any{
		"reason": SetAsideReason,
		"summary": fmt.Sprintf("%d record(s) the parent refused were set aside and are NOT replicated; the parent does not have them",
			len(records)),
		"observed_at":        float64(time.Now().UnixMilli()) / 1000,
		"suggested_severity": "error",
		"detail":             map[string]any{"records": entries, "parent": c.base},
		"remedy": "The parent refused each record alone for longer than the uplink grace (see parent_said: often a " +
			"limits.max_record_bytes mismatch or a record on a stream it may not rise on). The copies are kept: GET " +
			"/uplink/set-aside on this node's admin door lists them. They are not sent again automatically, since the " +
			"parent drops offsets below its high-water mark and a re-sent state value could replace a newer one. " +
			"Decide what to do with each, then DELETE /uplink/set-aside/{stream}/{offset}; the finding retires when none is left.",
	})
	if err != nil {
		return err
	}
	_, err = eng.IngestAdminAttributed(topic, payload, attribution)
	return err
}

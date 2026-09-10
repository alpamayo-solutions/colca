// Move-drain completion. It lives in repl rather than registry because the check
// needs the commands stream and the downlink delivery-floor cursors, which this
// package owns; registry keeps the identity's lifecycle and never reads streams.

package repl

import (
	"errors"
	"time"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// drainTickInterval is the periodic completion sweep. Completion is also checked
// on every poll by the draining child; the tick covers a child that never polls
// again, whose queue then resolves by expiry.
const drainTickInterval = 30 * time.Second

// drainScanBatch bounds one store.Read in the completion scan. The scan as a
// whole runs until the stream is exhausted.
const drainScanBatch = 500

// RunDrainTicker checks every draining child each drainTickInterval and
// auto-revokes those that completed. It runs once before the first tick, so
// drains that survived a restart are checked without waiting an interval.
func (s *Server) RunDrainTicker(stop <-chan struct{}) {
	s.evaluateAllDrains()
	ticker := time.NewTicker(drainTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.evaluateAllDrains()
		}
	}
}

// evaluateAllDrains checks every draining entry in the registry; the tick and
// the restart path call it.
func (s *Server) evaluateAllDrains() {
	for _, e := range s.reg.List() {
		if e.IsDraining() {
			s.evaluateDrain(e.ULID)
		}
	}
}

// evaluateDrain checks childULID's completion and auto-revokes it through
// registry.Manager.Revoke, recording the outcome. It is safe to call
// concurrently: Revoke decides, and the caller that loses gets ErrNotEnrolled,
// which is not an error here. A gap never lets a drain finish early:
// drainPendingCommands always scans the surviving range, and gapped only picks
// the outcome label.
func (s *Server) evaluateDrain(childULID string) {
	e, ok := s.reg.Get(childULID)
	if !ok || !e.IsDraining() {
		return
	}
	total, pending, gapped := s.drainPendingCommands(e)
	s.metrics.DrainPending(childULID, pending)
	if pending > 0 {
		return // still live, undelivered commands in the surviving range — not complete, gap or not
	}

	// A gap outranks "delivered", which means fetched and acked on the downlink
	// cursor and cannot be proven for pruned records, but never "still pending",
	// ruled out above. Any cursor behind the LWM completes as gapped, even if the
	// pruned range held nothing for this mount; the range cannot tell.
	outcome := metrics.DrainOutcomeDelivered
	switch {
	case gapped:
		outcome = metrics.DrainOutcomeGapped
	case total > 0:
		outcome = metrics.DrainOutcomeExpired
	}

	if _, _, err := s.reg.Revoke(childULID); err != nil {
		if errors.Is(err, registry.ErrNotEnrolled) {
			return // lost the race to a concurrent evaluation or a DELETE — not our error
		}
		s.log.Error("move-drain auto-revoke failed", "child", childULID, "err", err)
		return
	}
	s.metrics.DrainCompleted(childULID, outcome)
	if outcome == metrics.DrainOutcomeGapped {
		s.log.Warn("move-drain complete, auto-revoked: retention pruned undelivered commands under this mount before this child fetched or they expired — outcome recorded as gapped, never delivered",
			"child", childULID, "commands_seen_in_surviving_range", total)
		return
	}
	s.log.Info("move-drain complete, auto-revoked", "child", childULID, "outcome", outcome, "commands_seen", total)
}

// drainPendingCommands scans the surviving part of the commands stream under
// e's mount for undelivered commands, from the cursor inclusive, since the
// cursor is the next unread offset. total counts them and pending those still
// live; completion is pending == 0.
//
// gapped reports that e's delivery floor sits behind the LWM, so retention
// pruned something the child never consumed. It does not end the scan: a live
// command past the LWM still blocks the drain, and gapped only labels the
// outcome once pending reaches 0.
func (s *Server) drainPendingCommands(e *uns.Entry) (total, pending int, gapped bool) {
	st := s.eng.Store()
	mount, placed := s.eng.Elements().PathOf(e.Element)
	if !placed {
		// The child's element stopped resolving mid-drain. Nothing can be said about
		// what is still addressed to it, and "nothing pending" would revoke it, so
		// report one pending record and wait.
		s.log.Error("move-drain: the draining child's element does not resolve — treating as still pending",
			"child", e.ULID, "element", e.Element)
		return 1, 1, false
	}
	cursor := st.CursorGet(uns.DownlinkCursorPrefix+e.ULID, "commands")
	_, gapped = st.Gap("commands", cursor)
	from := cursor
	if lwm := st.LWM("commands"); gapped && lwm > from {
		// The pruned prefix is gone; Read would skip it anyway, but starting at the LWM
		// makes the surviving range explicit.
		from = lwm
	}
	next := st.NextOffset("commands")
	nowMS := s.eng.AuthoritativeNow().UnixMilli()
	filter := func(topic string) bool {
		p, err := uns.Parse(topic)
		if err != nil || !uns.IsCommand(s.eng.ClassOf(p.Contract)) {
			return false
		}
		// The same mount rule the /downlink filter uses (uns.UnderMount). They must
		// agree: a different boundary here would complete a drain while commands under
		// that mount were still deliverable.
		return uns.UnderMount(p.Path, mount)
	}
	for from < next {
		recs, nxt, err := st.Read("commands", from, drainScanBatch, filter)
		if err != nil {
			s.log.Error("move-drain completion scan failed — treating as still pending (never falsely completes a drain)",
				"child", e.ULID, "err", err)
			return total + 1, pending + 1, gapped
		}
		for _, r := range recs {
			total++
			if uns.CommandStillLive(r.Payload, nowMS) {
				pending++
			}
		}
		if nxt <= from {
			break // defensive: Read made no progress
		}
		from = nxt
	}
	return total, pending, gapped
}

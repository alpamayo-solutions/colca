// Move-drain completion. It lives in repl rather than registry because the check
// needs the commands stream and the downlink delivery-floor cursors, which this
// package owns; registry keeps the identity's lifecycle and never reads streams.

package repl

import (
	"errors"
	"github.com/alpamayo-solutions/colca/door"
	"time"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// drainScanBatch bounds one store.Read in the completion scan. The scan as a
// whole runs until the stream is exhausted.
const drainScanBatch = 500

// RunDrainCompletion resumes durable drains on startup, then on store/cursor
// changes or the earliest undelivered command expiry. Idle nodes do no scans.
func (s *Server) RunDrainCompletion(stop <-chan struct{}) {
	for {
		changed := s.eng.Store().BacklogChanges()
		delay := s.evaluateAllDrains()
		var due <-chan time.Time
		var timer *time.Timer
		if delay >= 0 {
			timer = time.NewTimer(delay)
			due = timer.C
		}
		select {
		case <-stop:
			if timer != nil {
				timer.Stop()
			}
			return
		case <-changed:
			if timer != nil {
				timer.Stop()
			}
			// Coalesce busy-stream hints without extending the batching deadline.
			timer = time.NewTimer(time.Second)
			select {
			case <-stop:
				timer.Stop()
				return
			case <-timer.C:
			}
		case <-due:
		}
	}
}

func (s *Server) evaluateAllDrains() time.Duration {
	next := time.Duration(-1)
	for _, e := range s.reg.List() {
		if e.IsDraining() {
			if delay := s.evaluateDrain(e.ULID); delay >= 0 && (next < 0 || delay < next) {
				next = delay
			}
		}
	}
	return next
}

// evaluateDrain checks childULID's completion and auto-retires it through
// registry.Manager.Retire, recording the outcome. It is safe to call
// concurrently: Retire decides, and the caller that loses gets ErrNotEnrolled,
// which is not an error here. A gap never lets a drain finish early:
// drainPendingCommands always scans the surviving range, and gapped only picks
// the outcome label.
func (s *Server) evaluateDrain(childULID string) time.Duration {
	e, ok := s.reg.Get(childULID)
	if !ok || !e.IsDraining() {
		return -1
	}
	total, pending, gapped, delay := s.scanDrain(e)
	s.metrics.DrainPending(childULID, pending)
	if pending > 0 {
		return delay // still live, undelivered commands in the surviving range — not complete, gap or not
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

	// A completed drain retires the child, not just revokes it. The child leaves
	// this parent for good: re-parented, its state rises through its new parent
	// at another path, or taken out of service. Either way what it replicated
	// here would stand as ghosts that consumers read as live, and nothing else
	// can retire them. A child that returns is a fresh enrollment and replicates
	// from its marks' reset.
	_, _, retired, err := s.reg.Retire(childULID)
	if err != nil {
		if errors.Is(err, registry.ErrNotEnrolled) {
			return -1 // lost the race to a concurrent evaluation or a DELETE — not our error
		}
		s.log.Error("move-drain auto-retire failed", "child", childULID, "err", err)
		return door.RetryDelay(err, 30*time.Second)
	}
	s.metrics.DrainCompleted(childULID, outcome)
	if outcome == metrics.DrainOutcomeGapped {
		s.log.Warn("move-drain complete, auto-retired: retention pruned undelivered commands under this mount before this child fetched or they expired — outcome recorded as gapped, never delivered",
			"child", childULID, "commands_seen_in_surviving_range", total, "records_retired", retired)
		return -1
	}
	s.log.Info("move-drain complete, auto-retired", "child", childULID, "outcome", outcome, "commands_seen", total, "records_retired", retired)
	return -1
}

// scanDrain scans the surviving part of the commands stream under
// e's mount for undelivered commands, from the cursor inclusive, since the
// cursor is the next unread offset. total counts them and pending those still
// live; completion is pending == 0.
//
// gapped reports that e's delivery floor sits behind the LWM, so retention
// pruned something the child never consumed. It does not end the scan: a live
// command past the LWM still blocks the drain, and gapped only labels the
// outcome once pending reaches 0.
func (s *Server) scanDrain(e *uns.Entry) (total, pending int, gapped bool, delay time.Duration) {
	delay = -1
	st := s.eng.Store()
	mount, placed := s.eng.Elements().PathOf(e.Element)
	if !placed {
		// The child's element stopped resolving mid-drain. Nothing can be said about
		// what is still addressed to it, and "nothing pending" would revoke it, so
		// report one pending record and wait.
		s.log.Error("move-drain: the draining child's element does not resolve — treating as still pending",
			"child", e.ULID, "element", e.Element)
		return 1, 1, false, door.RetryDelay(nil, 30*time.Second)
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
			return total + 1, pending + 1, gapped, door.RetryDelay(err, 30*time.Second)
		}
		for _, r := range recs {
			total++
			if uns.CommandStillLive(r.Payload, nowMS) {
				pending++
				// A command without expires_at never expires: only its delivery, or a
				// forced DELETE, ends the drain, and no timer is due for it.
				deadline, ok := uns.CommandDeadline(r.Payload)
				if !ok {
					continue
				}
				wait := time.Duration(max(1, deadline-nowMS+1)) * time.Millisecond
				if delay < 0 || wait < delay {
					delay = wait
				}
			}
		}
		if nxt <= from {
			break // defensive: Read made no progress
		}
		from = nxt
	}
	return total, pending, gapped, delay
}

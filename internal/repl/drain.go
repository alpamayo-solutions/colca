// Move-drain completion (the time sync move drain design §3.2/§3.4).
//
// This lives in repl, not registry: the completion predicate needs the
// store's commands stream and the /downlink delivery-floor cursor, both of
// which this package already owns for the /downlink door itself. registry
// stays the identity/lifecycle authority (status "draining", persisted) and
// never reaches into stream contents (registry package doc comment).
package repl

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// drainTickInterval is the periodic completion sweep (design §3.2):
// "evaluated on every /downlink poll by that child AND a 30s periodic tick,
// whichever fires first" — this is the second trigger, the one that still
// converges a drain whose child never polls again (already offline; the
// queue then resolves purely by expiry).
const drainTickInterval = 30 * time.Second

// drainScanBatch bounds one store.Read call inside the completion scan (the
// scan as a whole is unbounded — it loops until the commands stream is
// exhausted); this only caps the per-call cost, same reasoning as the
// downlink door's own defaultDownlinkMax.
const drainScanBatch = 500

// RunDrainTicker sweeps every draining child every drainTickInterval,
// auto-revoking whichever completed (design §3.2 item 4). It evaluates once
// immediately before the first tick, so a restart re-evaluates drains that
// persisted through it without waiting a full interval (design §3.2:
// "status ... survives restart; the tick re-evaluates after boot").
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

// evaluateAllDrains evaluates every currently draining entry in the
// registry — the periodic tick's and the post-restart entry point.
func (s *Server) evaluateAllDrains() {
	for _, e := range s.reg.List() {
		if e.IsDraining() {
			s.evaluateDrain(e.ULID)
		}
	}
}

// evaluateDrain checks childULID's completion predicate (design §3.2 item 3)
// and auto-revokes through the existing registry.Manager.Revoke path on
// completion (item 4), recording the outcome. Safe to call redundantly and
// concurrently — from a /downlink poll and the periodic tick at once, or
// twice in the same tick — registry.Manager.Revoke is the synchronization
// point: the caller that loses the race gets ErrNotEnrolled and is a no-op
// here, not an error.
//
// A gap and surviving pending work are ORTHOGONAL facts, never alternatives: a pruned prefix
// [cursor, LWM) being unrecoverable does not mean the SURVIVING range
// [LWM, next) has nothing left to deliver. drainPendingCommands always
// scans the surviving range regardless of gapped, so pending here reflects
// real, currently-live, undelivered commands the drain must still wait on —
// gapped only ever affects which outcome label the eventual completion
// gets, never whether the drain is allowed to complete right now.
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

	// Outcome precedence: a detected gap always wins over
	// "delivered" — the design's own §3.2 clarification defines "delivered"
	// as fetched-and-acked on the downlink cursor, and the pruned prefix's
	// contents are gone and can never be proven to have been that — but never wins over "still pending", which the check
	// above already ruled out. Conservative by construction: ANY cursor
	// behind the LWM completes as "gapped" once the surviving range clears,
	// independent of whether the pruned range provably held a command for
	// THIS mount specifically (the pruned range carries no per-mount
	// record, so "might have" is treated exactly like "did").
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

// drainPendingCommands scans the commands stream under e's mount for
// ClassCmd records not yet delivered, for the SURVIVING range only (see
// gapped below). total counts every such record found; pending is the
// subset still live (expires_at >= AuthoritativeNow) — completion is
// pending == 0 (design §3.2 item 3, erratum: the cursor boundary is
// inclusive — offset >= cursor, since store.CursorGet's cursor IS the next
// offset a consumer has not yet read; the record sitting exactly at that
// offset has itself not been delivered).
//
// gapped is true when e's own delivery-floor cursor sits behind the
// commands stream's current LWM (store.Gap) — retention pruned some or all
// of [cursor, LWM) before this child (whose cursor may never have advanced
// past its never-acked default) consumed it.
//
// gapped is NOT a short-circuit: a lost
// prefix says nothing about the SURVIVING range [LWM, next), which the scan
// below always covers regardless — store.Gap only proves loss up to LWM-1,
// per its own contract (store.go Gap doc comment), so a live, undelivered,
// unexpired command sitting at or after the LWM is exactly as real and
// exactly as blocking as it would be with no gap at all. Returning
// gapped=true early here (the round-1 shape) meant a draining child could
// be auto-revoked while such a command sat unread past the hole — command
// abandonment, strictly worse than the mislabeling round-1 fixed. gapped is
// therefore pure OUTCOME-LABEL information for the caller once pending
// naturally reaches 0 on its own — it never itself decides completion.
func (s *Server) drainPendingCommands(e *uns.Entry) (total, pending int, gapped bool) {
	st := s.eng.Store()
	mount, placed := s.eng.MountOf(e.ULID)
	if !placed {
		// The child's element stopped resolving mid-drain. Nothing can be said
		// about what is still addressed to it, and "nothing pending" would
		// auto-revoke it — so report one pending record and let the drain wait
		// for the namespace to come back.
		s.log.Error("move-drain: the draining child's element does not resolve — treating as still pending",
			"child", e.ULID, "element", e.Element)
		return 1, 1, false
	}
	cursor := st.CursorGet(uns.DownlinkCursorPrefix+e.ULID, "commands")
	_, gapped = st.Gap("commands", cursor)
	from := cursor
	if lwm := st.LWM("commands"); gapped && lwm > from {
		// The pruned prefix [cursor, lwm) no longer exists in the store —
		// store.Read would silently skip it anyway (deleted keys), but
		// starting exactly at the LWM makes the surviving range explicit
		// rather than relying on that skip behavior.
		from = lwm
	}
	next := st.NextOffset("commands")
	nowMS := s.eng.AuthoritativeNow().UnixMilli()
	filter := func(topic string) bool {
		p, err := uns.Parse(topic)
		if err != nil || !uns.IsCommand(s.eng.ClassOf(p.Contract)) {
			return false
		}
		// Same mount boundary check as the /downlink filter (server.go): the
		// path separator is the boundary, "mount10" is not under "mount1".
		return strings.HasPrefix(p.Path, mount+"/")
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
			if commandStillLive(r.Payload, nowMS) {
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

// commandStillLive reports whether a ClassCmd record's expires_at has not
// yet passed authoritativeNowMS. uns.Validate already guarantees every
// persisted _Cmd* payload carries a numeric expires_at (move-drain design
// §3.2: "Validate already requires a numeric expires_at on every _Cmd*, so
// the drain deadline is bounded"), so a decode failure here cannot happen
// for real data — treated as still-live defensively rather than silently
// completing a drain on malformed input.
func commandStillLive(payload []byte, authoritativeNowMS int64) bool {
	var body struct {
		ExpiresAt float64 `json:"expires_at"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return true
	}
	return int64(body.ExpiresAt) >= authoritativeNowMS
}

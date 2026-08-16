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
		if e.Status == uns.StatusDraining {
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
func (s *Server) evaluateDrain(childULID string) {
	e, ok := s.reg.Get(childULID)
	if !ok || e.Status != uns.StatusDraining {
		return
	}
	total, pending := s.drainPendingCommands(e)
	s.metrics.DrainPending(childULID, pending)
	if pending > 0 {
		return // still live, undelivered commands under the mount
	}
	outcome := metrics.DrainOutcomeDelivered
	if total > 0 {
		outcome = metrics.DrainOutcomeExpired
	}
	if _, err := s.reg.Revoke(childULID); err != nil {
		if errors.Is(err, registry.ErrNotEnrolled) {
			return // lost the race to a concurrent evaluation or a DELETE — not our error
		}
		s.log.Error("move-drain auto-revoke failed", "child", childULID, "err", err)
		return
	}
	s.metrics.DrainCompleted(childULID, outcome)
	s.log.Info("move-drain complete, auto-revoked", "child", childULID, "outcome", outcome, "commands_seen", total)
}

// drainPendingCommands scans the commands stream under e's mount, starting
// at its downlink delivery-floor cursor (not the stream start), for ClassCmd
// records not yet delivered. total counts every such record; pending is the
// subset still live (expires_at >= AuthoritativeNow) — completion is
// pending == 0 (design §3.2 item 3).
//
// [delta] The design text (§3.2 item 3) says "offset > the downlink:{child}
// cursor". This implementation scans from offset >= cursor instead, to match
// the store's actual cursor semantics (store.CursorGet: "the NEXT offset a
// named consumer should read from a stream" — the record sitting AT the
// cursor position has itself not been delivered yet). Reading the design
// text literally would let a drain complete while the very next undelivered
// command — the one at exactly the cursor offset — is silently uncounted,
// which contradicts the design's own termination guarantee ("deliver or
// expire, literally"). Flagged for review per the design's own
// [delta] convention.
func (s *Server) drainPendingCommands(e *uns.Entry) (total, pending int) {
	st := s.eng.Store()
	cursor := st.CursorGet(uns.DownlinkCursorPrefix+e.ULID, "commands")
	next := st.NextOffset("commands")
	nowMS := s.eng.AuthoritativeNow().UnixMilli()
	filter := func(topic string) bool {
		p, err := uns.Parse(topic)
		if err != nil || uns.ClassOf(p.Contract) != uns.ClassCmd {
			return false
		}
		// Same mount boundary check as the /downlink filter (server.go): the
		// path separator is the boundary, "mount10" is not under "mount1".
		return strings.HasPrefix(p.Path, e.Mount+"/")
	}
	for from := cursor; from < next; {
		recs, nxt, err := st.Read("commands", from, drainScanBatch, filter)
		if err != nil {
			s.log.Error("move-drain completion scan failed — treating as still pending (never falsely completes a drain)",
				"child", e.ULID, "err", err)
			return total + 1, pending + 1
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
	return total, pending
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

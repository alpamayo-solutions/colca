package engine

import (
	"fmt"
	"sync"
	"time"
)

// auditRepeatWindow is how long the repeats of one denial are counted before
// they are reported in one record.
const auditRepeatWindow = 5 * time.Minute

// maxAuditRepeatKeys bounds the denials tracked one by one. Past it, further
// denials share one account per door, operation and reason, so a flood of
// distinct unknown keys is still written at a bounded rate.
const maxAuditRepeatKeys = 2048

// auditRepeatKey is what makes two denials the same one: who was refused,
// what, why, and at which door. The route and method are left out, because a
// caller refused at the door is refused on every route it tries.
type auditRepeatKey struct {
	operation, reason, door string
	who                     string
	entityType, entityID    string
	subject                 string
}

type auditRepeat struct {
	since   time.Time
	repeats int
	last    AuditDenial
}

// auditRepeats keeps one denial from becoming one audit record per attempt. An
// edge that ran before its enrolment asked its parent ten times a second and
// was refused each time: 3,470 identical records in six minutes, 61 % of the
// parent's audit. The first denial of a caller and cause is written at once.
// Its repeats are counted, and written as one record carrying the count when
// the window ends: with the next denial after it, or by Flush when the caller
// stopped.
type auditRepeats struct {
	mu sync.Mutex
	m  map[auditRepeatKey]*auditRepeat
}

func auditDoor(d AuditDenial) string {
	door, _ := d.Metadata["door"].(string)
	return door
}

func auditKeyOf(d AuditDenial) auditRepeatKey {
	who := d.ActorID
	if who == "" {
		who = d.Peer
	}
	subject := ""
	for _, key := range []string{"filter", "topic", "cursor", "stream", "contract"} {
		if v, ok := d.Metadata[key]; ok {
			subject += key + "=" + fmt.Sprint(v) + ";"
		}
	}
	return auditRepeatKey{
		operation: d.Operation, reason: d.ReasonCode, door: auditDoor(d), who: who,
		entityType: d.EntityType, entityID: d.EntityID, subject: subject,
	}
}

// admit decides whether a denial is written now. With write it returns the
// repeats that were not written since the last record of the same denial, and
// when that record was written.
func (r *auditRepeats) admit(d AuditDenial, now time.Time) (write bool, repeated int, since time.Time) {
	key := auditKeyOf(d)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[auditRepeatKey]*auditRepeat{}
	}
	held, ok := r.m[key]
	if !ok && len(r.m) >= maxAuditRepeatKeys {
		// Full: the caller shares the account of everyone refused the same way.
		key.who, key.entityType, key.entityID, key.subject = "", "", "", ""
		held, ok = r.m[key]
	}
	if !ok {
		r.m[key] = &auditRepeat{since: now, last: d}
		return true, 0, time.Time{}
	}
	if now.Sub(held.since) < auditRepeatWindow {
		held.repeats++
		held.last = d
		return false, 0, time.Time{}
	}
	repeated, since = held.repeats, held.since
	held.since, held.repeats, held.last = now, 0, d
	return true, repeated, since
}

// due removes the accounts whose window ended, or every account with all, and
// returns those that hold repeats nobody was told about.
func (r *auditRepeats) due(now time.Time, all bool) []auditRepeat {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []auditRepeat
	for key, held := range r.m {
		if !all && now.Sub(held.since) < auditRepeatWindow {
			continue
		}
		delete(r.m, key)
		if held.repeats > 0 {
			out = append(out, *held)
		}
	}
	return out
}

func withAuditMetadata(metadata map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(metadata)+1)
	for k, v := range metadata {
		out[k] = v
	}
	out[key] = value
	return out
}

// repeatedDenial adds to a denial how often it was repeated without a record
// of its own, and since when.
func repeatedDenial(d AuditDenial, repeated int, since time.Time) AuditDenial {
	if repeated == 0 {
		return d
	}
	d.Metadata = withAuditMetadata(d.Metadata, "repeated", repeated)
	d.Metadata["repeated_since"] = since.UnixMilli()
	return d
}

// auditRepeatSweep is how often the repeats of denials whose window ended are
// written.
const auditRepeatSweep = time.Minute

// RunAuditRepeats writes the repeats of denials whose caller stopped: one
// record per denial with the count, so the audit says how often it happened
// even when no further denial follows. It returns when stop closes;
// FlushAuditRepeats then writes what is still counted.
func (e *Engine) RunAuditRepeats(stop <-chan struct{}) {
	ticker := time.NewTicker(auditRepeatSweep)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			e.writeAuditRepeats(false)
		}
	}
}

// FlushAuditRepeats writes every repeat count still held. A node calls it on
// Stop, before the store closes.
func (e *Engine) FlushAuditRepeats() { e.writeAuditRepeats(true) }

func (e *Engine) writeAuditRepeats(all bool) {
	for _, held := range e.auditRepeats.due(e.Now(), all) {
		_ = e.recordAuthorization(repeatedDenial(held.last, held.repeats, held.since), "denied")
	}
}

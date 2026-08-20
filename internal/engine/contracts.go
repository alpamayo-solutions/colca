// Contract authority resolution (schema-bundle design §7): with a loaded
// bundle the bundle answers class, tombstonability and schema for every
// contract it carries — fully replacing the builtin floor, never merging.
// Without one, the floor (plugins/uns) applies unchanged. The builtin-only
// contracts (_StreamGap, _EnrolledIdentity, _TimeSync) are ALWAYS answered by the
// binary: their producers live in this process, so their rules evolve with
// it (§10.2) and a bundle may not redeclare them (loader-enforced).
package engine

import (
	"errors"
	"fmt"

	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// RejectError carries the metrics reason with a door rejection so the broker
// hook can answer MQTT-5 PUBACK reason codes (design §8) without parsing
// error strings. Reason values are exactly the metrics label vocabulary.
type RejectError struct {
	Reason string
	Err    error
}

func (r *RejectError) Error() string { return r.Err.Error() }
func (r *RejectError) Unwrap() error { return r.Err }

// reject counts the reason and returns the typed error — the single exit for
// every door rejection that can surface on the MQTT wire.
func (e *Engine) reject(reason, format string, args ...any) (Result, error) {
	e.metrics.RejectPublish(reason)
	return Result{}, &RejectError{Reason: reason, Err: fmt.Errorf(format, args...)}
}

// rejectDenied preserves an authorization denial before returning the same
// typed error as reject. Audit persistence is best-effort only in the sense
// that its failure cannot turn the protected operation into an allow.
func (e *Engine) rejectDenied(reason string, actor Attribution, operation string, p *uns.Parsed, format string, args ...any) (Result, error) {
	d := AuditDenial{
		Operation: operation, ReasonCode: reason,
		ActorID: actor.ActorID, ActorLabel: actor.ActorLabel, ActorKind: actor.ActorKind,
	}
	if p != nil {
		d.EntityType, d.EntityID = p.Contract, p.Path
		d.Metadata = map[string]any{"contract": p.Contract}
	}
	e.recordDenial(d)
	return e.reject(reason, format, args...)
}

var builtinOnly = map[string]bool{"_StreamGap": true, "_EnrolledIdentity": true, "_TimeSync": true}

// SetContracts installs the loaded bundle table (node startup; nil = floor).
func (e *Engine) SetContracts(t *contracts.Table) { e.contracts = t }

// ClassOf resolves a contract's routing class through the active authority.
// Public because the repl server (KV projection on replicate, downlink
// command filter), drain accounting and the retention pruner must route by
// the SAME authority as the doors — a bundle-declared contract exists for
// all of them or none.
func (e *Engine) ClassOf(contract string) uns.Class {
	if e.contracts == nil || builtinOnly[contract] {
		return uns.ClassOf(contract)
	}
	if r, ok := e.contracts.Lookup(contract); ok {
		return r.Class
	}
	return uns.ClassNone
}

// validateContract applies the active authority's payload rules: tombstone
// admissibility for empty payloads, schema for everything else. Unknown
// contracts are rejected — the validated-namespace principle holds with and
// without a bundle.
func (e *Engine) validateContract(contract string, payload []byte) error {
	if e.contracts == nil || builtinOnly[contract] {
		return uns.Validate(contract, payload)
	}
	r, ok := e.contracts.Lookup(contract)
	if !ok {
		return fmt.Errorf("unknown contract %q — validated namespace rejects unknown contracts", contract)
	}
	if len(payload) == 0 {
		if r.Tombstone {
			return nil // path retirement (retention §7.1), schema never consulted (§10.1)
		}
		return fmt.Errorf("%s: empty payload (tombstone) is not admissible for this contract", contract)
	}
	return r.Validate(payload)
}

// BundleInfo reports the active authority for colca_contracts_bundle_info.
func (e *Engine) BundleInfo() (version, digest, source string, n int) {
	if e.contracts == nil {
		return "", "", "builtin", 0
	}
	version, digest, n = e.contracts.Info()
	return version, digest, "bundle", n
}

// ReasonOf is the exported hook-side accessor (mqttsrv maps it to PUBACK
// codes, design §8.1). Kept beside RejectError so the vocabulary and the
// accessor cannot drift apart. "" = untyped error.
func ReasonOf(err error) string {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return ""
}

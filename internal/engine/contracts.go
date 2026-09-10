// With a loaded bundle, the bundle answers class, tombstonability and schema for
// every contract it carries, replacing the builtin floor rather than merging with
// it. Without one, the floor in plugins/uns applies. _StreamGap,
// _EnrolledIdentity and _TimeSync are always answered by the binary that produces
// them; a bundle may not redeclare them.

package engine

import (
	"errors"
	"fmt"

	"github.com/alpamayo-solutions/colca/internal/contracts"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// RejectError carries the metrics reason of a door rejection, so the broker can
// answer MQTT 5 PUBACK reason codes without parsing strings. Denied marks an
// authorization denial as opposed to a malformed request; check it with
// errors.Is(err, ErrDenied).
type RejectError struct {
	Reason string
	Denied bool
	Err    error
}

func (r *RejectError) Error() string { return r.Err.Error() }
func (r *RejectError) Unwrap() error { return r.Err }

// Is reports whether this rejection is an authorization denial, so callers
// can write errors.Is(err, engine.ErrDenied) instead of parsing Reason.
func (r *RejectError) Is(target error) bool { return target == ErrDenied && r.Denied }

// ErrDenied is the sentinel authorization denials match via errors.Is.
var ErrDenied = errors.New("denied")

// reject counts the reason and returns the typed error; every door rejection
// that can reach the MQTT wire goes through it.
func (e *Engine) reject(reason, format string, args ...any) (Result, error) {
	e.metrics.RejectPublish(reason)
	return Result{}, &RejectError{Reason: reason, Err: fmt.Errorf(format, args...)}
}

// rejectDenied records an authorization denial in the audit stream and returns a
// RejectError marked as denied. A failed audit write never turns the denial into
// an allow.
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
	e.metrics.RejectPublish(reason)
	return Result{}, &RejectError{Reason: reason, Denied: true, Err: fmt.Errorf(format, args...)}
}

var builtinOnly = map[string]bool{"_StreamGap": true, "_EnrolledIdentity": true, "_TimeSync": true}

// SetContracts installs the loaded bundle table (node startup; nil = floor).
func (e *Engine) SetContracts(t *contracts.Table) { e.contracts = t }

// ClassOf resolves a contract's routing class through the active authority. It
// is exported because replication, drain accounting and retention must route
// exactly like the doors.
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
// admissibility for empty payloads, the schema otherwise. Unknown contracts are
// rejected.
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
			return nil // a tombstone skips the schema
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

// ReasonOf returns the reason of a RejectError, or "" for any other error.
// mqttsrv maps it to PUBACK codes.
func ReasonOf(err error) string {
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return ""
}

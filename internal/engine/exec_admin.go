// _CmdAdmin execution: enrolling and revoking the identities this node knows
// (cmdadmin design §5). This one stays in the core because it IS core — the
// registry is colca's own tree membership, not domain data. It reaches the
// engine through the same CommandExecutor port as every other executor, so the
// dispatch has no special case for it.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alpamayo-solutions/colca/plugins/uns"

	"github.com/alpamayo-solutions/colca/internal/registry"
)

// AdminExec is the registry write surface the admin executor drives — the exact
// code path the local enrollment door calls (one write path inside). Satisfied
// by *registry.Manager.
type AdminExec interface {
	Enroll(entryJSON []byte) (ulid string, offset uint64, err error)
	Revoke(ulid string) (offset uint64, wasDraining bool, err error)
}

// AdminExecutor answers _CmdAdmin against a node's registry.
type AdminExecutor struct{ registry AdminExec }

// NewAdminExecutor wires the registry manager in (node startup). A nil registry
// answers 500 on every _CmdAdmin addressed here — fail loud, never panic.
func NewAdminExecutor(r AdminExec) *AdminExecutor { return &AdminExecutor{registry: r} }

func (a *AdminExecutor) Handles(contract string) bool { return contract == "_CmdAdmin" }

// adminCmdBody carries the per-verb fields; the envelope (correlation_id,
// expires_at) is the engine's business and already handled by the time this runs.
type adminCmdBody struct {
	Entry json.RawMessage `json:"entry"`
	ULID  string          `json:"ulid"`
}

func (a *AdminExecutor) Execute(_ uns.CommandContext, contract, verb string, payload []byte) (int, string, string) {
	if a.registry == nil {
		return 500, "no admin executor wired", "error"
	}
	var body adminCmdBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "admin: unreadable payload: " + err.Error(), "invalid"
	}

	switch verb {
	case "enroll":
		if len(body.Entry) == 0 {
			return 422, "enroll: missing entry", "invalid"
		}
		ulid, _, err := a.registry.Enroll(body.Entry)
		if err != nil {
			return enrollCode(err), err.Error(), enrollResult(err)
		}
		return 200, "enrolled " + ulid, "ok"
	case "revoke":
		if body.ULID == "" {
			return 422, "revoke: missing ulid", "invalid"
		}
		if _, _, err := a.registry.Revoke(body.ULID); err != nil {
			if errors.Is(err, registry.ErrNotEnrolled) {
				// Idempotent by design (cmdadmin design §7): the desired state
				// already holds.
				return 200, "already revoked", "ok"
			}
			return 500, err.Error(), "error"
		}
		return 200, "revoked " + body.ULID, "ok"
	default:
		return 422, fmt.Sprintf("unknown admin verb %q", verb), "invalid"
	}
}

func enrollCode(err error) int {
	if errors.Is(err, registry.ErrConflict) {
		return 409
	}
	return 422 // entry rejected (validation); store failures surface as 500 via Revoke-style paths
}

func enrollResult(err error) string {
	if errors.Is(err, registry.ErrConflict) {
		return "conflict"
	}
	return "invalid"
}

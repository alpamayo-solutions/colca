// _CmdAdmin execution: enrolling and revoking identities on this node. It stays
// in the core because the registry is Colca's own membership, not domain data,
// and uses the same CommandExecutor port as every other executor.

package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/alpamayo-solutions/colca/plugins/uns"

	"github.com/alpamayo-solutions/colca/internal/registry"
)

// AdminExec is the registry write surface the admin executor drives, the same
// one the enrollment door uses. Satisfied by *registry.Manager.
type AdminExec interface {
	Enroll(entryJSON []byte) (ulid string, offset uint64, err error)
	Revoke(ulid string) (offset uint64, wasDraining bool, err error)
}

// AdminExecutor answers _CmdAdmin against a node's registry.
type AdminExecutor struct{ registry AdminExec }

// NewAdminExecutor wires the registry manager in. With a nil registry every
// _CmdAdmin addressed here answers 500.
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
				// Idempotent: the desired state already holds.
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

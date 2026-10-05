// _CmdAdmin execution: enrolling and revoking identities on this node, and
// reading its own logs (fetchLogs). It stays in the core because the registry
// and the logs stream are Colca's own, not domain data, and uses the same
// CommandExecutor port as every other executor.

package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// AdminExec is the registry write surface the admin executor drives, the same
// one the enrollment door uses. Satisfied by *registry.Manager.
type AdminExec interface {
	Enroll(entryJSON []byte) (ulid string, offset uint64, err error)
	Revoke(ulid string) (offset uint64, wasDraining bool, err error)
}

// AdminExecutor answers _CmdAdmin against a node's registry and logs stream.
type AdminExecutor struct {
	registry AdminExec
	logs     LogReader
	now      func() time.Time
	// scans holds a slot per running fetchLogs scan (FetchLogsMaxConcurrent).
	scans chan struct{}
	pages pageBucket
}

// NewAdminExecutor wires the registry manager and the node's store in. With a
// nil registry enroll and revoke answer 500; with nil logs fetchLogs does.
func NewAdminExecutor(r AdminExec, logs LogReader) *AdminExecutor {
	return &AdminExecutor{registry: r, logs: logs, now: time.Now, scans: make(chan struct{}, FetchLogsMaxConcurrent)}
}

func (a *AdminExecutor) Handles(contract string) bool { return contract == "_CmdAdmin" }

// adminCmdBody carries the per-verb fields; the envelope (correlation_id,
// expires_at) is the engine's business and already handled by the time this runs.
type adminCmdBody struct {
	Entry json.RawMessage `json:"entry"`
	ULID  string          `json:"ulid"`
}

func (a *AdminExecutor) Execute(ctx uns.CommandContext, contract, verb string, payload []byte) (int, string, string) {
	code, message, result, _ := a.ExecuteWithResult(ctx, contract, verb, payload)
	return code, message, result
}

// ExecuteWithResult is Execute that also returns the verb's result document,
// which the engine puts into the ack. Only fetchLogs has one.
func (a *AdminExecutor) ExecuteWithResult(
	ctx uns.CommandContext,
	contract, verb string,
	payload []byte,
) (int, string, string, json.RawMessage) {
	if verb == "fetchLogs" {
		return a.fetchLogs(payload)
	}
	code, message, result := a.executeRegistry(verb, payload)
	return code, message, result, nil
}

func (a *AdminExecutor) fetchLogs(payload []byte) (int, string, string, json.RawMessage) {
	if a.logs == nil {
		return 500, "no log store wired", "error", nil
	}
	q, err := parseFetchLogs(payload)
	if err != nil {
		return 422, err.Error(), "invalid", nil
	}
	if ok, wait := a.pages.take(a.now()); !ok {
		return 429, fmt.Sprintf("fetchLogs: more than %d pages an hour, retry in %s",
			FetchLogsPagesPerHour, wait.Round(time.Second)), "busy", nil
	}
	a.scans <- struct{}{}
	page, err := fetchLogs(a.logs, q, a.now(), fetchLogsBudgetFor(a.logs.MaxRecordBytes()))
	<-a.scans
	if err != nil {
		return 500, "fetchLogs: " + err.Error(), "error", nil
	}
	data, err := json.Marshal(page)
	if err != nil {
		return 500, "fetchLogs: encode page: " + err.Error(), "error", nil
	}
	return 200, fmt.Sprintf("%d log records", len(page.Records)), "ok", data
}

func (a *AdminExecutor) executeRegistry(verb string, payload []byte) (int, string, string) {
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

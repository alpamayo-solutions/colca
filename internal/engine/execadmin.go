// _CmdAdmin execution (cmdadmin design §5): a command record addressed to
// THIS node (level-4 ULID match) executes against the local registry right
// after it was durably persisted, and the outcome goes back up as an _Ack in
// the node's own frame. The hook fires on the trusted-down paths only —
// downlink from the parent and the local doors — never on IngestReplicated:
// commands flow down, a child pushing _CmdAdmin upward must not administer
// its ancestors.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// AdminExec is the registry write surface ExecAdmin drives — the exact code
// path the local enrollment door calls (one write path inside). Satisfied by
// *registry.Manager.
type AdminExec interface {
	Enroll(entryJSON []byte) (ulid string, offset uint64, err error)
	Revoke(ulid string) (offset uint64, wasDraining bool, err error)
}

// SetAdmin wires the registry manager in (node startup). An engine without
// one acks 500 on every _CmdAdmin addressed to it — fail loud, never panic.
func (e *Engine) SetAdmin(a AdminExec) { e.admin = a }

// adminCmdBody is the verb payload (design §2): correlation_id and
// expires_at are the command-class contract, entry/ulid are per-verb.
type adminCmdBody struct {
	CorrelationID string          `json:"correlation_id"`
	ExpiresAt     int64           `json:"expires_at"`
	Entry         json.RawMessage `json:"entry"`
	ULID          string          `json:"ulid"`
}

// maybeExecAdmin runs after a _Cmd* record was persisted by a trusted-down
// path. Non-admin contracts and foreign targets return immediately.
func (e *Engine) maybeExecAdmin(p uns.Parsed, payload []byte) {
	if p.Contract != "_CmdAdmin" || p.NodeID != e.cfg.ULID {
		return
	}
	verb := p.Path // at the target the mount-stripped path IS the verb
	var body adminCmdBody
	if err := json.Unmarshal(payload, &body); err != nil || body.CorrelationID == "" {
		// An ack without a correlation_id is unroutable (same rule as the
		// machine shim): log and count, nothing else we can do.
		e.log.Error("cmdadmin: unexecutable payload — no ack possible", "verb", verb, "err", err)
		e.metrics.AdminCmd(verb, "invalid")
		return
	}
	if body.ExpiresAt <= time.Now().UnixMilli() {
		e.ackAdmin(verb, body.CorrelationID, 498, "expired", "expired")
		return
	}
	if e.admin == nil {
		e.ackAdmin(verb, body.CorrelationID, 500, "no admin executor wired", "error")
		return
	}
	switch verb {
	case "enroll":
		if len(body.Entry) == 0 {
			e.ackAdmin(verb, body.CorrelationID, 422, "enroll: missing entry", "invalid")
			return
		}
		ulid, _, err := e.admin.Enroll(body.Entry)
		if err != nil {
			e.ackAdmin(verb, body.CorrelationID, enrollCode(err), err.Error(), enrollResult(err))
			return
		}
		e.ackAdmin(verb, body.CorrelationID, 200, "enrolled "+ulid, "ok")
	case "revoke":
		if body.ULID == "" {
			e.ackAdmin(verb, body.CorrelationID, 422, "revoke: missing ulid", "invalid")
			return
		}
		if _, _, err := e.admin.Revoke(body.ULID); err != nil {
			if errors.Is(err, registry.ErrNotEnrolled) {
				// Idempotent by design (§7): the desired state already holds.
				e.ackAdmin(verb, body.CorrelationID, 200, "already revoked", "ok")
				return
			}
			e.ackAdmin(verb, body.CorrelationID, 500, err.Error(), "error")
			return
		}
		e.ackAdmin(verb, body.CorrelationID, 200, "revoked "+body.ULID, "ok")
	default:
		e.ackAdmin(verb, body.CorrelationID, 422, fmt.Sprintf("unknown admin verb %q", verb), "invalid")
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

// ackAdmin publishes the execution outcome into the node's own commands
// stream (own frame — uplink mount-insert rebuilds the path hop by hop,
// design §6) and counts the metric.
func (e *Engine) ackAdmin(verb, corr string, code int, msg, result string) {
	e.metrics.AdminCmd(verb, result)
	topic := "colca/v1/_Ack/" + e.cfg.ULID + "/" + verb
	payload, err := json.Marshal(map[string]any{"correlation_id": corr, "result_code": code, "message": msg})
	if err != nil {
		e.log.Error("cmdadmin: ack encode failed", "verb", verb, "correlation_id", corr, "err", err)
		return
	}
	p, err := uns.Parse(topic)
	if err != nil {
		e.log.Error("cmdadmin: ack topic invalid", "topic", topic, "err", err)
		return
	}
	if _, err := e.persist(uns.ClassAck, p, topic, payload); err != nil {
		e.log.Error("cmdadmin: ack persist failed", "topic", topic, "correlation_id", corr, "err", err)
		return
	}
	e.log.Info("cmdadmin executed", "verb", verb, "correlation_id", corr, "result_code", code, "message", msg)
}

package enroll

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// commandBody holds the fields of every enrollment verb of _CmdAdmin. The
// command envelope's expires_at is the command's own expiry, so a
// pre-approval's expiry travels as valid_until.
type commandBody struct {
	Fingerprint string              `json:"fingerprint"`
	Element     string              `json:"element"`
	Mount       string              `json:"mount"`
	Name        string              `json:"name"`
	Reason      string              `json:"reason"`
	ULID        string              `json:"ulid"`
	ID          string              `json:"id"`
	Match       uns.EnrollmentMatch `json:"match"`
	ValidUntil  string              `json:"valid_until"`
	Uses        int                 `json:"uses"`
	Note        string              `json:"note"`
}

// Decide executes the enrollment verbs of _CmdAdmin at this node, the node
// that holds the request or pre-approval: approve, reject, block, unblock,
// preapprove, unpreapprove and request-key-change. handled is false for any
// other verb. The person is the command's verified actor.
func (m *Manager) Decide(ctx uns.CommandContext, verb string, payload []byte) (int, string, string, bool) {
	switch verb {
	case "approve", "reject", "block", "unblock", "preapprove", "unpreapprove", "request-key-change":
	default:
		return 0, "", "", false
	}
	var b commandBody
	if err := json.Unmarshal(payload, &b); err != nil {
		return 422, verb + ": unreadable payload: " + err.Error(), "invalid", true
	}
	a := Actor{ID: ctx.ActorID, Label: ctx.ActorLabel, Kind: ctx.ActorKind}
	if a.Label == "" && ctx.Actor != nil {
		a.Label = ctx.Actor.Username
	}
	var err error
	message := verb
	switch verb {
	case "approve":
		err = m.Approve(b.Fingerprint, b.Element, b.Mount, b.Name, a)
		message = "approved " + b.Fingerprint
	case "reject":
		err = m.Reject(b.Fingerprint, b.Reason, a)
		message = "rejected " + b.Fingerprint
	case "block":
		err = m.Block(b.Fingerprint, b.Reason, b.ULID, a)
		message = "blocked " + b.Fingerprint
	case "unblock":
		err = m.Unblock(b.Fingerprint, a)
		message = "unblocked " + b.Fingerprint
	case "preapprove":
		in := PreapprovalInput{ID: b.ID, Match: b.Match, Element: b.Element, Mount: b.Mount, Name: b.Name, Uses: b.Uses, Note: b.Note}
		if b.ValidUntil != "" {
			t, perr := time.Parse(time.RFC3339, b.ValidUntil)
			if perr != nil {
				return 422, "preapprove: valid_until must be RFC 3339", "invalid", true
			}
			in.ExpiresAt = t
		}
		var p uns.EnrollmentPreapproval
		p, err = m.Preapprove(in, a)
		message = "pre-approved " + p.ID
	case "unpreapprove":
		err = m.Unpreapprove(b.ID, a)
		message = "withdrew pre-approval " + b.ID
	case "request-key-change":
		err = m.RequestKeyChange(b.ULID, a)
		message = "key change requested for " + b.ULID
	}
	if err != nil {
		code, result := commandStatus(err)
		return code, fmt.Sprintf("%s: %v", verb, err), result, true
	}
	return 200, message, "ok", true
}

func commandStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ErrNotFound):
		return 404, "not_found"
	case errors.Is(err, ErrConflict), errors.Is(err, ErrBelowPolicy):
		return 409, "conflict"
	case errors.Is(err, ErrInvalid):
		return 422, "invalid"
	}
	return 500, "error"
}

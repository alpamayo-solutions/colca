package repl

import (
	"errors"
	"net/http"
	"time"
)

// UplinkState is the coarse condition of this node's uplink to its parent
// (colca-node design §3.1/§7 gap 4): what `chaski.Node.status()` derives its
// richer vocabulary from. It says nothing a caller could not read out of the
// "first contact"/"downlink fetch failed" log lines, but a Python caller
// cannot poll logs, and colcad already knows the answer — so /healthz serves
// it instead of a second, invented signal.
type UplinkState string

const (
	// UplinkConnecting: no successful reply from the parent yet on this
	// attempt (still dialing, DNS failing, or the parent unreachable) — the
	// state from NewClient until the first hello succeeds, and again after
	// any non-401 failure.
	UplinkConnecting UplinkState = "connecting"
	// UplinkUnauthorized: the parent answered but refused this node's key
	// (HTTP 401) — it has not enrolled this node yet, or pinned a different
	// key. Enrolling the node at the parent is the fix either way.
	UplinkUnauthorized UplinkState = "unauthorized"
	// UplinkConnected: the last hello or downlink poll succeeded.
	UplinkConnected UplinkState = "connected"
)

// Status is a snapshot of a Client's uplink condition, safe to read
// concurrently with RunUplink/RunDownlink (Client.Status uses an atomic.Value
// under the hood — no lock shared with the hot path).
type Status struct {
	State UplinkState `json:"state"`
	// Since is when the CURRENT state began, UTC. Zero only before the first
	// transition is recorded (never observable through Client.Status, which
	// seeds it in NewClient).
	Since time.Time `json:"since"`
}

// Status is this client's current uplink condition.
func (c *Client) Status() Status {
	if v, ok := c.status.Load().(Status); ok {
		return v
	}
	return Status{State: UplinkConnecting, Since: time.Now().UTC()}
}

// setStatus records a transition, but only if the state actually changed —
// so Since reflects when the CURRENT state began, not the timestamp of the
// most recent poll (which would make it useless: every successful poll would
// reset it).
func (c *Client) setStatus(state UplinkState) {
	current, _ := c.status.Load().(Status)
	if current.State == state {
		return
	}
	c.status.Store(Status{State: state, Since: time.Now().UTC()})
}

// classifyUplinkErr turns a hello/downlink failure into the state it implies:
// a parent that answered 401 refused this node's key specifically (§7's
// "unauthorized"); every other failure — dial error, timeout, 5xx, a route
// that does not exist — is transport-shaped, not identity-shaped, so it maps
// to "still connecting".
func classifyUplinkErr(err error) UplinkState {
	var re *replError
	if errors.As(err, &re) && re.Status == http.StatusUnauthorized {
		return UplinkUnauthorized
	}
	return UplinkConnecting
}

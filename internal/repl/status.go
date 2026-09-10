package repl

import (
	"errors"
	"net/http"
	"time"
)

// UplinkState is the condition of this node's link to its parent, as reported
// on /healthz. chaski.Node.status() derives its own states from it.
type UplinkState string

const (
	// UplinkConnecting: no successful reply from the parent yet, from NewClient
	// until the first hello succeeds and again after any failure other than 401.
	UplinkConnecting UplinkState = "connecting"
	// UplinkUnauthorized: the parent refused this node's key (HTTP 401), because it
	// has not enrolled the node or pinned another key.
	UplinkUnauthorized UplinkState = "unauthorized"
	// UplinkConnected: the last hello or downlink poll succeeded.
	UplinkConnected UplinkState = "connected"
)

// Status is a snapshot of a Client's uplink condition, safe to read while the
// replication loops run.
type Status struct {
	State UplinkState `json:"state"`
	// Since is when the current state began, in UTC. It is zero only before the
	// first transition, which NewClient records.
	Since time.Time `json:"since"`
}

// Status is this client's current uplink condition.
func (c *Client) Status() Status {
	if v, ok := c.status.Load().(Status); ok {
		return v
	}
	return Status{State: UplinkConnecting, Since: time.Now().UTC()}
}

// setStatus records a transition only if the state changed, so Since marks the
// start of the current state rather than the latest poll.
func (c *Client) setStatus(state UplinkState) {
	current, _ := c.status.Load().(Status)
	if current.State == state {
		return
	}
	c.status.Store(Status{State: state, Since: time.Now().UTC()})
}

// classifyUplinkErr maps a hello or downlink failure to a state: a 401 means the
// parent refused this node's key; anything else (dial error, timeout, 5xx,
// missing route) means still connecting.
func classifyUplinkErr(err error) UplinkState {
	var re *replError
	if errors.As(err, &re) && re.Status == http.StatusUnauthorized {
		return UplinkUnauthorized
	}
	return UplinkConnecting
}

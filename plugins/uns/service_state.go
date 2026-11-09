package uns

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ServiceStateContract is one service's private state: what a stateless
// service needs back after a restart, such as an evaluator's checkpoint or the
// work it has accepted and not finished. It is not for app or plant data,
// which belong in contracts other readers are granted.
//
// The topic is
//
//	{root}/v1/_ServiceState/{node}/{service-position}/{key…}
//
// where the service position is the one its _ServiceDetails sits under
// (ServiceContext: its mount, then its name) and key is the service's own,
// at least one segment.
//
// What sets it apart from every other state contract:
//
//   - Only the service at that position writes it (and the admin door). A
//     write grant over the path does not, nor does any other service.
//   - Only that service reads it (and the admin door): /kv, /kv/lookup and
//     /fetch leave every other caller's view of it empty. Element read grants
//     never reach it.
//   - It is never on the MQTT bus, so no subscription filter can show it.
//   - It does not replicate up the tree: the parent has no use for a child
//     service's checkpoint.
//   - It lands on its own stream, service_state, short-lived and never
//     uplinked, so a checkpoint wakes no consumer of the entities stream.
//     Its current value is the KV entry, which outlives the stream's records.
//   - It is retired with the service: a revoked identity's _ServiceState goes
//     with its _ServiceDetails.
//
// The payload is the service's own JSON object; colca requires only a
// version (a positive integer, for the service's own format) and bounds its
// size like any record's. An empty payload retires the key.
const ServiceStateContract = "_ServiceState"

// ServiceStateOwnedBy reports whether path is a key of the service e: e is a
// local service and path lies strictly below its position. ok is false when
// e's bound element does not resolve here: such a service owns nothing until
// it does.
func ServiceStateOwnedBy(sc Scope, e *Entry, path string) bool {
	if !e.IsLocal() || e.Name == "" {
		return false
	}
	mount := ""
	if e.Element != "" {
		if sc == nil {
			return false
		}
		resolved, ok := sc.PathOf(e.Element)
		if !ok {
			return false
		}
		mount = resolved
	}
	position := strings.Join(ServiceContext(mount, e.Name), "/")
	return strings.HasPrefix(path, position+"/") && len(path) > len(position)+1
}

// validateServiceState checks the one field colca reads: version.
func validateServiceState(payload []byte) error {
	var record map[string]any
	if err := json.Unmarshal(payload, &record); err != nil {
		return fmt.Errorf("%s: payload must be a JSON object: %w", ServiceStateContract, err)
	}
	if record == nil {
		return fmt.Errorf("%s: payload must be a JSON object", ServiceStateContract)
	}
	version, ok := record["version"].(float64)
	if !ok || version < 1 || version != float64(int64(version)) {
		return fmt.Errorf("%s: field %q must be a positive integer", ServiceStateContract, "version")
	}
	return nil
}

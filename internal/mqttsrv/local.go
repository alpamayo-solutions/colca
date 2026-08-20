package mqttsrv

import (
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The local door: an unpublished plaintext listener reachable only from inside
// the deployment's own network. Reaching it IS the credential (local-service-
// trust design §4) — there is no key to present and nothing to verify.
//
// The CONNECT username is a NAME, not a secret: it selects a registry entry and
// therefore a scope. The `mount` user property beside it is the placement
// declaration, read only when the entry is created.
const listenerLocal = "local"

func isLocalListener(cl *mqtt.Client) bool { return cl.Net.Listener == listenerLocal }

// mountDeclaration reads the CONNECT `mount` user property (MQTT v5). Absent on
// a v3 client, which simply means "no declaration" — that service binds to the
// node's root.
func mountDeclaration(pk packets.Packet) string {
	for _, up := range pk.Properties.User {
		if up.Key == "mount" {
			return up.Val
		}
	}
	return ""
}

// authenticateLocal admits any CONNECT that names a non-empty username: the
// network position already proved the door, so there is nothing left to prove
// about the identity beyond resolving it to a registry entry.
//
// Two checks refuse a name that resolves to a KEYED identity (a machine or a
// child node), and both are needed — they catch it through different paths:
//
//   - h.reg.Get(name): a name that collides with some OTHER entry's ULID.
//     Register alone would not catch this — its own uniqueness check is
//     scoped to the byName index (registry.go's Enroll), a different key
//     space than byID — so presenting a machine's ULID as a "name" here would
//     otherwise fall straight through to self-registration and silently mint
//     an unrelated kind=local entry.
//   - entry.MayUseDoor(uns.DoorLocal), checked AFTER Register returns:
//     uns.Entry.Validate permits a `name` field on KindMachine and KindNode
//     too, and Manager.Enroll indexes ANY non-empty Name into byName
//     regardless of kind (registry.go). So an operator who gives a machine or
//     child node a friendly `name` makes it resolvable by ByName — and
//     Register's own idempotent-reconnect path (registry/local.go: "entry
//     exists? return it, no kind check") would hand that keyed identity's
//     ULID to a certless local session. This is the check that actually
//     closes that hole: it asks the domain a question rather than
//     enumerating the ways a name might resolve to something keyed, so it
//     also subsumes the Get(name) case above and any future kind.
//
// A name is how the local door finds ITS OWN entries; a keyed identity is
// found by key, and letting a name double for one on this door would blur a
// boundary that is supposed to stay sharp — the local door hands out no
// identity whose whole point is that it proves itself with a key.
func (h *colcaHook) authenticateLocal(cl *mqtt.Client, pk packets.Packet) bool {
	name := string(pk.Connect.Username)
	if name == "" {
		h.log.Warn("local door rejected: no name given")
		h.metrics.AuthReject(metrics.DoorLocal, metrics.AuthNoName)
		h.auditDenied("authenticate", metrics.AuthNoName, metrics.DoorLocal, nil, nil)
		return false
	}
	if e, ok := h.reg.Get(name); ok && !e.MayUseDoor(uns.DoorLocal) {
		h.log.Warn("local door rejected: name collides with a keyed identity's ulid", "name", name, "kind", e.Kind)
		h.metrics.AuthReject(metrics.DoorLocal, metrics.AuthKind)
		h.auditDenied("authenticate", metrics.AuthKind, metrics.DoorLocal, e, nil)
		return false
	}
	entry, err := h.reg.Register(name, mountDeclaration(pk))
	if err != nil {
		h.log.Warn("local door rejected: registration failed", "name", name, "err", err)
		h.metrics.AuthReject(metrics.DoorLocal, metrics.AuthRegister)
		h.auditDenied("authenticate", metrics.AuthRegister, metrics.DoorLocal, nil, nil)
		return false
	}
	if !entry.MayUseDoor(uns.DoorLocal) {
		h.log.Warn("local door rejected: name resolves to a keyed identity", "name", name, "ulid", entry.ULID, "kind", entry.Kind)
		h.metrics.AuthReject(metrics.DoorLocal, metrics.AuthKind)
		h.auditDenied("authenticate", metrics.AuthKind, metrics.DoorLocal, entry, nil)
		return false
	}
	// mochi carries Username to OnPublish/OnACLCheck, and every later lookup is
	// by ULID — so the session speaks the same identity language as a machine's.
	cl.Properties.Username = []byte(entry.ULID)
	h.log.Debug("local service attached", "name", name, "ulid", entry.ULID)
	return true
}

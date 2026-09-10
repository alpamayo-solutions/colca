package mqttsrv

import (
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The local door is a plaintext listener reachable only inside the deployment;
// reaching it is the credential. The CONNECT username is a name, not a secret: it
// selects a registry entry and so a scope. The mount user property is read only
// when the entry is created.
const listenerLocal = "local"

func isLocalListener(cl *mqtt.Client) bool { return cl.Net.Listener == listenerLocal }

// mountDeclaration reads the CONNECT mount user property (MQTT 5). A v3 client has
// none, and the service binds to the node's root.
func mountDeclaration(pk packets.Packet) string {
	for _, up := range pk.Properties.User {
		if up.Key == "mount" {
			return up.Val
		}
	}
	return ""
}

// authenticateLocal admits any CONNECT with a non-empty username, resolved to a
// registry entry.
//
// It never hands out a keyed identity (a machine or child node) by name.
// h.reg.Get(name) catches a name that is another entry's ULID.
// MayUseDoor(DoorLocal) after Register catches a machine that was given a friendly
// name, which Register would otherwise return as is.
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
	// mochi carries Username to OnPublish and OnACLCheck, and every later lookup is by
	// ULID, as for a machine.
	cl.Properties.Username = []byte(entry.ULID)
	h.log.Debug("local service attached", "name", name, "ulid", entry.ULID)
	return true
}

// MQTT 5 re-authentication on the human doors. A client that sends the
// authentication method AuthMethodToken in its CONNECT (the token itself still
// goes in the password) gets the method back in the CONNACK, which tells it this
// node renews tokens in place. Before its token runs out it sends an AUTH packet,
// reason 0x19 (re-authenticate), with the same method and the new token as
// authentication data. The node verifies it exactly as at CONNECT, requires the
// same subject, and moves the session's grants and expiry to it; subscriptions
// stay. A refused token ends the connection with a DISCONNECT that says why.

package mqttsrv

import (
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// AuthMethodToken is the MQTT 5 authentication method of a human session that
// renews its token over AUTH.
const AuthMethodToken = "colca-token"

// OnPacketEncode names the authentication method in the CONNACK of a human
// session that asked for it, as MQTT 5 requires of a server that accepts one.
func (h *colcaHook) OnPacketEncode(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	if pk.FixedHeader.Type == packets.Connack && pk.ReasonCode == packets.CodeSuccess.Code &&
		isHumanListener(cl) && cl.Properties.Props.AuthenticationMethod == AuthMethodToken {
		pk.Properties.AuthenticationMethod = AuthMethodToken
	}
	return pk
}

// OnAuthPacket handles a client-initiated re-authentication. Returning a reason
// code of 0x80 or above makes mochi send it in a DISCONNECT and close the
// connection.
func (h *colcaHook) OnAuthPacket(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	connected := cl.Properties.Props.AuthenticationMethod
	if !isHumanListener(cl) || connected == "" {
		// AUTH without an authentication method in the CONNECT is a protocol error.
		return pk, packets.ErrProtocolViolation
	}
	if pk.Properties.AuthenticationMethod != connected {
		return pk, packets.ErrBadAuthenticationMethod // [MQTT-4.12.1-1]
	}
	if pk.ReasonCode != packets.CodeReAuthenticate.Code {
		// The node never asks to continue an exchange, so 0x18 has nothing to answer.
		return pk, packets.ErrProtocolViolation
	}
	current, ok := h.humans.get(cl.ID)
	if !ok {
		return pk, packets.ErrNotAuthorized
	}
	v, reason, err := h.ver.VerifyForScope(string(pk.Properties.AuthenticationData), uns.ScopeBrokerMQTT)
	if err != nil {
		h.log.Warn("human re-authentication rejected", "sub", current.sub, "reason", reason, "err", err)
		h.metrics.AuthReject(metrics.DoorMQTT, reason)
		h.auditDenied("reauthenticate", reason, metrics.DoorMQTT, current.entry, nil)
		return pk, packets.ErrNotAuthorized
	}
	if v.Sub != current.sub {
		h.log.Warn("human re-authentication rejected: another subject", "sub", current.sub, "new_sub", v.Sub)
		h.metrics.AuthReject(metrics.DoorMQTT, metrics.AuthSubjectChanged)
		h.auditDenied("reauthenticate", metrics.AuthSubjectChanged, metrics.DoorMQTT, v.Entry, nil)
		return pk, packets.ErrNotAuthorized
	}
	h.humans.put(cl.ID, sessionFor(v))
	h.log.Debug("human re-authenticated", "sub", v.Sub, "exp", v.Exp, "listener", cl.Net.Listener)
	err = cl.WritePacket(packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Auth},
		ReasonCode:  packets.CodeSuccess.Code,
		Properties:  packets.Properties{AuthenticationMethod: connected},
	})
	return pk, err
}

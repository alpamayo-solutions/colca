// Undelivered-command observability (audit finding, no redelivery built:
// the cmdadmin design §5/§11 explicitly defers
// delivery-failure machinery, and the exposed path has no production
// consumer yet — this file makes the failure VISIBLE, nothing more).
//
// The gap: a live _CmdParam addressed to an enrolled machine is persisted to
// the commands stream and mirrored onto the local MQTT bus
// (persistTSAttributed), but if the target machine's MQTT session is not
// currently connected — broker restarted, machine reconnecting, first
// connect to a fresh broker process — the publish reaches zero subscribers.
// No PUBACK failure exists for this (MQTT has none for "nobody was
// listening"), no ack ever returns, and the issuer learns only at its own
// ack-timeout/TTL expiry. Before this file, that drop left no trace at all.
package engine

import (
	"encoding/json"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// observeCommandDelivery is called from persistTSAttributed immediately after
// the record was mirrored onto the local bus. It counts
// colca_command_undelivered_total and logs a warning for the one case this
// signal exists to catch: a live command addressed to an identity THIS node
// can actually deliver to over the local MQTT bus — a machine enrolled here —
// that just had zero live SUBSCRIPTIONS on its topic.
//
// Read that literally, not as "the bytes were lost": e.hasSubscriber
// (mqttsrv.HasLocalSubscriber) answers subscription existence, not
// byte-level delivery confirmation — see its doc comment for exactly what
// it can and cannot see (a per-client write mochi attempts and fails is
// invisible to it, swallowed inside mochi at Debug). This counter can
// therefore undercount real delivery failures; it cannot overcount them.
//
// Three things deliberately do NOT trip this counter, each guarded below:
//
//   - No broker, or the subscriber lookup was never wired (e.hasSubscriber
//     == nil): nothing can be claimed either way, so nothing is counted.
//     This also covers every existing unit test that builds an *Engine
//     without a broker — they see zero calls to this metric, not false
//     positives.
//   - A command this node has no MQTT door for. Level 4 of a _Cmd* topic is
//     the TARGET identity, and it is the one part of a command topic that is
//     never rewritten on the way down (only the path is: the mount prefix is
//     stripped hop by hop), so the same lookup asks the right question at
//     every node the command passes through: "is the target something I can
//     hand to over my own bus?". Three answers say no, and all three are
//     healthy. (a) The target is not in this node's registry at all: the
//     command is transiting toward a descendant, and it is persisted and
//     mirrored at EVERY ancestor on the way (IngestDownlinkAttributed →
//     persistTSAttributed), where zero subscribers is simply what a relay
//     looks like — the target is reached over the replication door, not this
//     bus. (b) The target is a kind=node child: same thing at the last hop, a
//     child node is fed over replication (9443) and never subscribes here.
//     (c) The target is this node itself: _CmdConfigure, _CmdEdit and
//     _CmdAdmin execute in-process via maybeExec, which runs right after this
//     same persist call returns, and no machine is meant to listen on its own
//     node's command topic. That case needs no branch of its own — a node
//     holds no kind=machine entry for itself, so it falls out of the same
//     question. A kind=local service is a no too, and for a reason worth
//     writing down since it DOES share this bus: a local service is found by
//     the name it presents and never learns the ULID the node mints for it
//     (uns.LocalCursorPrefix), so nothing can address a command to one at
//     level 4. MayUseDoor(DoorMQTT) — "is this a machine" — is therefore the
//     whole question, and adding DoorLocal beside it would be a branch no
//     traffic can reach.
//   - An already-expired command: arriving expired is not a delivery
//     failure, it is an issuer that waited too long before the command was
//     even persisted. uns.CommandStillLive is the one place this "is it
//     still live" question is answered — the same predicate move-drain
//     completion (repl/drain.go) uses, so the two can never disagree about
//     the same expires_at field.
func (e *Engine) observeCommandDelivery(class uns.Class, p uns.Parsed, topic string, payload []byte) {
	if !uns.IsCommand(class) || e.hasSubscriber == nil {
		return
	}
	target, ok := e.ids.Get(p.NodeID)
	if !ok || !target.MayUseDoor(uns.DoorMQTT) {
		return // not deliverable over this node's bus — see above
	}
	if !uns.CommandStillLive(payload, e.AuthoritativeNow().UnixMilli()) {
		return // arrived already expired — not a delivery failure
	}
	if e.hasSubscriber(topic) {
		return // reached at least one live subscriber
	}
	e.metrics.CommandUndelivered()
	e.log.Warn("command published to the local bus had no live subscription — "+
		"no ack will follow unless the target reconnects and the issuer reissues",
		"topic", topic, "correlation_id", correlationIDOf(payload))
}

// correlationIDOf extracts correlation_id for the warning above, best-effort:
// uns.Validate already requires every _Cmd* payload to carry one, so a decode
// failure here cannot happen for real data. An empty string is still a useful
// log line — it says "look at the topic" rather than nothing at all.
func correlationIDOf(payload []byte) string {
	var env cmdEnvelope
	_ = json.Unmarshal(payload, &env)
	return env.CorrelationID
}

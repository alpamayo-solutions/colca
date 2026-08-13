// Package uns is the Colca-specific plugin layer: topic grammar, contract
// classes, mount insert/strip and payload validation for the `colca/#` namespace.
//
// It has no dependencies beyond the Go standard library and must never be
// imported *by* the core — the core stays generic, the domain knowledge lives
// here. Keep it that way.
package uns

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Class is the routing class of a contract; it decides the stream a record
// lands in and the direction it flows between nodes.
type Class int

const (
	ClassNone   Class = iota
	ClassData         // _Metric …    write: owner (level4 == identity)
	ClassEntity       // _EdgeNode, _SystemElement, _Signal
	ClassCmd          // _Cmd*        write: ancestors/admin, flows down
	ClassAck          // _Ack         write: owner, flows up
)

// Parsed is a decomposed UNS topic: colca/v1/_Contract/{node-id}/{path…}
type Parsed struct {
	Prefix, Version, Contract, NodeID, Path string
}

// IsUns reports whether the topic belongs to the uns namespace.
func IsUns(topic string) bool { return strings.HasPrefix(topic, "colca/") }

// Parse decomposes an UNS topic. It requires at least 5 segments (so there is
// always a non-empty hierarchy path) and a _Contract at segment index 2.
func Parse(topic string) (Parsed, error) {
	seg := strings.Split(topic, "/")
	if len(seg) < 5 {
		return Parsed{}, fmt.Errorf("uns grammar: need >=5 segments, got %d in %q", len(seg), topic)
	}
	if !strings.HasPrefix(seg[2], "_") {
		return Parsed{}, fmt.Errorf("uns grammar: level 3 must be _Contract, got %q", seg[2])
	}
	return Parsed{
		Prefix:   seg[0],
		Version:  seg[1],
		Contract: seg[2],
		NodeID:   seg[3],
		Path:     strings.Join(seg[4:], "/"),
	}, nil
}

// ClassOf maps a contract name to its routing class. Concrete names are matched
// before the _Cmd prefix rule, so an exact contract can never be swallowed by
// the prefix; every _Cmd* contract is a command and never falls through to
// ClassNone.
func ClassOf(contract string) Class {
	switch {
	case contract == "_Metric":
		return ClassData
	case contract == "_EdgeNode" || contract == "_SystemElement" || contract == "_Signal":
		return ClassEntity
	case contract == "_Ack":
		return ClassAck
	case strings.HasPrefix(contract, "_Cmd"):
		return ClassCmd
	}
	return ClassNone
}

// StreamFor maps a class to the persistent stream that stores it.
func StreamFor(c Class) string {
	switch c {
	case ClassData:
		return "metrics"
	case ClassEntity:
		return "entities"
	case ClassCmd, ClassAck:
		return "commands"
	}
	return ""
}

// MountInsert inserts the mount name directly after segment 4 (node-id), i.e.
// at the head of the hierarchy path — the uplink rewrite done on every hop.
func MountInsert(topic, mount string) string {
	seg := strings.SplitN(topic, "/", 5)
	if len(seg) < 5 {
		return topic
	}
	return strings.Join([]string{seg[0], seg[1], seg[2], seg[3], mount + "/" + seg[4]}, "/")
}

// MountStrip removes the mount prefix from the hierarchy part — the exact
// inverse of MountInsert, done on every downlink hop. ok=false when the path
// does not start with the mount, in which case the record belongs to a foreign
// mount and must not be delivered.
func MountStrip(topic, mount string) (string, bool) {
	seg := strings.SplitN(topic, "/", 5)
	if len(seg) < 5 {
		return topic, false
	}
	rest, found := strings.CutPrefix(seg[4], mount+"/")
	if !found {
		return topic, false
	}
	return strings.Join([]string{seg[0], seg[1], seg[2], seg[3], rest}, "/"), true
}

// Validate applies minimal per-contract schema checks (hand-rolled stand-in for
// the generated schema bundle — same enforcement point, swappable later).
// Unknown contracts are rejected: that is the point of a validated namespace.
func Validate(contract string, payload []byte) error {
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return fmt.Errorf("%s: payload is not valid JSON: %w", contract, err)
	}
	reqNum := func(k string) error {
		if _, ok := m[k].(float64); !ok {
			return fmt.Errorf("%s: field %q must be a number", contract, k)
		}
		return nil
	}
	reqStr := func(k string) error {
		if v, ok := m[k].(string); !ok || v == "" {
			return fmt.Errorf("%s: field %q must be a non-empty string", contract, k)
		}
		return nil
	}
	switch {
	case contract == "_Metric":
		return reqNum("v")
	case contract == "_Ack":
		if err := reqStr("correlation_id"); err != nil {
			return err
		}
		return reqNum("result_code")
	case contract == "_EdgeNode" || contract == "_SystemElement" || contract == "_Signal":
		return reqStr("ulid")
	case strings.HasPrefix(contract, "_Cmd"):
		if err := reqStr("correlation_id"); err != nil {
			return err
		}
		return reqNum("expires_at")
	}
	return fmt.Errorf("unknown contract %q — validated namespace rejects unknown contracts", contract)
}

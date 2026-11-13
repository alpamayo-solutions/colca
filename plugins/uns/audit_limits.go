package uns

import "unicode/utf8"

// auditFieldLimits is how many characters each text field of an `_AuditEvent`
// may have. The limits are declared in the payload contract (contracts/
// payload.py: AuditEvent) and reach the doors in the schema bundle. They are
// repeated here for the two places that cannot ask the bundle: the node's own
// audit writer, which appends without passing a door, and the built-in rules a
// node without a bundle runs. A test compares this table with the generated
// bundle in both directions, so it cannot drift from the contract.
var auditFieldLimits = map[string]int{
	"event_id":       255,
	"operation":      512,
	"actor_id":       255,
	"actor_label":    255,
	"entity_type":    255,
	"entity_id":      255,
	"correlation_id": 255,
	"reason_code":    255,
}

// AuditFieldLimits returns a copy of the limits, by field name.
func AuditFieldLimits() map[string]int {
	out := make(map[string]int, len(auditFieldLimits))
	for field, limit := range auditFieldLimits {
		out[field] = limit
	}
	return out
}

// BoundAuditField cuts value to what the contract allows for field. The node's
// own audit writer uses it: a denial names what the caller sent (a contract, a
// topic path), which can be any length, and a record over the limit is one the
// read side cannot store. A cut record still says who was refused what; a
// field without a limit is returned as it is.
func BoundAuditField(field, value string) string {
	limit, bounded := auditFieldLimits[field]
	if !bounded || len(value) <= limit {
		// Bytes are never fewer than characters, so this is inside the limit.
		return value
	}
	seen := 0
	for at := range value {
		if seen == limit {
			return value[:at]
		}
		seen++
	}
	return value
}

// auditFieldTooLong names the first text field of an audit payload that is
// over its limit, in a fixed order so the answer does not depend on map order.
func auditFieldTooLong(m map[string]any) (field string, limit int, found bool) {
	for _, field := range []string{
		"event_id", "operation", "actor_id", "actor_label",
		"entity_type", "entity_id", "correlation_id", "reason_code",
	} {
		if value, ok := m[field].(string); ok && utf8.RuneCountInString(value) > auditFieldLimits[field] {
			return field, auditFieldLimits[field], true
		}
	}
	return "", 0, false
}

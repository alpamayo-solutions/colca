package uns

import "fmt"

// FieldError is a record refused because one of its fields lies outside what
// its contract allows: a string longer than its limit, a number out of range,
// an id of the wrong shape. The limit has exactly one definition, the payload
// contract (contracts/ payload.py: MaxLength, Bounds, Pattern), which the
// schema bundle carries to the node. The core's validator answers with this
// type, so an executor that composed the record can name the field instead of
// failing the commit; nothing here or in the core re-states a limit.
type FieldError struct {
	// Field is the dotted path of the refused value in the payload ("unit",
	// "bind_intent.variable"). Empty when the record as a whole is refused.
	Field string
	// Keyword is the schema keyword that refused it: maxLength, minimum,
	// maximum, pattern, type, enum, required, ...
	Keyword string
	// Limit is the keyword's bound where it has one ("50", "0"), else "".
	Limit string
	// Err is the validator's own description, kept for logs and doors.
	Err error
}

func (e *FieldError) Error() string { return e.Err.Error() }

func (e *FieldError) Unwrap() error { return e.Err }

// Refusal is how a command executor answers a FieldError:
//
//	invalid_field: <field>: <keyword>[ <limit>]
//
// The api turns it into a per-field message, so the shape is a wire contract
// of its own, pinned by vectors/field_refusal.json on both sides.
func (e *FieldError) Refusal() string {
	if e.Limit == "" {
		return fmt.Sprintf("invalid_field: %s: %s", e.Field, e.Keyword)
	}
	return fmt.Sprintf("invalid_field: %s: %s %s", e.Field, e.Keyword, e.Limit)
}

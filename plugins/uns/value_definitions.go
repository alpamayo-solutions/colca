package uns

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// The value definition rules of colca_data_contracts.value_definitions, pinned
// by vectors/value_definitions.json: which data types take a definition and
// how their keys are written, how many values one may name and how long a
// meaning may be.
var valueDefinitionKeyKinds = map[string]string{
	"int": "int", "integer": "int", "boolean": "boolean", "string": "string",
}

const (
	valueDefinitionMaxEntries = 500
	valueDefinitionMaxText    = 120
)

var valueDefinitionIntKey = regexp.MustCompile(`^(0|-?[1-9][0-9]*)$`)

// valueDefinitionKeyIsCanonical reports whether key is a value of kind written
// the one canonical way.
func valueDefinitionKeyIsCanonical(kind, key string) bool {
	switch kind {
	case "int":
		if !valueDefinitionIntKey.MatchString(key) {
			return false
		}
		_, err := strconv.ParseInt(key, 10, 64)
		return err == nil
	case "boolean":
		return key == "true" || key == "false"
	default:
		count := utf8.RuneCountInString(key)
		return count > 0 && count <= valueDefinitionMaxText
	}
}

// valueDefinitionsProblem judges a definition for a signal or slot of
// dataType. It answers the first broken rule as a FieldError, judging the
// whole definition first and then each key in sorted order, or nil when the
// definition is valid. An absent or empty definition is valid for every data
// type.
func valueDefinitionsProblem(dataType string, value any) *FieldError {
	refuse := func(keyword, limit, detail string) *FieldError {
		return &FieldError{
			Field: "value_definitions", Keyword: keyword, Limit: limit,
			Err: fmt.Errorf("_Signal: value_definitions %s", detail),
		}
	}
	if value == nil {
		return nil
	}
	definitions, ok := value.(map[string]any)
	if !ok {
		return refuse("type", "", "must be an object")
	}
	if len(definitions) == 0 {
		return nil
	}
	kind, ok := valueDefinitionKeyKinds[dataType]
	if !ok {
		return refuse("data_type", "", fmt.Sprintf("are only taken by int, boolean and string signals, not %q", dataType))
	}
	if len(definitions) > valueDefinitionMaxEntries {
		return refuse("maxProperties", strconv.Itoa(valueDefinitionMaxEntries),
			fmt.Sprintf("name %d values, at most %d", len(definitions), valueDefinitionMaxEntries))
	}
	keys := make([]string, 0, len(definitions))
	for key := range definitions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		meaning, ok := definitions[key].(string)
		switch {
		case !ok:
			return refuse("type", "", fmt.Sprintf("%q: the meaning must be text", key))
		case !valueDefinitionKeyIsCanonical(kind, key):
			return refuse("propertyNames", kind, fmt.Sprintf("%q is not a canonical %s value", key, kind))
		case strings.TrimSpace(meaning) == "":
			return refuse("minLength", "1", fmt.Sprintf("%q: the meaning is empty", key))
		case utf8.RuneCountInString(meaning) > valueDefinitionMaxText:
			return refuse("maxLength", strconv.Itoa(valueDefinitionMaxText),
				fmt.Sprintf("%q: the meaning is longer than %d characters", key, valueDefinitionMaxText))
		case meaning != strings.TrimSpace(meaning):
			return refuse("whitespace", "", fmt.Sprintf("%q: the meaning has surrounding whitespace", key))
		}
	}
	return nil
}

// validateSignalValueDefinitionsField checks a _Signal record's
// value_definitions against the record's own data_type.
func validateSignalValueDefinitionsField(m map[string]any) error {
	dataType, _ := m["data_type"].(string)
	if problem := valueDefinitionsProblem(dataType, m["value_definitions"]); problem != nil {
		return problem
	}
	return nil
}

// CheckSignalValueDefinitions refuses a batch holding a _Signal record whose
// value_definitions break a rule for the record's data_type. Every signal a
// batch writes is judged whole, so changing a signal's data_type to one its
// definition does not fit is refused like a bad definition.
func CheckSignalValueDefinitions(batch []StateRecord) error {
	for _, rec := range batch {
		parsed, err := Parse(rec.Topic)
		if err != nil || parsed.Contract != "_Signal" || len(rec.Payload) == 0 {
			continue
		}
		var m map[string]any
		if json.Unmarshal(rec.Payload, &m) != nil {
			continue // not a record: the schema check refuses it
		}
		if err := validateSignalValueDefinitionsField(m); err != nil {
			return err
		}
	}
	return nil
}

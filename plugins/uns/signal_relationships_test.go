package uns

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const signalRelationshipsVectorPath = "../../contracts/src/colca_data_contracts/vectors/signal_relationships.json"

type relationshipVectors struct {
	Access           []string `json:"access"`
	NumericDataTypes []string `json:"numeric_data_types"`
	Rules            map[string]struct {
		MaxSourcesPerTarget *int   `json:"max_sources_per_target"`
		Chains              bool   `json:"chains"`
		DataTypes           string `json:"data_types"`
	} `json:"rules"`
	Cases []struct {
		Name  string `json:"name"`
		Valid bool   `json:"valid"`
		Ends  map[string]struct {
			DataType      *string     `json:"data_type"`
			Relationships [][2]string `json:"relationships"`
		} `json:"ends"`
	} `json:"cases"`
}

func readRelationshipVectors(t *testing.T) relationshipVectors {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(signalRelationshipsVectorPath))
	if err != nil {
		t.Fatalf("read relationship vectors: %v", err)
	}
	var vectors relationshipVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("decode relationship vectors: %v", err)
	}
	if len(vectors.Cases) == 0 || len(vectors.Rules) == 0 {
		t.Fatal("relationship vectors carry no rules or cases")
	}
	return vectors
}

// The Go rule table is the Python one, which the contracts suite checks
// against its definition.
func TestRelationshipRulesMatchTheGoldenVectors(t *testing.T) {
	vectors := readRelationshipVectors(t)
	if len(vectors.Rules) != len(relationshipRules) {
		t.Fatalf("rules: vectors %d, Go %d", len(vectors.Rules), len(relationshipRules))
	}
	for name, want := range vectors.Rules {
		got, ok := relationshipRules[name]
		if !ok {
			t.Fatalf("Go has no rule for %q", name)
		}
		limit := 0
		if want.MaxSourcesPerTarget != nil {
			limit = *want.MaxSourcesPerTarget
		}
		if got.MaxSourcesPerTarget != limit || got.Chains != want.Chains || got.DataTypes != want.DataTypes {
			t.Fatalf("rule %s: Go %+v, vectors %+v", name, got, want)
		}
	}
	if len(vectors.NumericDataTypes) != len(numericDataTypes) {
		t.Fatalf("numeric data types: vectors %v, Go %v", vectors.NumericDataTypes, numericDataTypes)
	}
	for _, name := range vectors.NumericDataTypes {
		if !numericDataTypes[name] {
			t.Fatalf("Go does not count %q as numeric", name)
		}
	}
	if len(vectors.Access) != len(signalAccessValues) {
		t.Fatalf("access: vectors %v, Go %v", vectors.Access, signalAccessValues)
	}
	for _, name := range vectors.Access {
		if !signalAccessValues[name] {
			t.Fatalf("Go does not know access %q", name)
		}
	}
}

func TestRelationshipProblemsMatchTheGoldenCases(t *testing.T) {
	for _, tc := range readRelationshipVectors(t).Cases {
		t.Run(tc.Name, func(t *testing.T) {
			ends := map[string]relationshipEnd{}
			for key, end := range tc.Ends {
				dataType := ""
				if end.DataType != nil {
					dataType = *end.DataType
				}
				links := make([]relationshipLink, len(end.Relationships))
				for i, pair := range end.Relationships {
					links[i] = relationshipLink{Type: pair[0], Target: pair[1]}
				}
				ends[key] = relationshipEnd{DataType: dataType, Links: links}
			}
			problems := relationshipProblems(ends, func(key string) string { return key }, nil, nil)
			if (len(problems) == 0) != tc.Valid {
				t.Fatalf("valid=%v, problems %v", tc.Valid, problems)
			}
		})
	}
}

func linkedSignal(t *testing.T, path, id, dataType string, links ...relationshipLink) StateRecord {
	t.Helper()
	payload := map[string]any{"id": id, "name": path, "data_type": dataType}
	if len(links) > 0 {
		payload["relationships"] = links
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return StateRecord{Topic: "colca/v1/_Signal/n-edge1/press/" + path, Payload: encoded}
}

func asCurrent(records ...StateRecord) []KVRecord {
	current := make([]KVRecord, len(records))
	for i, rec := range records {
		parsed, _ := Parse(rec.Topic)
		current[i] = KVRecord{Topic: rec.Topic, Path: parsed.Path, NodeID: parsed.NodeID, Payload: rec.Payload}
	}
	return current
}

func TestCheckSignalRelationships(t *testing.T) {
	speed := linkedSignal(t, "speed", "sig-speed", "float")
	other := linkedSignal(t, "other", "sig-other", "float")
	actual := relationshipLink{Type: "actual", Target: "sig-speed"}
	cases := []struct {
		name    string
		current []KVRecord
		batch   []StateRecord
		refused string
	}{
		{
			name:    "a setpoint links to an existing actual",
			current: asCurrent(speed),
			batch:   []StateRecord{linkedSignal(t, "sp", "sig-sp", "float", actual)},
		},
		{
			name:  "the target is written in the same batch",
			batch: []StateRecord{speed, linkedSignal(t, "sp", "sig-sp", "float", actual)},
		},
		{
			name:    "the target does not exist",
			batch:   []StateRecord{linkedSignal(t, "sp", "sig-sp", "float", actual)},
			refused: "press/sp: actual target sig-speed does not exist",
		},
		{
			name:    "the batch retires the target it links to",
			current: asCurrent(speed),
			batch:   []StateRecord{{Topic: speed.Topic}, linkedSignal(t, "sp", "sig-sp", "float", actual)},
			refused: "does not exist",
		},
		{
			name:    "a second setpoint names the same actual",
			current: asCurrent(speed, linkedSignal(t, "sp", "sig-sp", "float", actual)),
			batch:   []StateRecord{linkedSignal(t, "sp2", "sig-sp2", "float", actual)},
			refused: "press/speed is the actual target of more than one signal: press/sp, press/sp2",
		},
		{
			name:    "incompatible data types",
			current: asCurrent(linkedSignal(t, "speed", "sig-speed", "boolean")),
			batch:   []StateRecord{linkedSignal(t, "sp", "sig-sp", "float", actual)},
			refused: "not compatible",
		},
		{
			name: "a link left behind does not block an unrelated write",
			current: asCurrent(
				linkedSignal(t, "sp", "sig-sp", "float", relationshipLink{Type: "actual", Target: "sig-gone"}),
			),
			batch: []StateRecord{other},
		},
		{
			name: "rewriting a source with unchanged links is not judged again",
			current: asCurrent(
				linkedSignal(t, "sp", "sig-sp", "float", relationshipLink{Type: "actual", Target: "sig-gone"}),
			),
			batch: []StateRecord{linkedSignal(t, "sp", "sig-sp", "float", relationshipLink{Type: "actual", Target: "sig-gone"})},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckSignalRelationships(tc.current, tc.batch)
			if tc.refused == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var refusal *RelationshipError
			if !errors.As(err, &refusal) || !strings.Contains(err.Error(), tc.refused) {
				t.Fatalf("err = %v, want a RelationshipError containing %q", err, tc.refused)
			}
		})
	}
}

func TestValidateSignalRelationshipsField(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]any
		refused string
	}{
		{"no field", map[string]any{"id": "s"}, ""},
		{"empty list", map[string]any{"id": "s", "relationships": []any{}}, ""},
		{"one link", map[string]any{"id": "s", "relationships": []any{
			map[string]any{"relationship_type": "actual", "target_signal_id": "t"},
		}}, ""},
		{"not a list", map[string]any{"id": "s", "relationships": "actual"}, "must be an array"},
		{"unknown type", map[string]any{"id": "s", "relationships": []any{
			map[string]any{"relationship_type": "derived_from", "target_signal_id": "t"},
		}}, "unknown relationship_type"},
		{"no target", map[string]any{"id": "s", "relationships": []any{
			map[string]any{"relationship_type": "actual"},
		}}, "no target_signal_id"},
		{"self", map[string]any{"id": "s", "relationships": []any{
			map[string]any{"relationship_type": "actual", "target_signal_id": "s"},
		}}, "names the signal itself"},
		{"two of one type", map[string]any{"id": "s", "relationships": []any{
			map[string]any{"relationship_type": "upper_limit", "target_signal_id": "a"},
			map[string]any{"relationship_type": "upper_limit", "target_signal_id": "b"},
		}}, "more than one upper_limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSignalRelationshipsField(tc.payload)
			if tc.refused == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.refused) {
				t.Fatalf("err = %v, want %q", err, tc.refused)
			}
		})
	}
}

func TestSignalContractRefusesAnUnknownAccess(t *testing.T) {
	if err := Validate("_Signal", []byte(`{"id":"s","access":"write"}`)); err == nil ||
		!strings.Contains(err.Error(), "access must be read or read_write") {
		t.Fatalf("err = %v", err)
	}
	for _, access := range []string{"read", "read_write"} {
		if err := Validate("_Signal", []byte(`{"id":"s","access":"`+access+`"}`)); err != nil {
			t.Fatalf("access %s refused: %v", access, err)
		}
	}
}

func linksAt(t *testing.T, f *fakeStore, topic string) []relationshipLink {
	t.Helper()
	raw, ok := f.KVGet(topic)
	if !ok {
		t.Fatalf("no record at %s", topic)
	}
	var signal relationshipSignal
	if err := json.Unmarshal(raw, &signal); err != nil {
		t.Fatal(err)
	}
	return signal.Relationships
}

// Deleting a signal drops the links that name it, in the same batch, from
// signals the edit does not otherwise touch.
func TestEditDeleteDropsTheLinksNamingTheSignal(t *testing.T) {
	f := newStore("n-edge1")
	speedVersion := seedEditEntity(t, f, "_Signal", "press/speed", map[string]any{
		"id": "sig-speed", "name": "speed", "system_element_id": "el-press", "data_type": "float",
	})
	seedEditEntity(t, f, "_Signal", "press/speed_max", map[string]any{
		"id": "sig-max", "name": "speed_max", "system_element_id": "el-press", "data_type": "float",
	})
	seedEditEntity(t, f, "_Signal", "press/sp", map[string]any{
		"id": "sig-sp", "name": "sp", "system_element_id": "el-press", "data_type": "float",
		"relationships": []map[string]string{
			{"relationship_type": "actual", "target_signal_id": "sig-speed"},
			{"relationship_type": "upper_limit", "target_signal_id": "sig-max"},
		},
	})
	exec := NewEditExec(f, nil)

	code, msg, _, writes := exec.ExecuteWithWrites(asHuman, "_CmdEdit", "apply", editBody(
		t, "op-delete-actual", map[string]uint64{"signal:sig-speed": speedVersion},
		map[string]any{"type": "delete", "entity": map[string]any{"kind": "signal", "id": "sig-speed"}},
	))
	if code != 200 || len(writes) != 2 || f.batchCalls != 1 {
		t.Fatalf("delete = %d %q writes=%+v batches=%d", code, msg, writes, f.batchCalls)
	}
	got := linksAt(t, f, "colca/v1/_Signal/n-edge1/press/sp")
	if len(got) != 1 || got[0] != (relationshipLink{Type: "upper_limit", Target: "sig-max"}) {
		t.Fatalf("links after delete = %+v, want only the upper_limit", got)
	}
}

// A move tombstones a signal's old position, but the signal still exists:
// its links stay.
func TestRelationshipCleanupKeepsLinksToAMovedSignal(t *testing.T) {
	f := newStore("n-edge1")
	seedEditEntity(t, f, "_Signal", "press/speed", map[string]any{"id": "sig-speed", "name": "speed"})
	seedEditEntity(t, f, "_Signal", "press/sp", map[string]any{
		"id": "sig-sp", "name": "sp",
		"relationships": []map[string]string{{"relationship_type": "actual", "target_signal_id": "sig-speed"}},
	})
	moved := []StateRecord{
		{Topic: "colca/v1/_Signal/n-edge1/press/speed"},
		{Topic: "colca/v1/_Signal/n-edge1/line/speed", Payload: []byte(`{"id":"sig-speed","name":"speed"}`)},
	}
	if got := withRelationshipCleanup(f, moved); len(got) != len(moved) {
		t.Fatalf("cleanup added %d records to a move", len(got)-len(moved))
	}
}

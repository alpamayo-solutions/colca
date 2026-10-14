// Signal relationships: directed links from one signal (the source, whose
// _Signal record holds them) to another signal of the same node, each with a
// type. The rules are colca_data_contracts.signal_relationships.RULES;
// vectors/signal_relationships.json pins this copy to that one.
//
// The door's schema already holds a link's shape (a known type, a ULID
// target). What needs the other records is checked here, when a batch is
// committed: the target exists, the type's rules hold across the node's
// signals. Only sources whose links the batch changes are judged, so a link
// left behind by a deleted target does not block an unrelated write.

package uns

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// relationshipRule is what one relationship type allows.
type relationshipRule struct {
	// MaxSourcesPerTarget is how many sources may name one target with the
	// type; 0 for no limit.
	MaxSourcesPerTarget int
	// Chains says whether a target may itself be the source of a link of the
	// type. Without chains there can be no cycle either.
	Chains bool
	// DataTypes is "same_or_numeric", "numeric" or "any".
	DataTypes string
}

var relationshipRules = map[string]relationshipRule{
	"actual":      {MaxSourcesPerTarget: 1, Chains: false, DataTypes: "same_or_numeric"},
	"upper_limit": {Chains: true, DataTypes: "numeric"},
	"lower_limit": {Chains: true, DataTypes: "numeric"},
	"feedback":    {Chains: false, DataTypes: "any"},
}

// numericDataTypes holds the numeric types of both vocabularies: data-model
// slots (integer, number) and signals (int, float).
var numericDataTypes = map[string]bool{"integer": true, "number": true, "int": true, "float": true}

// signalAccessValues is the _Signal access vocabulary; absent means "read".
var signalAccessValues = map[string]bool{"read": true, "read_write": true}

// relationshipLink is one (type, target) pair.
type relationshipLink struct {
	Type   string `json:"relationship_type"`
	Target string `json:"target_signal_id"`
}

// relationshipEnd is a signal, or a slot, as the rules see it.
type relationshipEnd struct {
	DataType string
	Links    []relationshipLink
}

func relationshipTypesCompatible(rule, source, target string) bool {
	if rule == "any" || source == "" || target == "" {
		return true
	}
	bothNumeric := numericDataTypes[source] && numericDataTypes[target]
	if rule == "numeric" {
		return bothNumeric
	}
	return source == target || bothNumeric
}

// relationshipProblems returns every broken rule in ends, one message each,
// in a stable order. label names a key in messages. checked, when not nil,
// limits the per-source rules to the sources it accepts; the rules across
// sources still see every end. notSignals names keys that exist but are no
// signal, such as a child slot.
func relationshipProblems(
	ends map[string]relationshipEnd,
	label func(string) string,
	checked func(string) bool,
	notSignals map[string]bool,
) []string {
	isChecked := func(key string) bool { return checked == nil || checked(key) }
	problems := []string{}
	type typedTarget struct{ kind, target string }
	sourcesOf := map[typedTarget][]string{}
	edges := map[string]map[string]string{} // type -> source -> target

	keys := make([]string, 0, len(ends))
	for key := range ends {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		end := ends[key]
		report := isChecked(key)
		seen := map[string]bool{}
		for _, link := range end.Links {
			rule, known := relationshipRules[link.Type]
			if !known {
				if report {
					problems = append(problems, fmt.Sprintf(
						"%s: unknown relationship type %q; use one of %v", label(key), link.Type, relationshipTypeNames(),
					))
				}
				continue
			}
			if seen[link.Type] {
				if report {
					problems = append(problems, fmt.Sprintf("%s: holds more than one %s relationship", label(key), link.Type))
				}
				continue
			}
			seen[link.Type] = true
			if link.Target == key {
				if report {
					problems = append(problems, fmt.Sprintf("%s: %s names the signal itself", label(key), link.Type))
				}
				continue
			}
			target, exists := ends[link.Target]
			if !exists {
				if report {
					reason := "does not exist"
					if notSignals[link.Target] {
						reason = "is not a signal"
					}
					problems = append(problems, fmt.Sprintf("%s: %s target %s %s", label(key), link.Type, label(link.Target), reason))
				}
				continue
			}
			if report && !relationshipTypesCompatible(rule.DataTypes, end.DataType, target.DataType) {
				problems = append(problems, fmt.Sprintf(
					"%s: %s target %s has data type %q, not compatible with %q",
					label(key), link.Type, label(link.Target), target.DataType, end.DataType,
				))
			}
			id := typedTarget{kind: link.Type, target: link.Target}
			sourcesOf[id] = append(sourcesOf[id], key)
			if edges[link.Type] == nil {
				edges[link.Type] = map[string]string{}
			}
			edges[link.Type][key] = link.Target
		}
	}

	kinds := make([]string, 0, len(edges))
	for kind := range edges {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		rule := relationshipRules[kind]
		links := edges[kind]
		if rule.MaxSourcesPerTarget > 0 {
			ids := make([]typedTarget, 0)
			for id, sources := range sourcesOf {
				if id.kind == kind && len(sources) > rule.MaxSourcesPerTarget {
					ids = append(ids, id)
				}
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i].target < ids[j].target })
			for _, id := range ids {
				sources := sourcesOf[id]
				anyChecked := false
				names := make([]string, len(sources))
				for i, source := range sources {
					anyChecked = anyChecked || isChecked(source)
					names[i] = label(source)
				}
				if anyChecked {
					problems = append(problems, fmt.Sprintf(
						"%s is the %s target of more than one signal: %s", label(id.target), kind, strings.Join(names, ", "),
					))
				}
			}
		}
		sources := make([]string, 0, len(links))
		for source := range links {
			sources = append(sources, source)
		}
		sort.Strings(sources)
		if !rule.Chains {
			for _, source := range sources {
				target := links[source]
				if _, chained := links[target]; chained && (isChecked(source) || isChecked(target)) {
					problems = append(problems, fmt.Sprintf(
						"%s: %s target %s holds a %s relationship itself; %s links do not chain",
						label(source), kind, label(target), kind, kind,
					))
				}
			}
			continue
		}
		problems = append(problems, relationshipCycles(kind, links, sources, label, isChecked)...)
	}
	return problems
}

// relationshipCycles reports each cycle in one type's links. A source holds at
// most one link per type, so following links from any source either ends or
// enters exactly one cycle.
func relationshipCycles(
	kind string, links map[string]string, sources []string, label func(string) string, isChecked func(string) bool,
) []string {
	problems := []string{}
	reported := map[string]bool{}
	for _, start := range sources {
		trail := []string{}
		onTrail := map[string]int{}
		node := start
		for {
			if _, more := links[node]; !more || reported[node] {
				break
			}
			if _, looped := onTrail[node]; looped {
				break
			}
			onTrail[node] = len(trail)
			trail = append(trail, node)
			node = links[node]
		}
		if at, looped := onTrail[node]; looped {
			cycle := trail[at:]
			anyChecked := false
			names := make([]string, 0, len(cycle)+1)
			for _, member := range cycle {
				reported[member] = true
				anyChecked = anyChecked || isChecked(member)
				names = append(names, label(member))
			}
			if anyChecked {
				names = append(names, label(node))
				problems = append(problems, fmt.Sprintf("%s links form a cycle: %s", kind, strings.Join(names, " -> ")))
			}
		}
		for _, member := range trail {
			reported[member] = true
		}
	}
	return problems
}

func relationshipTypeNames() []string {
	names := make([]string, 0, len(relationshipRules))
	for name := range relationshipRules {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// RelationshipError refuses a batch whose signal relationships break a rule.
// Executors answer it with a 422.
type RelationshipError struct {
	Problems []string
}

func (e *RelationshipError) Error() string {
	return "invalid_relationships: " + strings.Join(e.Problems, "; ")
}

// relationshipSignal is the part of a _Signal record the rules read.
type relationshipSignal struct {
	ID            string             `json:"id"`
	DataType      string             `json:"data_type"`
	Relationships []relationshipLink `json:"relationships"`
}

func sameLinks(left, right []relationshipLink) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// CheckSignalRelationships refuses a batch that leaves one of this node's
// signals holding relationships that break a rule. current is every _Signal
// record the node holds before the batch. Only signals whose relationships
// the batch changes are judged, against the node's signals after the batch.
//
// The engine runs it under one lock with the commit, so two commands that
// link to one target at the same time cannot both pass.
func CheckSignalRelationships(current []KVRecord, batch []StateRecord) error {
	before := map[string][]relationshipLink{} // id -> links before the batch
	after := map[string]relationshipSignal{}  // topic -> signal after the batch
	paths := map[string]string{}              // id -> path, for messages
	for _, rec := range current {
		var s relationshipSignal
		if json.Unmarshal(rec.Payload, &s) != nil || s.ID == "" {
			continue
		}
		before[s.ID] = s.Relationships
		after[rec.Topic] = s
		paths[s.ID] = rec.Path
	}
	changed := map[string]bool{}
	for _, rec := range batch {
		parsed, err := Parse(rec.Topic)
		if err != nil || parsed.Contract != "_Signal" {
			continue
		}
		if len(rec.Payload) == 0 {
			delete(after, rec.Topic)
			continue
		}
		var s relationshipSignal
		if json.Unmarshal(rec.Payload, &s) != nil || s.ID == "" {
			continue // not a record: the schema check refuses it
		}
		after[rec.Topic] = s
		paths[s.ID] = parsed.Path
		if len(s.Relationships) > 0 && !sameLinks(before[s.ID], s.Relationships) {
			changed[s.ID] = true
		}
	}
	if len(changed) == 0 {
		return nil
	}
	ends := make(map[string]relationshipEnd, len(after))
	for _, s := range after {
		ends[s.ID] = relationshipEnd{DataType: s.DataType, Links: s.Relationships}
	}
	label := func(id string) string {
		if path, ok := paths[id]; ok && path != "" {
			return path
		}
		return id
	}
	problems := relationshipProblems(ends, label, func(id string) bool { return changed[id] }, nil)
	if len(problems) == 0 {
		return nil
	}
	return &RelationshipError{Problems: problems}
}

// relationshipsWithout drops every link to a target in removed. It reports
// whether anything was dropped.
func relationshipsWithout(raw json.RawMessage, removed map[string]bool) (json.RawMessage, bool) {
	if len(raw) == 0 {
		return raw, false
	}
	var links []relationshipLink
	if json.Unmarshal(raw, &links) != nil {
		return raw, false
	}
	kept := make([]relationshipLink, 0, len(links))
	for _, link := range links {
		if !removed[link.Target] {
			kept = append(kept, link)
		}
	}
	if len(kept) == len(links) {
		return raw, false
	}
	return rawJSON(kept), true
}

// withRelationshipCleanup returns records plus what keeps the node's links
// pointing at signals that still exist: a batch that retires a signal also
// drops every link naming it, from the records the batch already writes and
// from the node's other signals. Executors call it on the batch they are about
// to commit, so whichever verb retires a signal leaves no link behind.
func withRelationshipCleanup(store EntityStore, records []StateRecord) []StateRecord {
	removed := map[string]bool{}
	inBatch := map[string]int{}
	for i, rec := range records {
		parsed, err := Parse(rec.Topic)
		if err != nil || parsed.Contract != "_Signal" {
			continue
		}
		inBatch[rec.Topic] = i
		if len(rec.Payload) > 0 {
			continue
		}
		if held, ok := store.KVGet(rec.Topic); ok {
			if id := recordID(held); id != "" {
				removed[id] = true
			}
		}
	}
	// A signal the batch writes again, at the same or another position (a
	// move tombstones the old one), still exists.
	for _, rec := range records {
		if parsed, err := Parse(rec.Topic); err == nil && parsed.Contract == "_Signal" && len(rec.Payload) > 0 {
			delete(removed, recordID(rec.Payload))
		}
	}
	if len(removed) == 0 {
		return records
	}
	out := append([]StateRecord(nil), records...)
	strip := func(payload []byte) ([]byte, bool) {
		var fields map[string]json.RawMessage
		if json.Unmarshal(payload, &fields) != nil {
			return payload, false
		}
		kept, changed := relationshipsWithout(fields["relationships"], removed)
		if !changed {
			return payload, false
		}
		fields["relationships"] = kept
		encoded, err := json.Marshal(fields)
		if err != nil {
			return payload, false
		}
		return encoded, true
	}
	extra := []StateRecord{}
	for _, rec := range store.KVScan("_Signal", store.NodeID()) {
		if index, written := inBatch[rec.Topic]; written {
			if len(out[index].Payload) > 0 {
				if payload, changed := strip(out[index].Payload); changed {
					out[index].Payload = payload
				}
			}
			continue
		}
		if payload, changed := strip(rec.Payload); changed {
			extra = append(extra, StateRecord{Topic: rec.Topic, Payload: payload})
		}
	}
	sort.Slice(extra, func(i, j int) bool { return extra[i].Topic < extra[j].Topic })
	return append(out, extra...)
}

// validateSignalRelationshipsField checks the shape of a _Signal record's
// relationships on its own: a list of objects with a known type and a target,
// never the signal itself, at most one per type. Whether the target exists and
// the type's rules hold is CheckSignalRelationships' part.
func validateSignalRelationshipsField(m map[string]any) error {
	value, present := m["relationships"]
	if !present || value == nil {
		return nil
	}
	items, ok := value.([]any)
	if !ok {
		return fmt.Errorf("_Signal: relationships must be an array")
	}
	id, _ := m["id"].(string)
	seen := map[string]bool{}
	for i, item := range items {
		link, ok := item.(map[string]any)
		if !ok {
			return fmt.Errorf("_Signal: relationships[%d] must be an object", i)
		}
		kind, _ := link["relationship_type"].(string)
		if _, known := relationshipRules[kind]; !known {
			return fmt.Errorf("_Signal: relationships[%d] has unknown relationship_type %q; use one of %v",
				i, kind, relationshipTypeNames())
		}
		target, _ := link["target_signal_id"].(string)
		if target == "" {
			return fmt.Errorf("_Signal: relationships[%d] has no target_signal_id", i)
		}
		if target == id {
			return fmt.Errorf("_Signal: relationships[%d] names the signal itself", i)
		}
		if seen[kind] {
			return fmt.Errorf("_Signal: holds more than one %s relationship", kind)
		}
		seen[kind] = true
	}
	return nil
}

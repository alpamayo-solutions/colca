package uns

import (
	"encoding/json"
	"strings"
)

// Plan before write, then authorize the plan (node-side command authorization
// design §3C).
//
// A `_CmdEdit` is composed into the exact records it will write BEFORE
// any of them is written. Every position in that plan is then checked with
// the one per-position decision the doors use for every other command —
// `AuthorizeCmdAt(scope, actor, "configure", path)`, a prefix comparison of
// the actor's configure grants against the record's path — and one
// uncovered position refuses the whole command with zero writes. The door's
// own check for this contract is class-only (its route names the owning
// node, not a position); this is the fine check, against what the command
// actually touches, which is the property preflight on the api side could
// only approximate from the other side of the door.
//
// The refusal is shaped like a not-found: the same `entity_not_found: <key>`
// a missing entity earns, naming the entity the caller named. A caller
// cannot learn that an element exists by being refused on it (the existence
// oracle stays closed, as it does in preflight's `_require_row`).

// SetScope wires the node's element scope in. Without it only `#` grants
// resolve (zoneOf fails closed on a nil scope), so a scoped grant covers
// nothing — the production node always sets it; a test that exercises
// element-scoped grants supplies its own.
func (w *EditExec) SetScope(sc Scope) { w.scope = sc }

// editTouched is one position a plan writes or repositions, with the
// key the caller is told about if it is not covered.
type editTouched struct {
	path string
	key  string
	// operate marks a position an `operate` grant covers as well as a
	// configure one: an annotation the person creates, or one they authored
	// (annotation-cutover design: an operator annotates; only configure
	// edits another author's annotation).
	operate bool
}

// authorizeTouched refuses the command unless every touched position is
// covered by the actor's grants — configure always, operate where the
// position allows it. code 0 means covered.
func (w *EditExec) authorizeTouched(ctx CommandContext, touched []editTouched) (int, string, string) {
	for _, t := range touched {
		// The node root (an empty path) is the position every element here
		// sits under, so only a zone of "#" covers it: a realm-wide grant, or
		// a grant naming the node's own element or one of its ancestors —
		// which zoneOf resolves to "#" through Scope.Reaches, the ancestry the
		// parent taught. A grant on any element BELOW the node resolves to
		// that element's path and does not cover the root.
		covered := AuthorizeCmdAt(w.scope, ctx.Actor, "configure", t.path) ||
			(t.operate && AuthorizeCmdAt(w.scope, ctx.Actor, "operate", t.path))
		if !covered {
			return 409, "entity_not_found: " + t.key, "conflict"
		}
	}
	return 0, "", ""
}

// keycloakServiceAccountPrefix is how Keycloak names a client-credentials
// token's preferred_username — the same constant the api's
// `_service_account_client_id` keys on.
const keycloakServiceAccountPrefix = "service-account-"

// AnnotationSource is the `source` the api stamps on an annotation this
// actor authors (`commands.annotation_source`, pinned by the shared
// `vectors/annotation_id.json` `sources` cases): half of the annotation's
// derived id, which is what lets a node prove authorship from the id alone.
// A person is `user/<sub>`; a Keycloak service account — nobody is behind
// its sub — is `service/<client-id>`. An mTLS service never acts here (the
// executor refuses a non-human actor), so that kind is the api's alone.
func AnnotationSource(e *Entry) string {
	if e == nil {
		return ""
	}
	if strings.HasPrefix(e.Username, keycloakServiceAccountPrefix) {
		if client := strings.TrimPrefix(e.Username, keycloakServiceAccountPrefix); client != "" {
			return "service/" + client
		}
	}
	return "user/" + e.ULID
}

// annotationOperable reports whether an `operate` grant may carry this
// annotation intent: a create always (the annotation will be the person's
// own), an update or delete only of an annotation the person authored —
// provable without any lookup, because the id is derived from
// (type, source, time_start, signal set) and the source names the author.
// The signal set is re-derived from the intent's own `signal_ids`, so under
// an `operate` grant an update must carry the set the annotation was
// created with: a different set derives a different identity, and an
// update naming one id while describing another is not provably the
// person's own. `configure` coverage (the superset) is not routed through
// here and may still repoint any annotation.
func annotationOperable(intent editIntent, actor *Entry) bool {
	if intent.Action == "create" {
		return true
	}
	if actor == nil || intent.TimeStart == nil || intent.AnnotationID == "" {
		return false
	}
	source := AnnotationSource(actor)
	own := deriveAnnotationID(intent.AnnotationTypeID, source, *intent.TimeStart, intent.SignalIDs)
	return intent.Source == source && intent.AnnotationID == own
}

// touchedByRecords is the plan a composer produced, as positions: one per
// record (a tombstone at an old topic, a payload at a new one). The key is
// the snapshot entity at that topic when there is one, otherwise the anchor
// the caller named — the parent of a create, the target of a placement.
func touchedByRecords(
	records []StateRecord, entities map[string]editSnapshot, anchor string,
) []editTouched {
	byTopic := make(map[string]string, len(entities))
	for key, entity := range entities {
		if entity.Record.Topic != "" {
			byTopic[entity.Record.Topic] = key
		}
	}
	touched := make([]editTouched, 0, len(records))
	for _, record := range records {
		p, err := Parse(record.Topic)
		if err != nil {
			// A composer never emits an unparseable topic; if it did, no
			// grant could cover it — fail closed on the anchor.
			touched = append(touched, editTouched{path: "", key: anchor})
			continue
		}
		key, known := byTopic[record.Topic]
		if !known {
			key = anchor
			if id := recordID(record.Payload); id != "" {
				if kind := editKindOf(p.Contract); kind != "" {
					key = entityVersionKey(kind, id)
				}
			}
		}
		touched = append(touched, editTouched{path: p.Path, key: key})
	}
	return touched
}

// touchedEntity is the position of one snapshot entity, or the not-found
// shape when the snapshot has no such entity — the same answer either way,
// so refusing on it never says which.
func touchedEntity(entities map[string]editSnapshot, kind, id string) editTouched {
	key := entityVersionKey(kind, id)
	entity, ok := entities[key]
	if !ok {
		return editTouched{path: "", key: key}
	}
	return editTouched{path: entity.Record.Path, key: key}
}

// editKindOf is the intent vocabulary for a state contract — the reverse
// of editSourceEntity.
func editKindOf(contract string) string {
	for kind, source := range map[string]string{
		"system-element": "SystemElement",
		"signal":         "Signal",
		"constant":       "Constant",
		"colca-node":     "Node",
	} {
		if "_"+source == contract {
			return kind
		}
	}
	return ""
}

func recordID(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var value struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(payload, &value); err != nil {
		return ""
	}
	return value.ID
}

// planFor is the write-set of one composed intent (design §3C table): the
// records it will write, plus what a record does not spell out — the target
// of a placement, the connector a binding draws on, every signal an
// annotation names.
func (w *EditExec) planFor(
	ctx CommandContext,
	intent editIntent,
	records []StateRecord,
	entities map[string]editSnapshot,
	catalogues map[string]editCatalogueSnapshot,
) []editTouched {
	switch intent.Type {
	case "create":
		anchor := entityVersionKey("system-element", intent.ParentID)
		return append([]editTouched{touchedEntity(entities, "system-element", intent.ParentID)},
			touchedByRecords(records, entities, anchor)...)
	case "placement":
		anchor := entityVersionKey("system-element", intent.TargetParentID)
		return append([]editTouched{touchedEntity(entities, "system-element", intent.TargetParentID)},
			touchedByRecords(records, entities, anchor)...)
	case "binding":
		anchor := "catalogue:" + intent.ConnectorID
		touched := []editTouched{{path: "", key: anchor}}
		if catalogue, ok := catalogues[intent.ConnectorID]; ok {
			if p, err := Parse(catalogue.Record.Topic); err == nil {
				// `_DataTags/{node}/{mount…}/{service-name}`: the connector's
				// element is the mount, the last segment is its name.
				mount := p.Path
				if i := strings.LastIndex(mount, "/"); i >= 0 {
					mount = mount[:i]
				} else {
					mount = ""
				}
				touched[0].path = mount
			}
		}
		return append(touched, touchedByRecords(records, entities, anchor)...)
	case "annotation":
		// The record sits at a reserved path no element owns; what the
		// annotation touches is every signal it names. None named → only a
		// realm-wide grant may write it. An operate grant carries it too,
		// for a create or the person's own annotation.
		operable := annotationOperable(intent, ctx.Actor)
		if len(intent.SignalIDs) == 0 {
			return []editTouched{{path: "", key: "signal:", operate: operable}}
		}
		touched := make([]editTouched, 0, len(intent.SignalIDs))
		for _, id := range intent.SignalIDs {
			t := touchedEntity(entities, "signal", id)
			t.operate = operable
			touched = append(touched, t)
		}
		return touched
	case "alarm", "alarm_acknowledgement":
		// The signal the alarm is about — never the config record's own
		// reserved path, which no element owns.
		return w.alarmPositions(intent, entities)
	case "notification_config":
		// The whole node: one position, the node's own root. See
		// notificationConfigPositions for how a grant on the element the
		// node's parent enrolled it at resolves to cover it.
		return w.notificationConfigPositions()
	case "resource":
		// The positions the composed records sit on — one for a create or an
		// in-place update, two for a move, both checked. Not `touchedByRecords`:
		// a resource is not in the entity snapshot, it is addressed by its own
		// position, and its element is the path above the record.
		return w.resourcePositions(intent, records)
	default: // update, delete, model
		anchor := entityVersionKey(intent.Entity.Kind, intent.Entity.ID)
		return touchedByRecords(records, entities, anchor)
	}
}

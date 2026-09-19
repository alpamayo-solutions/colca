package uns

import (
	"encoding/json"
	"strings"
)

// Plan before write, then authorize the plan. A _CmdEdit is composed into the
// records it will write, and every position in that plan is checked with
// AuthorizeCmdAt against the actor's configure grants; one uncovered position
// refuses the whole command with nothing written. The door only checks the
// class, since the route names the owning node, not a position.
//
// A refusal looks like entity_not_found for the named entity, so a caller
// cannot learn that an element exists by being refused on it.

// SetScope sets the node's element scope. Without it only "#" grants resolve;
// production always sets it.
func (w *EditExec) SetScope(sc Scope) { w.scope = sc }

// editTouched is one position a plan writes or repositions, with the
// key the caller is told about if it is not covered.
type editTouched struct {
	path string
	key  string
	// operate marks a position an operate grant also covers: an annotation
	// the person creates or authored. Editing someone else's needs configure.
	operate bool
}

// authorizeTouched refuses the command unless every touched position is
// covered by the actor's grants — configure always, operate where the
// position allows it. code 0 means covered.
func (w *EditExec) authorizeTouched(ctx CommandContext, touched []editTouched) (int, string, string) {
	for _, t := range touched {
		// Only a "#" zone covers the node root (empty path): a realm-wide
		// grant, or a grant on the node's own element or an ancestor, which
		// zoneOf resolves to "#". A grant on an element below the node does not.
		covered := AuthorizeCmdAt(w.scope, ctx.Actor, "configure", t.path) ||
			(t.operate && AuthorizeCmdAt(w.scope, ctx.Actor, "operate", t.path))
		if !covered {
			return 409, "entity_not_found: " + t.key, "conflict"
		}
	}
	return 0, "", ""
}

// keycloakServiceAccountPrefix starts the preferred_username of a Keycloak
// client-credentials token.
const keycloakServiceAccountPrefix = "service-account-"

// AnnotationSource is the source the api stamps on an annotation this actor
// authors, and half of its derived id, so a node can prove authorship from the
// id alone. A person is user/<sub>, a Keycloak service account
// service/<client-id>. The vectors in annotation_id.json pin both.
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

// annotationOperable reports whether an operate grant may carry this annotation
// intent: always for a create, and for an update or delete only of the
// person's own annotation, which the derived id proves without a lookup. The id
// is re-derived from the intent's signal_ids, so an update under operate must
// keep the original signal set. Configure may change any annotation.
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

// touchedByRecords turns a composer's records into positions, one per record.
// The key is the snapshot entity at that topic, or else the anchor the caller
// named (a create's parent, a placement's target).
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
// shape if there is none; both refuse the same way.
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

// planFor returns the positions one composed intent writes: its records plus
// what they do not spell out, such as a placement's target, a binding's
// connector or an annotation's signals.
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
		// The record sits at a reserved path, so the positions are the signals
		// the annotation names; with none, only a realm-wide grant covers it.
		// An operate grant also covers a create or the person's own annotation.
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
	case "alarm":
		// The signal the alarm is about — never the config record's own
		// reserved path, which no element owns.
		return w.alarmPositions(intent, entities)
	case "notification_config":
		// The whole node: one position, its root. See
		// notificationConfigPositions.
		return w.notificationConfigPositions()
	case "resource":
		// A resource is not in the entity snapshot, so check the positions of
		// its records: one for a create or update, two for a move.
		return w.resourcePositions(intent, records)
	default: // update, delete, model
		anchor := entityVersionKey(intent.Entity.Kind, intent.Entity.ID)
		return touchedByRecords(records, entities, anchor)
	}
}

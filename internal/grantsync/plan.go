package grantsync

import (
	"sort"
	"strings"
)

// ResourcePlan is direction one: which elements need registering, relabelling
// or removing so an administrator can assign against them.
type ResourcePlan struct {
	Create []Resource
	Update []Resource
	Delete []Resource
	// Unmanaged names resources without our marker. They are reported, never
	// deleted — somebody made them by hand, in a store this service does not own.
	Unmanaged []string
}

// PlanResources diffs the tree's elements against the registered resources.
//
// The resource NAME is the element id — what a grant carries, and what survives
// a rename. The DISPLAY NAME is the current path, which is what an
// administrator recognises in the console and therefore what a rename must
// update. Keycloak preserves a resource's identity across a display-name
// change, so a renamed element keeps every grant pointing at the same thing.
func PlanResources(elements map[string]string, existing []Resource, owner string) ResourcePlan {
	var plan ResourcePlan
	byName := map[string]Resource{}
	for _, r := range existing {
		byName[r.Name] = r
	}

	for _, id := range sortedKeys(elements) {
		path := elements[id]
		current, ok := byName[id]
		if !ok {
			plan.Create = append(plan.Create, Resource{
				Name: id, DisplayName: path, Type: ElementResourceType,
				Attributes: map[string][]string{ManagedByAttr: {owner}},
			})
			continue
		}
		if current.DisplayName != path || !current.ManagedBy(owner) {
			plan.Update = append(plan.Update, Resource{
				ID: current.ID, Name: id, DisplayName: path, Type: ElementResourceType,
				Attributes: map[string][]string{ManagedByAttr: {owner}},
			})
		}
	}

	var names []string
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, live := elements[name]; live {
			continue
		}
		resource := byName[name]
		if resource.Type != ElementResourceType {
			continue // not ours to reason about at all
		}
		if !resource.ManagedBy(owner) {
			plan.Unmanaged = append(plan.Unmanaged, name)
			continue
		}
		plan.Delete = append(plan.Delete, resource)
	}
	return plan
}

// GroupDefinition is the record written into the tree. Field names match what a
// node reads (uns's `group`), because this is the same contract seen from the
// writing side.
type GroupDefinition struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Grants []string `json:"grants"`
}

// Orphan is a grant naming an element no node in the tree holds.
type Orphan struct {
	Group   string
	Element string
}

// DefinitionPlan is direction two: what to write into the tree and what to
// withdraw from it, plus what to name but not touch.
type DefinitionPlan struct {
	Upsert  []GroupDefinition
	Retract []string
	// Foreign names definitions authored by another node that Keycloak does not
	// account for. Not ours to withdraw — a tombstone written here would not
	// remove them anyway, since records are keyed by author.
	Foreign []string
	// Unresolvable names grants pointing at elements no node holds.
	Unresolvable []Orphan
}

// PlanDefinitions diffs Keycloak's compiled grants against the tree.
//
// Convergence INCLUDES removal: a group deleted in Keycloak, or a permission
// withdrawn, retracts its definition — otherwise access outlives its own
// revocation, which is the failure with the worst consequences here. But only
// over definitions this node authored.
func PlanDefinitions(desired map[string][]string, tree TreeView, rootULID string) DefinitionPlan {
	var plan DefinitionPlan

	for _, group := range sortedKeys(desired) {
		grants := desired[group]
		for _, grant := range grants {
			if element := elementOf(grant); element != "" {
				if _, held := tree.Elements[element]; !held {
					plan.Unresolvable = append(plan.Unresolvable, Orphan{Group: group, Element: element})
				}
			}
		}
		held, ok := tree.Groups[group]
		if ok && equal(held.Grants, grants) {
			continue
		}
		plan.Upsert = append(plan.Upsert, GroupDefinition{ID: group, Name: group, Grants: grants})
	}

	var held []string
	for id := range tree.Groups {
		held = append(held, id)
	}
	sort.Strings(held)
	for _, id := range held {
		if _, wanted := desired[id]; wanted {
			continue
		}
		if tree.Groups[id].Author != rootULID {
			plan.Foreign = append(plan.Foreign, id)
			continue
		}
		plan.Retract = append(plan.Retract, id)
	}
	return plan
}

// elementOf returns the element a grant names, or "" for a grant with no
// element (`admin:#`, `read:#`) — those are realm-wide and resolve everywhere.
func elementOf(grant string) string {
	parts := strings.SplitN(grant, ":", 3)
	if len(parts) < 2 {
		return ""
	}
	zone := strings.TrimSuffix(parts[1], "/#")
	if zone == "#" {
		return ""
	}
	return zone
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

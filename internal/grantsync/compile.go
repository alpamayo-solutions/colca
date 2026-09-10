package grantsync

import (
	"fmt"
	"sort"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// ColcaGrantsAttr holds grant strings set directly on a Keycloak group. It is
// how admin:# is expressed, since there is no element to attach it to, and a
// way to unblock access when the authz objects are wrong.
const ColcaGrantsAttr = "colca_grants"

// CompileGrants turns permissions into the grant strings a node evaluates. The
// mapping is mechanical: this service carries decisions, it does not make them.
// Output is sorted and deduplicated because it is compared with the tree every
// cycle. Problems are returned, not raised, so one bad grant does not cost a
// group the rest.
func CompileGrants(perms []Permission, attrs map[string][]string) (map[string][]string, []error) {
	var problems []error
	buckets := map[string]map[string]bool{}
	add := func(group, grant string) {
		if buckets[group] == nil {
			buckets[group] = map[string]bool{}
		}
		buckets[group][grant] = true
	}

	// The class vocabulary comes from uns, not from a list restated here: a
	// class added there must not need a second edit to become grantable.
	classes := map[string]bool{}
	for _, c := range uns.CmdClasses() {
		classes[c] = true
	}

	for _, perm := range perms {
		var granted []string
		read := false
		for _, s := range perm.Scopes {
			switch {
			case s == "read":
				read = true
			case classes[s]:
				granted = append(granted, s)
			default:
				problems = append(problems, fmt.Errorf(
					"permission %s: scope %q is not one this service understands", perm.Name, s))
			}
		}

		for _, element := range perm.Elements {
			// A resource name must be an element id. FormatGrant reads "#" and "" as the
			// whole namespace, so such a name would grant the entire tree, and ParseGrant
			// cannot tell afterwards. Refuse it here.
			if element == "" {
				problems = append(problems, fmt.Errorf(
					"permission %s: refusing a resource with no name", perm.Name))
				continue
			}
			if err := uns.ValidElementID(element); err != nil {
				problems = append(problems, fmt.Errorf(
					"permission %s: refusing resource %q: %w", perm.Name, element, err))
				continue
			}
			var built []uns.Grant
			if read {
				built = append(built, uns.Grant{Verb: "read", Element: element})
			}
			if len(granted) > 0 {
				built = append(built, uns.Grant{Verb: "cmd", Element: element, Classes: granted})
			}
			for _, g := range built {
				// Rendering goes through uns too, so the grammar is stated in
				// exactly one place and this cannot drift from what nodes parse.
				grant, err := uns.FormatGrant(g)
				if err != nil {
					problems = append(problems, fmt.Errorf(
						"permission %s: %w", perm.Name, err))
					continue
				}
				for _, group := range perm.Groups {
					add(group, grant)
				}
			}
		}
	}

	for group, values := range attrs {
		for _, value := range values {
			if _, err := uns.ParseGrant(value); err != nil {
				problems = append(problems, fmt.Errorf(
					"group %s: dropping unusable %s attribute %q: %w", group, ColcaGrantsAttr, value, err))
				continue
			}
			add(group, value)
		}
	}

	out := map[string][]string{}
	for group, set := range buckets {
		grants := make([]string, 0, len(set))
		for grant := range set {
			// Only emit what a node accepts, or the definition would be refused at every
			// node.
			if _, err := uns.ParseGrant(grant); err != nil {
				problems = append(problems, fmt.Errorf("group %s: refusing to publish %q: %w",
					group, grant, err))
				continue
			}
			grants = append(grants, grant)
		}
		if len(grants) == 0 {
			continue
		}
		sort.Strings(grants)
		out[group] = grants
	}
	return out, problems
}

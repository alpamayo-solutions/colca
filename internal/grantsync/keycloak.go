package grantsync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// The six things a grant can allow: uns's read verb and its five command
// classes. These are the only scope names the resource server declares, and the
// only ones CompileGrants understands.
var AuthzScopes = [...]string{"read", "param", "operate", "maintain", "configure", "admin"}

const (
	// ManagedByAttr marks a resource this tooling owns. Policies and
	// permissions have no attribute map, so they carry it in their description.
	ManagedByAttr = "colca.managed-by"
	// ElementResourceType distinguishes an element resource from anything else
	// somebody may have created on the same resource server.
	ElementResourceType = "colca:element"
)

// Resource is one authz resource — an element, made assignable.
type Resource struct {
	ID          string              `json:"_id"`
	Name        string              `json:"name"`
	DisplayName string              `json:"displayName"`
	Type        string              `json:"type"`
	Attributes  map[string][]string `json:"attributes"`
}

// ManagedBy reports whether this deployment owns the resource. Anything else is
// somebody's hand-made object and is reported rather than deleted.
func (r Resource) ManagedBy(owner string) bool {
	for _, v := range r.Attributes[ManagedByAttr] {
		if v == owner {
			return true
		}
	}
	return false
}

// Permission is one (groups × elements × scopes) grant, already resolved: group
// NAMES as the token carries them, element ids, and scope names.
type Permission struct {
	Name     string
	Groups   []string
	Elements []string
	Scopes   []string
}

// KeycloakView is one consistent read of the resource server.
type KeycloakView struct {
	Resources   []Resource
	Permissions []Permission
	// Attributes is the escape hatch: colca_grants set directly on a group,
	// passed through verbatim. It is how admin:# is expressed — realm-wide, with
	// no element to hang on.
	Attributes map[string][]string
	// Problems is data seen but not fixed: a group policy naming a group id
	// this realm's groups no longer account for — almost always a group deleted
	// without cleaning up the permission that still points at it. Reported
	// rather than silently dropped, the same way CompileGrants reports an
	// unusable hand-typed grant.
	Problems []error
}

// Keycloak reads one realm's resource server over the admin API.
type Keycloak struct {
	BaseURL      string // Keycloak base incl. its http relative path
	Realm        string
	ClientID     string // service account used to read
	ClientSecret string
	AuthzClient  string // clientId whose resource server holds the objects
	HTTP         *http.Client

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func (k *Keycloak) client() *http.Client {
	if k.HTTP != nil {
		return k.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// accessToken fetches (and briefly caches) a service-account token.
func (k *Keycloak) accessToken(ctx context.Context) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.token != "" && time.Now().Before(k.expiry) {
		return k.token, nil
	}
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {k.ClientID},
		"client_secret": {k.ClientSecret},
	}
	endpoint := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token",
		strings.TrimRight(k.BaseURL, "/"), url.PathEscape(k.Realm))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
		strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := k.client().Do(req)
	if err != nil {
		return "", fmt.Errorf("keycloak token: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		// Never echo the raw body: the request carried the client secret and
		// Keycloak sometimes reflects request context back.
		return "", fmt.Errorf("keycloak token: HTTP %d (client %q, realm %q)",
			resp.StatusCode, k.ClientID, k.Realm)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.AccessToken == "" {
		return "", fmt.Errorf("keycloak token: unreadable response")
	}
	k.token = payload.AccessToken
	// Renew well before expiry; a cycle is short and a stale token mid-cycle
	// would surface as a spurious 401 that skips the cycle for no reason.
	ttl := time.Duration(payload.ExpiresIn) * time.Second
	if ttl > 30*time.Second {
		ttl -= 30 * time.Second
	}
	k.expiry = time.Now().Add(ttl)
	return k.token, nil
}

func (k *Keycloak) get(ctx context.Context, path string, out any) error {
	token, err := k.accessToken(ctx)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/admin/realms/%s%s",
		strings.TrimRight(k.BaseURL, "/"), url.PathEscape(k.Realm), path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := k.client().Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, truncate(body, 300))
	}
	if len(body) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	return nil
}

func (k *Keycloak) authzPath(clientUUID, suffix string) string {
	return "/clients/" + clientUUID + "/authz/resource-server" + suffix
}

// ClientUUID resolves the resource-server client's internal id.
func (k *Keycloak) ClientUUID(ctx context.Context) (string, error) {
	var rows []struct {
		ID       string `json:"id"`
		ClientID string `json:"clientId"`
	}
	if err := k.get(ctx, "/clients?clientId="+url.QueryEscape(k.AuthzClient), &rows); err != nil {
		return "", err
	}
	for _, row := range rows {
		if row.ClientID == k.AuthzClient {
			return row.ID, nil
		}
	}
	return "", fmt.Errorf("realm %q has no client %q — the resource server has to exist before "+
		"an element can be registered on it", k.Realm, k.AuthzClient)
}

type kcGroup struct {
	ID         string              `json:"id"`
	Name       string              `json:"name"`
	Attributes map[string][]string `json:"attributes"`
	// SubGroupCount is Keycloak 23+'s replacement for inlining children in a
	// group listing: the listing carries the count, never the members. A
	// realm whose response omits the field (or an older Keycloak that still
	// inlines subGroups directly) reads it as zero, which is the correct
	// "nothing more to fetch" answer either way.
	SubGroupCount int `json:"subGroupCount"`
}

type kcGroupPolicy struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Groups []struct {
		ID string `json:"id"`
	} `json:"groups"`
}

// View reads the whole resource server and the realm's groups.
//
// It returns on the FIRST error and never a partial view. A partial read is the
// dangerous one: it does not look like a failure, it looks like a smaller set of
// grants — and the caller would converge the tree down to it.
func (k *Keycloak) View(ctx context.Context) (KeycloakView, error) {
	clientUUID, err := k.ClientUUID(ctx)
	if err != nil {
		return KeycloakView{}, err
	}

	var groups []kcGroup
	if err := k.get(ctx, "/groups?briefRepresentation=false&max=-1", &groups); err != nil {
		return KeycloakView{}, err
	}
	groupNames := map[string]string{}
	attributes := map[string][]string{}
	// Keycloak 23+ never inlines a group's children in a listing response — the
	// listing carries only subGroupCount, and the members come exclusively from
	// GET /groups/{id}/children. This is the one traversal for every realm: a
	// group with subGroupCount 0 (set, or simply absent from an older response)
	// has nothing more to fetch.
	var walk func(context.Context, []kcGroup) error
	walk = func(ctx context.Context, gs []kcGroup) error {
		for _, g := range gs {
			groupNames[g.ID] = g.Name
			if hatch := g.Attributes[ColcaGrantsAttr]; len(hatch) > 0 {
				attributes[g.Name] = append(attributes[g.Name], hatch...)
			}
			if g.SubGroupCount == 0 {
				continue
			}
			var children []kcGroup
			if err := k.get(ctx, "/groups/"+url.PathEscape(g.ID)+"/children?briefRepresentation=false&max=-1",
				&children); err != nil {
				return err
			}
			if err := walk(ctx, children); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(ctx, groups); err != nil {
		return KeycloakView{}, err
	}

	// deep=true is required: without it Keycloak omits `attributes`, and every
	// resource would read as unmanaged.
	var resources []Resource
	if err := k.get(ctx, k.authzPath(clientUUID, "/resource?deep=true&max=-1"), &resources); err != nil {
		return KeycloakView{}, err
	}

	// A group policy's MEMBERSHIP only comes back from this list; a permission's
	// associated-policies view returns an empty config whatever the policy holds.
	var policies []kcGroupPolicy
	if err := k.get(ctx, k.authzPath(clientUUID, "/policy/group?max=-1"), &policies); err != nil {
		return KeycloakView{}, err
	}
	policyByID := map[string]kcGroupPolicy{}
	for _, p := range policies {
		policyByID[p.ID] = p
	}

	var perms []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := k.get(ctx, k.authzPath(clientUUID, "/permission/scope?max=-1"), &perms); err != nil {
		return KeycloakView{}, err
	}

	view := KeycloakView{Resources: resources, Attributes: attributes}
	for _, perm := range perms {
		// Three reads per permission is Keycloak's shape, not a choice: the
		// list row carries none of its resources, scopes or policies.
		var assoc []struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		if err := k.get(ctx, k.authzPath(clientUUID, "/policy/"+perm.ID+"/associatedPolicies"), &assoc); err != nil {
			return KeycloakView{}, err
		}
		var names []string
		for _, a := range assoc {
			policy, isGroupPolicy := policyByID[a.ID]
			if !isGroupPolicy {
				continue // an associated policy of another type: not this service's concern
			}
			for _, g := range policy.Groups {
				name, ok := groupNames[g.ID]
				if !ok {
					// The policy still names a group UUID our realm read does not
					// account for — almost always a group deleted without cleaning
					// up the permission bound to it. Report it: silently dropping it
					// makes an intended grant vanish with nothing to explain why.
					view.Problems = append(view.Problems, fmt.Errorf(
						"permission %s: group policy %s names group %s, which this realm no longer has",
						perm.Name, policy.Name, g.ID))
					continue
				}
				names = append(names, name)
			}
		}
		if len(names) == 0 {
			continue // bound to no group: grants nobody anything
		}

		var resourceRows []struct {
			Name string `json:"name"`
		}
		if err := k.get(ctx, k.authzPath(clientUUID, "/policy/"+perm.ID+"/resources"), &resourceRows); err != nil {
			return KeycloakView{}, err
		}
		var scopeRows []struct {
			Name string `json:"name"`
		}
		if err := k.get(ctx, k.authzPath(clientUUID, "/policy/"+perm.ID+"/scopes"), &scopeRows); err != nil {
			return KeycloakView{}, err
		}

		p := Permission{Name: perm.Name, Groups: names}
		for _, r := range resourceRows {
			p.Elements = append(p.Elements, r.Name)
		}
		for _, s := range scopeRows {
			p.Scopes = append(p.Scopes, s.Name)
		}
		view.Permissions = append(view.Permissions, p)
	}
	return view, nil
}

// --- writes (direction one: elements become assignable) --------------------

func (k *Keycloak) write(ctx context.Context, method, path string, body any) error {
	token, err := k.accessToken(ctx)
	if err != nil {
		return err
	}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = strings.NewReader(string(encoded))
	}
	endpoint := fmt.Sprintf("%s/admin/realms/%s%s",
		strings.TrimRight(k.BaseURL, "/"), url.PathEscape(k.Realm), path)
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := k.client().Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		reason, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, truncate(reason, 300))
	}
	return nil
}

// EnsureScopes declares the six authz scopes, so a permission can name them.
func (k *Keycloak) EnsureScopes(ctx context.Context, clientUUID string) error {
	var have []struct {
		Name string `json:"name"`
	}
	if err := k.get(ctx, k.authzPath(clientUUID, "/scope?max=-1"), &have); err != nil {
		return err
	}
	present := map[string]bool{}
	for _, s := range have {
		present[s.Name] = true
	}
	for _, name := range AuthzScopes {
		if present[name] {
			continue
		}
		if err := k.write(ctx, http.MethodPost, k.authzPath(clientUUID, "/scope"),
			map[string]string{"name": name}); err != nil {
			return err
		}
	}
	return nil
}

func (k *Keycloak) CreateResource(ctx context.Context, clientUUID string, r Resource) error {
	return k.write(ctx, http.MethodPost, k.authzPath(clientUUID, "/resource"), resourceBody(r))
}

func (k *Keycloak) UpdateResource(ctx context.Context, clientUUID string, r Resource) error {
	return k.write(ctx, http.MethodPut, k.authzPath(clientUUID, "/resource/"+r.ID), resourceBody(r))
}

func (k *Keycloak) DeleteResource(ctx context.Context, clientUUID, resourceID string) error {
	return k.write(ctx, http.MethodDelete, k.authzPath(clientUUID, "/resource/"+resourceID), nil)
}

func resourceBody(r Resource) map[string]any {
	scopes := make([]map[string]string, 0, len(AuthzScopes))
	for _, s := range AuthzScopes {
		scopes = append(scopes, map[string]string{"name": s})
	}
	return map[string]any{
		"name":        r.Name,
		"displayName": r.DisplayName,
		"type":        ElementResourceType,
		"scopes":      scopes,
		"attributes":  r.Attributes,
	}
}

// truncate bounds a Keycloak response body quoted into an error.
func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

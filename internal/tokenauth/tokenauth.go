// Package tokenauth verifies OIDC tokens offline: the signature against a JWKS
// cached in the store (so restarts work while the issuer is down), issuer,
// audience and lifetime with 60s skew, then the grants claim into a uns.Entry.
// The only network calls are the periodic refresh and a rate-limited refresh on
// an unknown kid.
package tokenauth

import (
	"crypto"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Reject reasons, used verbatim as metric labels.
const (
	ReasonBadToken = "bad_token" // malformed, bad signature, wrong alg, unknown kid after re-fetch
	ReasonExpired  = "expired"   // exp passed or nbf in the future (±60s skew)
	ReasonIssuer   = "issuer"    // iss or aud mismatch
	ReasonScope    = "scope"     // valid PAT, but not for this integration door
)

const (
	defaultRefresh = time.Hour
	// unknownKidMinInterval rate-limits the refetch an unknown kid triggers, so a
	// flood of bad tokens cannot turn into a flood of requests.
	unknownKidMinInterval = 5 * time.Minute
	clockSkew             = 60 * time.Second
)

// Config mirrors config.Auth (the config package stays yaml-only; the
// caller maps fields).
type Config struct {
	NotBefore int64 // reject tokens issued before a permanent trust handover
	Issuer    string
	Audience  string
	JWKSURL   string
	Refresh   time.Duration // 0 → 1h
}

// Verified is a successfully verified token.
type Verified struct {
	Entry            *uns.Entry // KindHuman, grants from colca_grants
	Sub              string
	Username         string // preferred_username, "" if absent
	Exp              time.Time
	Scopes           []string // empty for OIDC JWTs, explicit for personal access tokens
	Credential       string   // "oidc" or "pat"
	CredentialID     string   // PAT lookup id; empty for OIDC JWTs
	CredentialDigest string   // hash-only PAT verifier at CONNECT; empty for OIDC JWTs
}

// Metrics is the nil-safe observer surface (implemented by *metrics.Metrics
// once the families exist; nil in unit tests).
type Metrics interface {
	JWKSRefreshFailed()
	SetJWKSKeys(n int)
}

type Verifier struct {
	cfg    Config
	st     *store.Store
	client *http.Client
	log    *slog.Logger
	m      Metrics

	mu        sync.RWMutex
	keys      map[string]crypto.PublicKey
	lastFetch time.Time // last unknown-kid-triggered fetch attempt (rate limit)

	// groupsIdx resolves a token's group ids to grants. It is wired once the engine
	// exists; until then groups grant nothing.
	groupsMu  sync.RWMutex
	groupsIdx *uns.GroupIndex
	patIdx    *uns.PersonalAccessTokenIndex
}

// SetGroupIndex wires the group resolver (node startup).
func (v *Verifier) SetGroupIndex(idx *uns.GroupIndex) {
	v.groupsMu.Lock()
	v.groupsIdx = idx
	v.groupsMu.Unlock()
}

// SetPersonalAccessTokenIndex wires the hash-only credential definitions held
// by the engine after it has been constructed.
func (v *Verifier) SetPersonalAccessTokenIndex(idx *uns.PersonalAccessTokenIndex) {
	v.groupsMu.Lock()
	v.patIdx = idx
	v.groupsMu.Unlock()
}

func (v *Verifier) groups() *uns.GroupIndex {
	v.groupsMu.RLock()
	defer v.groupsMu.RUnlock()
	return v.groupsIdx
}

func (v *Verifier) personalAccessTokens() *uns.PersonalAccessTokenIndex {
	v.groupsMu.RLock()
	defer v.groupsMu.RUnlock()
	return v.patIdx
}

// New builds a verifier and loads the persisted JWKS, without network access. A
// node that starts offline with a persisted JWKS works.
func New(cfg Config, st *store.Store, m Metrics) (*Verifier, error) {
	if cfg.Issuer == "" || cfg.Audience == "" || cfg.JWKSURL == "" {
		return nil, fmt.Errorf("tokenauth: issuer, audience and jwks_url are all required")
	}
	if cfg.Refresh == 0 {
		cfg.Refresh = defaultRefresh
	}
	v := &Verifier{
		cfg:    cfg,
		st:     st,
		client: &http.Client{Timeout: fetchTimeout},
		log:    slog.Default().With("comp", "tokenauth"),
		m:      m,
	}
	if raw := st.JWKSGet(); raw != nil {
		keys, err := parseJWKS(raw)
		if err != nil {
			// A corrupt persisted document would silently lock every human out; fail loudly.
			return nil, fmt.Errorf("tokenauth: persisted JWKS is corrupt: %w", err)
		}
		v.keys = keys
		v.notifyKeyCount()
	}
	return v, nil
}

// Run refreshes the JWKS on the configured cadence until stop closes, starting
// immediately.
func (v *Verifier) Run(stop <-chan struct{}) {
	v.refresh()
	t := time.NewTicker(v.cfg.Refresh)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			v.refresh()
		}
	}
}

// refresh fetches, parses, persists and swaps the key set. On failure the current
// keys stay.
func (v *Verifier) refresh() {
	raw, err := fetchJWKS(v.client, v.cfg.JWKSURL)
	if err != nil {
		v.log.Warn("jwks refresh failed — keeping cached keys", "url", v.cfg.JWKSURL, "err", err)
		if v.m != nil {
			v.m.JWKSRefreshFailed()
		}
		return
	}
	keys, err := parseJWKS(raw)
	if err != nil {
		v.log.Warn("jwks refresh returned an unusable document — keeping cached keys", "err", err)
		if v.m != nil {
			v.m.JWKSRefreshFailed()
		}
		return
	}
	if err := v.st.JWKSPut(raw); err != nil {
		v.log.Warn("jwks persistence failed — keys active in-memory only", "err", err)
	}
	v.mu.Lock()
	v.keys = keys
	v.mu.Unlock()
	v.notifyKeyCount()
	v.log.Debug("jwks refreshed", "keys", len(keys))
}

func (v *Verifier) notifyKeyCount() {
	if v.m != nil {
		v.mu.RLock()
		n := len(v.keys)
		v.mu.RUnlock()
		v.m.SetJWKSKeys(n)
	}
}

// keyFor resolves a kid. On a miss it refetches once, rate-limited, since an
// unknown kid usually means the keys were rotated.
func (v *Verifier) keyFor(kid string) (crypto.PublicKey, bool) {
	v.mu.RLock()
	k, ok := v.keys[kid]
	v.mu.RUnlock()
	if ok {
		return k, true
	}
	v.mu.Lock()
	limited := time.Since(v.lastFetch) < unknownKidMinInterval
	if !limited {
		v.lastFetch = time.Now()
	}
	v.mu.Unlock()
	if limited {
		return nil, false
	}
	v.refresh()
	v.mu.RLock()
	k, ok = v.keys[kid]
	v.mu.RUnlock()
	return k, ok
}

// Verify checks the token and returns the verified identity, or a reason from
// the Reason* vocabulary.
func (v *Verifier) Verify(token string) (*Verified, string, error) {
	return v.VerifyForScope(token, "")
}

func personalAccessTokenReason(err error) string {
	reason := ReasonBadToken
	if strings.Contains(err.Error(), "expired") {
		reason = ReasonExpired
	} else if strings.Contains(err.Error(), "scope ") {
		reason = ReasonScope
	}
	return reason
}

// VerifyPersonalAccessTokenSession rechecks an established PAT session using
// only its non-secret lookup id. The local definition is authoritative, so a
// replicated tombstone or scope update takes effect without contacting the
// parent node.
func (v *Verifier) VerifyPersonalAccessTokenSession(
	id, authenticatedDigest, requiredScope string, now time.Time,
) (string, error) {
	idx := v.personalAccessTokens()
	if idx == nil {
		return ReasonBadToken, fmt.Errorf("personal access token verification is not wired")
	}
	if err := idx.AuthorizeSession(id, authenticatedDigest, requiredScope, now); err != nil {
		return personalAccessTokenReason(err), err
	}
	return "", nil
}

// VerifyForScope verifies OIDC JWTs as before and additionally accepts a
// replicated personal access token when it explicitly grants requiredScope.
func (v *Verifier) VerifyForScope(token, requiredScope string) (*Verified, string, error) {
	cutoff := v.cfg.NotBefore
	if cutoff > 0 {
		state, err := v.st.StandaloneGet()
		if err != nil || state == nil || !state.Ready {
			return nil, ReasonBadToken, fmt.Errorf("standalone identity handover is incomplete")
		}
		cutoff = state.Since
	}
	if strings.HasPrefix(token, "pk_pat_") {
		idx := v.personalAccessTokens()
		if idx == nil {
			return nil, ReasonBadToken, fmt.Errorf("personal access token verification is not wired")
		}
		record, entry, err := idx.Authenticate(token, requiredScope, time.Now())
		if err != nil {
			return nil, personalAccessTokenReason(err), fmt.Errorf("token rejected: %w", err)
		}
		expires := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
		if record.ExpiresAt != "" {
			expires, _ = time.Parse(time.RFC3339, record.ExpiresAt)
		}
		return &Verified{
			Entry: entry, Sub: record.OwnerSub, Username: record.OwnerEmail,
			Exp: expires, Scopes: append([]string(nil), record.Scopes...),
			Credential: "pat", CredentialID: record.ID, CredentialDigest: record.HashedSecret,
		}, "", nil
	}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256", "ES256"}), // allowlist; none/HS* die here
		jwt.WithLeeway(clockSkew),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(v.cfg.Issuer),
		jwt.WithAudience(v.cfg.Audience),
	)
	claims := jwt.MapClaims{}
	_, err := parser.ParseWithClaims(token, claims, func(t *jwt.Token) (interface{}, error) {
		kid, _ := t.Header["kid"].(string)
		key, ok := v.keyFor(kid)
		if !ok {
			return nil, fmt.Errorf("no key for kid %q", kid)
		}
		return key, nil
	})
	if err != nil {
		return nil, reasonFor(err), fmt.Errorf("token rejected: %w", err)
	}

	if cutoff > 0 {
		issued, err := claims.GetIssuedAt()
		if err != nil || issued == nil || issued.Unix() < cutoff {
			return nil, ReasonBadToken, fmt.Errorf("token predates standalone handover")
		}
	}
	sub, _ := claims["sub"].(string)
	exp, expErr := claims.GetExpirationTime()
	if expErr != nil || exp == nil {
		return nil, ReasonBadToken, fmt.Errorf("token rejected: unreadable exp")
	}
	grants := stringList(claims["colca_grants"])
	// Grants name system elements, which mean the same at every node, so they are
	// kept as they are. The groups claim is resolved against the _Group definitions
	// this node holds: membership lives in the identity provider, grants in the tree.
	entry, problems, err := uns.TokenEntryWithGroups(sub, grants, stringList(claims["groups"]), v.groups())
	if err != nil {
		return nil, ReasonBadToken, fmt.Errorf("token rejected: %w", err)
	}
	for _, problem := range problems {
		// Not fatal, and deliberately: one stale membership must cost the human
		// that group, not everything they hold.
		v.log.Warn("token: a group contributed no grants", "sub", sub, "err", problem)
	}
	username, _ := claims["preferred_username"].(string)
	entry.Username = username
	return &Verified{Entry: entry, Sub: sub, Username: username, Exp: exp.Time, Credential: "oidc"}, "", nil
}

// reasonFor maps golang-jwt validation errors onto the metric vocabulary.
func reasonFor(err error) string {
	switch {
	case jwtErrorIs(err, jwt.ErrTokenExpired), jwtErrorIs(err, jwt.ErrTokenNotValidYet):
		return ReasonExpired
	case jwtErrorIs(err, jwt.ErrTokenInvalidIssuer), jwtErrorIs(err, jwt.ErrTokenInvalidAudience):
		return ReasonIssuer
	default:
		return ReasonBadToken
	}
}

func jwtErrorIs(err, target error) bool { return err != nil && errors.Is(err, target) }

// stringList accepts the claim as a JSON array of strings (the mapper's
// multivalued form) or a single string; anything else is treated as absent.
func stringList(v any) []string {
	switch x := v.(type) {
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if x == "" {
			return nil
		}
		return []string{x}
	}
	return nil
}

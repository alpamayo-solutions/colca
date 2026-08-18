// Package tokenauth verifies human OIDC tokens OFFLINE (human-authz design
// §2): signature against a cached JWKS (persisted in the store so restarts
// while the issuer is unreachable keep validating), issuer/audience/lifetime
// checks with fixed 60s skew, and extraction of the colca_grants claim into
// an ephemeral uns.Entry. The issuer is never called on a request path — the
// only network I/O is the background refresh and the rate-limited
// refresh-on-unknown-kid.
package tokenauth

import (
	"crypto"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Reject reasons — EXACT metric label values (design §7).
const (
	ReasonBadToken = "bad_token" // malformed, bad signature, wrong alg, unknown kid after re-fetch
	ReasonExpired  = "expired"   // exp passed or nbf in the future (±60s skew)
	ReasonIssuer   = "issuer"    // iss or aud mismatch
)

const (
	defaultRefresh = time.Hour
	// unknownKidMinInterval rate-limits the synchronous re-fetch an unknown
	// kid triggers (§2.2): a garbage-token flood must not become an outbound
	// request flood.
	unknownKidMinInterval = 5 * time.Minute
	clockSkew             = 60 * time.Second
)

// Config mirrors config.Auth (the config package stays yaml-only; the
// caller maps fields).
type Config struct {
	Issuer   string
	Audience string
	JWKSURL  string
	Refresh  time.Duration // 0 → 1h
}

// Verified is a successfully verified token.
type Verified struct {
	Entry    *uns.Entry // KindHuman, grants from colca_grants
	Sub      string
	Username string // preferred_username, "" if absent
	Exp      time.Time
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

	// groupsIdx resolves a token's group ids to grants. Late-bound: the index
	// is a projection of records the engine holds, and the verifier is built
	// before the engine. Nil until wired, which resolves every group to nothing
	// — the fail-closed direction.
	groupsMu  sync.RWMutex
	groupsIdx *uns.GroupIndex
}

// SetGroupIndex wires the group resolver (node startup).
func (v *Verifier) SetGroupIndex(idx *uns.GroupIndex) {
	v.groupsMu.Lock()
	v.groupsIdx = idx
	v.groupsMu.Unlock()
}

func (v *Verifier) groups() *uns.GroupIndex {
	v.groupsMu.RLock()
	defer v.groupsMu.RUnlock()
	return v.groupsIdx
}

// New builds a verifier and loads the persisted JWKS if one exists. NO
// network happens here — the first fetch is Run's job (or the first unknown
// kid). A node starting offline with a persisted JWKS is fully functional.
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
			// Fail-loud: a corrupt persisted document would silently lock every
			// human out until the next successful fetch — name it instead.
			return nil, fmt.Errorf("tokenauth: persisted JWKS is corrupt: %w", err)
		}
		v.keys = keys
		v.notifyKeyCount()
	}
	return v, nil
}

// Run refreshes the JWKS on the configured cadence until stop closes. The
// first refresh happens immediately (a fresh node needs keys before the
// first human connects).
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

// refresh fetches, parses, persists and swaps the key set. A failure keeps
// the current keys — cached keys are only ever REPLACED by a good fetch.
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

// keyFor resolves a kid. On a miss it performs ONE rate-limited synchronous
// re-fetch (§2.2 refresh-on-unknown-kid: an unknown kid IS the rotation
// signal) and retries the lookup once.
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

// Verify checks the token per §2.2 order and returns the Verified identity,
// or a reject reason from the Reason* vocabulary.
func (v *Verifier) Verify(token string) (*Verified, string, error) {
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

	sub, _ := claims["sub"].(string)
	exp, expErr := claims.GetExpirationTime()
	if expErr != nil || exp == nil {
		return nil, ReasonBadToken, fmt.Errorf("token rejected: unreadable exp")
	}
	grants := stringList(claims["colca_grants"])
	// No translation: a grant names a system element, and an element id means
	// the same thing at every node (id-grants design §4). The frame arithmetic
	// this used to do at verification is a lookup at decision time now.
	//
	// The groups claim is where a human's authority normally comes from
	// (definition-stream design §8): the token names groups, the node resolves
	// them against the definitions its parent pushed down. Membership lives in
	// the identity provider; grants live in the tree.
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
	return &Verified{Entry: entry, Sub: sub, Username: username, Exp: exp.Time}, "", nil
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

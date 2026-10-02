// Package tokenauth verifies OIDC tokens offline: the issuer against the
// configured list, the signature against that issuer's JWKS cached in the store
// (so restarts work while the issuer is down), audience and lifetime with 60s
// skew, then the grants claim into a uns.Entry. The only network calls are the
// periodic refresh and the refresh an unknown kid triggers.
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
	ReasonBadToken  = "bad_token"  // malformed, bad signature, wrong alg, unknown kid after re-fetch
	ReasonExpired   = "expired"    // exp passed or nbf in the future (±60s skew)
	ReasonIssuer    = "issuer"     // iss not in the list, or aud mismatch
	ReasonScope     = "scope"      // valid PAT, but not for this integration door
	ReasonLoggedOut = "logged_out" // the identity provider ended the token's session
)

const (
	defaultRefresh = time.Hour
	// unknownKidMinInterval is the minimum time between an unknown-kid refetch
	// and the last one that succeeded, so a flood of bad tokens against a healthy
	// issuer cannot turn into a flood of requests. Failed fetches do not count:
	// while the key set is missing or stale, an unknown kid fetches at once
	// (one fetch in flight, short timeout).
	unknownKidMinInterval  = 10 * time.Second
	unknownKidFetchTimeout = 3 * time.Second
	clockSkew              = 60 * time.Second
)

// Config mirrors config.Auth (the config package stays yaml-only; the
// caller maps fields).
type Config struct {
	NotBefore int64 // reject tokens issued before a permanent trust handover
	Issuers   []Issuer
	Audience  string
	Refresh   time.Duration // 0 → 1h
}

// Issuer is one accepted `iss` value and the JWKS its keys are fetched from.
// Issuers that share a JWKS URL share one key set and one fetch.
type Issuer struct {
	ID      string
	JWKSURL string
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
	SessionID        string   // the identity provider's session (`sid`); empty if absent
	IssuedAt         time.Time
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
	quick  *http.Client // unknown-kid fetches, which a login waits for
	log    *slog.Logger
	m      Metrics

	refetchAfter time.Duration // unknownKidMinInterval, shortened in tests

	sources  []*jwksSource          // one per distinct JWKS URL, in config order
	byIssuer map[string]*jwksSource // iss → the key set its tokens are signed with

	// groupsIdx resolves a token's group ids to grants. It is wired once the engine
	// exists; until then groups grant nothing.
	groupsMu  sync.RWMutex
	groupsIdx *uns.GroupIndex
	patIdx    *uns.PersonalAccessTokenIndex

	unknownGroups uns.GroupNotices

	logouts logouts
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

// jwksSource is the key set served at one JWKS URL.
type jwksSource struct {
	url string

	mu       sync.RWMutex
	keys     map[string]crypto.PublicKey
	lastKid  time.Time     // last successful unknown-kid refetch (rate limit)
	failed   bool          // the last fetch of any kind failed
	inflight chan struct{} // closed when the running unknown-kid fetch ends
}

// New builds a verifier and loads the persisted JWKS documents, without network
// access. A node that starts offline with persisted JWKS works.
func New(cfg Config, st *store.Store, m Metrics) (*Verifier, error) {
	if cfg.Audience == "" || len(cfg.Issuers) == 0 {
		return nil, fmt.Errorf("tokenauth: audience and at least one issuer are required")
	}
	if cfg.Refresh == 0 {
		cfg.Refresh = defaultRefresh
	}
	v := &Verifier{
		cfg:      cfg,
		st:       st,
		client:   &http.Client{Timeout: fetchTimeout},
		quick:    &http.Client{Timeout: unknownKidFetchTimeout},
		log:      slog.Default().With("comp", "tokenauth"),
		m:        m,
		byIssuer: make(map[string]*jwksSource, len(cfg.Issuers)),

		refetchAfter: unknownKidMinInterval,
	}
	byURL := make(map[string]*jwksSource)
	for _, is := range cfg.Issuers {
		if is.ID == "" || is.JWKSURL == "" {
			return nil, fmt.Errorf("tokenauth: every issuer needs an id and a jwks_url")
		}
		if _, dup := v.byIssuer[is.ID]; dup {
			return nil, fmt.Errorf("tokenauth: issuer %q listed twice", is.ID)
		}
		src, ok := byURL[is.JWKSURL]
		if !ok {
			src = &jwksSource{url: is.JWKSURL}
			if raw := st.JWKSGet(is.JWKSURL); raw != nil {
				keys, err := parseJWKS(raw)
				if err != nil {
					// A corrupt persisted document would silently lock every human out; fail loudly.
					return nil, fmt.Errorf("tokenauth: persisted JWKS for %s is corrupt: %w", is.JWKSURL, err)
				}
				src.keys = keys
			}
			byURL[is.JWKSURL] = src
			v.sources = append(v.sources, src)
		}
		v.byIssuer[is.ID] = src
	}
	v.notifyKeyCount()
	return v, nil
}

// Run refreshes every JWKS on the configured cadence until stop closes,
// starting immediately.
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

// refresh fetches every JWKS source.
func (v *Verifier) refresh() {
	for _, src := range v.sources {
		v.fetchSource(src, v.client)
	}
}

// fetchSource fetches, parses, persists and swaps one key set. On failure the
// current keys stay. It reports whether the fetch succeeded.
func (v *Verifier) fetchSource(src *jwksSource, client *http.Client) bool {
	fail := func(msg string, err error) bool {
		v.log.Warn(msg, "url", src.url, "err", err)
		if v.m != nil {
			v.m.JWKSRefreshFailed()
		}
		src.mu.Lock()
		src.failed = true
		src.mu.Unlock()
		return false
	}
	raw, err := fetchJWKS(client, src.url)
	if err != nil {
		return fail("jwks refresh failed — keeping cached keys", err)
	}
	keys, err := parseJWKS(raw)
	if err != nil {
		return fail("jwks refresh returned an unusable document — keeping cached keys", err)
	}
	if err := v.st.JWKSPut(src.url, raw); err != nil {
		v.log.Warn("jwks persistence failed — keys active in-memory only", "url", src.url, "err", err)
	}
	src.mu.Lock()
	src.keys = keys
	src.failed = false
	src.mu.Unlock()
	v.notifyKeyCount()
	v.log.Debug("jwks refreshed", "url", src.url, "keys", len(keys))
	return true
}

// notifyKeyCount reports the keys cached across all sources.
func (v *Verifier) notifyKeyCount() {
	if v.m == nil {
		return
	}
	n := 0
	for _, src := range v.sources {
		src.mu.RLock()
		n += len(src.keys)
		src.mu.RUnlock()
	}
	v.m.SetJWKSKeys(n)
}

// keyFor resolves a kid in one source. On a miss it refetches that source and
// looks again, since an unknown kid usually means the keys were rotated or not
// loaded yet. Against a healthy key set the refetch is rate-limited by the last
// successful one; while no key set has loaded or the last fetch failed (the
// identity provider is starting or restarting), it fetches at once. Either way
// at most one fetch runs, and concurrent misses wait for it.
func (v *Verifier) keyFor(src *jwksSource, kid string) (crypto.PublicKey, bool) {
	src.mu.Lock()
	if k, ok := src.keys[kid]; ok {
		src.mu.Unlock()
		return k, true
	}
	if wait := src.inflight; wait != nil {
		src.mu.Unlock()
		<-wait
		return src.lookup(kid)
	}
	healthy := src.keys != nil && !src.failed
	if healthy && time.Since(src.lastKid) < v.refetchAfter {
		src.mu.Unlock()
		return nil, false
	}
	done := make(chan struct{})
	src.inflight = done
	src.mu.Unlock()

	ok := v.fetchSource(src, v.quick)

	src.mu.Lock()
	if ok {
		src.lastKid = time.Now()
	}
	src.inflight = nil
	src.mu.Unlock()
	close(done)
	return src.lookup(kid)
}

func (src *jwksSource) lookup(kid string) (crypto.PublicKey, bool) {
	src.mu.RLock()
	defer src.mu.RUnlock()
	k, ok := src.keys[kid]
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
		jwt.WithAudience(v.cfg.Audience),
	)
	claims := jwt.MapClaims{}
	_, err := parser.ParseWithClaims(token, claims, v.signingKey)
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
		var unknown *uns.UnknownGroupError
		if errors.As(problem, &unknown) {
			if v.unknownGroups.First(unknown.ID) {
				v.log.Info("token names a group this node does not define; it grants nothing here "+
					"(logged once per group)", "group", unknown.ID)
			}
			continue
		}
		v.log.Warn("token: a group contributed no grants", "sub", sub, "err", problem)
	}
	sid, _ := claims["sid"].(string)
	var issuedAt time.Time
	if iat, err := claims.GetIssuedAt(); err == nil && iat != nil {
		issuedAt = iat.Time
	}
	if v.logouts.covers(sid, sub, issuedAt, time.Now()) {
		return nil, ReasonLoggedOut, fmt.Errorf("token rejected: its session was logged out")
	}
	username, _ := claims["preferred_username"].(string)
	entry.Username = username
	return &Verified{
		Entry: entry, Sub: sub, Username: username, Exp: exp.Time, Credential: "oidc",
		SessionID: sid, IssuedAt: issuedAt,
	}, "", nil
}

// signingKey is the jwt.Keyfunc for every token this verifier accepts. The issuer
// picks the key set; the signature check that follows is what makes the
// unverified `iss` read here trustworthy.
func (v *Verifier) signingKey(t *jwt.Token) (interface{}, error) {
	iss, _ := t.Claims.GetIssuer()
	src, ok := v.byIssuer[iss]
	if !ok {
		return nil, fmt.Errorf("%w: %q is not an accepted issuer", jwt.ErrTokenInvalidIssuer, iss)
	}
	kid, _ := t.Header["kid"].(string)
	key, ok := v.keyFor(src, kid)
	if !ok {
		return nil, fmt.Errorf("no key for kid %q", kid)
	}
	return key, nil
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

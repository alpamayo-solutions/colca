package tokenauth

// Back-channel logout, per OpenID Connect Back-Channel Logout 1.0: the identity
// provider POSTs a signed logout token naming a session (`sid`) or a person
// (`sub`), and every token of that session stops working here at once instead of
// at its `exp`.

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// BackchannelLogoutEvent is the member the logout token's `events` claim must hold.
const BackchannelLogoutEvent = "http://schemas.openid.net/event/backchannel-logout"

const (
	// logoutRetention is how long a logged-out session stays refused. It must
	// outlast every access token issued for that session before the logout; access
	// tokens live minutes, so a day leaves a wide margin.
	logoutRetention = 24 * time.Hour
	// logoutMaxAge bounds the age of a logout token that carries no `exp`.
	logoutMaxAge = 5 * time.Minute
)

// ErrLogoutToken wraps every reason a logout token is refused.
var ErrLogoutToken = errors.New("invalid logout token")

// Logout is one verified back-channel logout.
type Logout struct {
	// SID is the ended session. Empty when the token names only a person.
	SID string
	// Sub is the person. With no SID, every session they opened up to IssuedAt ends.
	Sub      string
	IssuedAt time.Time
}

// Ends reports whether a token or session with this sid, sub and issue time
// belongs to what the logout ended. With a sid, that session and nothing else;
// with only a sub, everything that person was issued up to the logout.
func (l Logout) Ends(sid, sub string, issuedAt time.Time) bool {
	if l.SID != "" {
		return sid == l.SID
	}
	return sub == l.Sub && !issuedAt.After(l.IssuedAt)
}

// logouts remembers the logouts still in force, the logout tokens already used,
// and who wants to hear about a new logout.
type logouts struct {
	mu        sync.Mutex
	ended     map[Logout]time.Time // logout → when it is forgotten
	seen      map[string]time.Time // issuer + jti → when it is forgotten
	listeners []func(Logout)
}

// covers reports whether a logout in force covers this sid, sub and issue time.
func (l *logouts) covers(sid, sub string, issuedAt, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for logout, until := range l.ended {
		if now.Before(until) && logout.Ends(sid, sub, issuedAt) {
			return true
		}
	}
	return false
}

// record stores a logout unless its token was used before, and returns the
// listeners to tell. A replayed token is refused.
func (l *logouts) record(logout Logout, jtiKey string, jtiUntil, now time.Time) ([]func(Logout), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, until := range l.ended {
		if !now.Before(until) {
			delete(l.ended, key)
		}
	}
	for key, until := range l.seen {
		if !now.Before(until) {
			delete(l.seen, key)
		}
	}
	if _, used := l.seen[jtiKey]; used {
		return nil, fmt.Errorf("%w: jti was already used", ErrLogoutToken)
	}
	if l.seen == nil {
		l.seen = map[string]time.Time{}
		l.ended = map[Logout]time.Time{}
	}
	l.seen[jtiKey] = jtiUntil
	l.ended[logout] = now.Add(logoutRetention)
	return slices.Clone(l.listeners), nil
}

// LoggedOut reports whether a logout now in force ended the session this token
// belongs to. A door that stores a session after verifying its token asks again
// once it is stored, so a logout arriving in between is not missed.
func (v *Verifier) LoggedOut(t *Verified) bool {
	return t.Credential == "oidc" && v.logouts.covers(t.SessionID, t.Sub, t.IssuedAt, time.Now())
}

// OnLogout registers fn to run after every accepted back-channel logout, for the
// doors to end the sessions it covers.
func (v *Verifier) OnLogout(fn func(Logout)) {
	v.logouts.mu.Lock()
	v.logouts.listeners = append(v.logouts.listeners, fn)
	v.logouts.mu.Unlock()
}

// BackchannelLogout verifies a logout token and puts the logout in force: tokens
// of the ended session are refused from now on, and the OnLogout listeners run
// before it returns. The token is checked as the specification requires:
// signature from the issuer's JWKS, `iss` among the accepted issuers, `aud` this
// node's audience, `iat` present, `exp` (if present) not passed, a `sid` or a
// `sub`, the back-channel logout event in `events`, no `nonce`, and a `jti` not
// seen before.
func (v *Verifier) BackchannelLogout(token string) (Logout, error) {
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256", "ES256"}),
		jwt.WithLeeway(clockSkew),
		jwt.WithAudience(v.cfg.Audience),
		jwt.WithIssuedAt(),
	)
	claims := jwt.MapClaims{}
	if _, err := parser.ParseWithClaims(token, claims, v.signingKey); err != nil {
		return Logout{}, fmt.Errorf("%w: %w", ErrLogoutToken, err)
	}
	now := time.Now()
	iat, err := claims.GetIssuedAt()
	if err != nil || iat == nil {
		return Logout{}, fmt.Errorf("%w: no iat", ErrLogoutToken)
	}
	jtiUntil := now.Add(logoutMaxAge + clockSkew)
	if exp, err := claims.GetExpirationTime(); err == nil && exp != nil {
		jtiUntil = exp.Add(clockSkew)
	} else if now.Sub(iat.Time) > logoutMaxAge+clockSkew {
		return Logout{}, fmt.Errorf("%w: issued %s ago and carries no exp", ErrLogoutToken, now.Sub(iat.Time).Round(time.Second))
	}
	events, _ := claims["events"].(map[string]any)
	if _, ok := events[BackchannelLogoutEvent].(map[string]any); !ok {
		return Logout{}, fmt.Errorf("%w: events does not hold %s", ErrLogoutToken, BackchannelLogoutEvent)
	}
	if _, ok := claims["nonce"]; ok {
		return Logout{}, fmt.Errorf("%w: carries a nonce", ErrLogoutToken)
	}
	sid, _ := claims["sid"].(string)
	sub, _ := claims["sub"].(string)
	if sid == "" && sub == "" {
		return Logout{}, fmt.Errorf("%w: neither sid nor sub", ErrLogoutToken)
	}
	jti, _ := claims["jti"].(string)
	if jti == "" {
		return Logout{}, fmt.Errorf("%w: no jti", ErrLogoutToken)
	}
	iss, _ := claims.GetIssuer()
	logout := Logout{SID: sid, Sub: sub, IssuedAt: iat.Time}
	listeners, err := v.logouts.record(logout, iss+"\x00"+jti, jtiUntil, now)
	if err != nil {
		return Logout{}, err
	}
	v.log.Info("back-channel logout", "sid", sid, "sub", sub)
	for _, fn := range listeners {
		fn(logout)
	}
	return logout, nil
}

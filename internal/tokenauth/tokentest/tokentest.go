// Package tokentest is the fake OIDC issuer every Go test level uses
// (human-authz design §9): a generated RSA keypair, a static JWKS served over
// httptest, and token minting — hermetic, no containers. The REAL issuer path
// (Keycloak, mapper, aggregation) is proven once, in the level-4 system suite.
package tokentest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// MintOpts overrides for negative tests; zero values fall back to the
// issuer's correct parameters.
type MintOpts struct {
	Sub      string
	Grants   []string
	Exp      time.Time
	Nbf      time.Time
	Iss      string // override issuer
	Aud      string // override audience
	Alg      string // "RS256" (default) | "none" | "HS256"
	Kid      string // override key id
	WrongKey bool   // sign with a key the JWKS does not serve
	Username string // preferred_username
}

type Issuer struct {
	key      *rsa.PrivateKey
	wrongKey *rsa.PrivateKey
	kid      string
	iss, aud string
	srv      *httptest.Server
	requests atomic.Int64
}

// NewIssuer generates a keypair and serves its JWKS on a loopback httptest
// server. Close is registered on t.
func NewIssuer(t *testing.T) *Issuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	i := &Issuer{key: key, wrongKey: wrong, kid: "test-key-1",
		iss: "https://issuer.test/realms/colca", aud: "colca"}
	i.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i.requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(i.jwksJSON())
	}))
	t.Cleanup(i.srv.Close)
	return i
}

// Iss and Aud expose the issuer coordinates (for building tokenauth.Config —
// tokentest must not import tokenauth, the dependency points the other way).
func (i *Issuer) Iss() string     { return i.iss }
func (i *Issuer) Aud() string     { return i.aud }
func (i *Issuer) JWKSURL() string { return i.srv.URL }

// Requests returns how many JWKS fetches the server has answered — tests use
// it to pin the refresh-on-unknown-kid rate limit.
func (i *Issuer) Requests() int64 { return i.requests.Load() }

// CloseServer stops serving the JWKS (offline-issuer tests). Safe to call once.
func (i *Issuer) CloseServer() { i.srv.Close() }

// Rotate replaces the signing key AND the kid; the JWKS serves ONLY the new
// key (no overlap — the harsh rotation the refresh-on-unknown-kid heals).
func (i *Issuer) Rotate(t *testing.T) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	i.key = key
	i.kid = i.kid + "r"
}

// Mint issues a correctly signed token.
func (i *Issuer) Mint(sub string, grants []string, exp time.Time) string {
	return i.MintOpt(MintOpts{Sub: sub, Grants: grants, Exp: exp})
}

// MintOpt issues a token with overrides for the negative paths.
func (i *Issuer) MintOpt(o MintOpts) string {
	iss, aud, kid := i.iss, i.aud, i.kid
	if o.Iss != "" {
		iss = o.Iss
	}
	if o.Aud != "" {
		aud = o.Aud
	}
	if o.Kid != "" {
		kid = o.Kid
	}
	exp := o.Exp
	if exp.IsZero() {
		exp = time.Now().Add(5 * time.Minute)
	}
	claims := jwt.MapClaims{
		"iss": iss,
		"aud": aud,
		"sub": o.Sub,
		"exp": exp.Unix(),
		"iat": time.Now().Add(-time.Minute).Unix(),
	}
	if !o.Nbf.IsZero() {
		claims["nbf"] = o.Nbf.Unix()
	}
	if o.Grants != nil {
		claims["colca_grants"] = o.Grants
	}
	if o.Username != "" {
		claims["preferred_username"] = o.Username
	}

	switch o.Alg {
	case "none":
		tok := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
		tok.Header["kid"] = kid
		s, _ := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
		return s
	case "HS256":
		tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		tok.Header["kid"] = kid
		s, _ := tok.SignedString([]byte("hmac-secret"))
		return s
	default:
		tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
		tok.Header["kid"] = kid
		key := i.key
		if o.WrongKey {
			key = i.wrongKey
		}
		s, _ := tok.SignedString(key)
		return s
	}
}

// jwksJSON renders the current key as a JWKS document (RSA, base64url-raw).
func (i *Issuer) jwksJSON() []byte {
	n := base64.RawURLEncoding.EncodeToString(i.key.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(i.key.E)).Bytes())
	doc := map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "alg": "RS256", "use": "sig", "kid": i.kid, "n": n, "e": e,
	}}}
	raw, _ := json.Marshal(doc)
	return raw
}

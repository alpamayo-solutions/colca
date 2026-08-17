package tokenauth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"time"
)

// jwk is the subset of RFC 7517 colca understands: RSA (n, e) and EC P-256
// (crv, x, y) signature keys. Everything else is skipped — an unusable key in
// the document must not fail the usable ones.
type jwk struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type jwks struct {
	Keys []jwk `json:"keys"`
}

// parseJWKS converts a JWKS document into kid → public key. Unknown ktys and
// non-sig keys are skipped; a document yielding ZERO usable keys is an error
// (a verifier with no keys can only reject, better to say why).
func parseJWKS(raw []byte) (map[string]crypto.PublicKey, error) {
	var doc jwks
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("jwks: not valid JSON: %w", err)
	}
	keys := map[string]crypto.PublicKey{}
	for _, k := range doc.Keys {
		if k.Use != "" && k.Use != "sig" {
			continue
		}
		switch k.Kty {
		case "RSA":
			pub, err := rsaKey(k)
			if err != nil {
				return nil, fmt.Errorf("jwks: key %q: %w", k.Kid, err)
			}
			keys[k.Kid] = pub
		case "EC":
			if k.Crv != "P-256" {
				continue // ES256 is the only EC alg on the allowlist
			}
			pub, err := ecKey(k)
			if err != nil {
				return nil, fmt.Errorf("jwks: key %q: %w", k.Kid, err)
			}
			keys[k.Kid] = pub
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("jwks: document contains no usable RS256/ES256 signature keys")
	}
	return keys, nil
}

func rsaKey(k jwk) (*rsa.PublicKey, error) {
	n, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("bad n: %w", err)
	}
	e, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("bad e: %w", err)
	}
	if len(n) == 0 || len(e) == 0 {
		return nil, fmt.Errorf("empty modulus or exponent")
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}, nil
}

func ecKey(k jwk) (*ecdsa.PublicKey, error) {
	x, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil {
		return nil, fmt.Errorf("bad x: %w", err)
	}
	y, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil {
		return nil, fmt.Errorf("bad y: %w", err)
	}
	pub := &ecdsa.PublicKey{Curve: elliptic.P256(), X: new(big.Int).SetBytes(x), Y: new(big.Int).SetBytes(y)}
	if !pub.Curve.IsOnCurve(pub.X, pub.Y) {
		return nil, fmt.Errorf("point not on P-256")
	}
	return pub, nil
}

// fetchJWKS GETs the document with a bounded timeout.
func fetchJWKS(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("jwks fetch: HTTP %d from %s", resp.StatusCode, url)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

const fetchTimeout = 10 * time.Second

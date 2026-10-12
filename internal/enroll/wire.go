// Package enroll is node enrollment by request and approval (node enrollment
// spec §4–§8): a child with an unknown key asks its parent to join, a person
// approves, rejects or blocks the request (or decided before, with a
// pre-approval), and the parent issues the child a certificate signed with
// its own node key. The registry stays the authority on whether a key is
// admitted; the certificate records what was approved.
//
// The parent side is Manager: the pending store, the pre-approvals, the
// decisions, TPM attestation and the certificates. The child side is in
// package repl, which speaks the wire types of this file.
package enroll

import (
	"crypto"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmattest"
)

// RequestRoute is the route a child files its request at, on its parent's
// replication door. It is the only route there an unknown key may call.
const RequestRoute = "/enroll/request"

// Request is the body of POST /enroll/request. Everything in it is advisory
// except the attestation, which is verified: the key is the one the TLS peer
// proved it holds.
type Request struct {
	ULID           string `json:"ulid"`
	Name           string `json:"name,omitempty"`
	RequestedMount string `json:"requested_mount,omitempty"`
	// KeyStore is what the child claims for its key: file or tpm.
	KeyStore     string `json:"key_store,omitempty"`
	ColcaVersion string `json:"colca_version,omitempty"`
	// Attestation proves the TLS key lives in a TPM (§6). The parent answers
	// with a Challenge, and the child sends Activation in its next request.
	Attestation *tpmattest.Evidence `json:"attestation,omitempty"`
	Activation  []byte              `json:"activation,omitempty"`
	// KeyChange moves an enrolled child to a new key (§7.2). The request is
	// made with the current key.
	KeyChange *KeyChange `json:"key_change,omitempty"`
}

// KeyChange is a child's request to move its entry to a new key, signed by
// the new key (proof of possession) and sent over TLS with the current one.
type KeyChange struct {
	// Pubkey is the new key as SPKI hex.
	Pubkey   string `json:"pubkey"`
	KeyStore string `json:"key_store,omitempty"`
	// Timestamp is when the child signed, unix milliseconds.
	Timestamp int64 `json:"timestamp"`
	// Signature is the new key's signature over KeyChangeMessage.
	Signature   []byte              `json:"signature"`
	Attestation *tpmattest.Evidence `json:"attestation,omitempty"`
	Activation  []byte              `json:"activation,omitempty"`
}

// Response statuses.
const (
	StatusPending  = "pending"
	StatusApproved = "approved"
	StatusRejected = "rejected"
	StatusBlocked  = "blocked"
)

// Response is the parent's answer: 202 pending (with a Challenge while
// attestation is under way), 200 approved with the certificate chain, 403
// rejected or blocked.
type Response struct {
	Status      string `json:"status"`
	Fingerprint string `json:"fingerprint,omitempty"`
	// Certificate is the issued leaf followed by the parent's certificate, PEM.
	Certificate string `json:"certificate,omitempty"`
	NotAfter    string `json:"not_after,omitempty"`
	// KeyStore is the level the parent recorded for the key.
	KeyStore  string     `json:"key_store,omitempty"`
	Reason    string     `json:"reason,omitempty"`
	Challenge *Challenge `json:"challenge,omitempty"`
	// KeyChangeRequested asks the child to move to a new key now.
	KeyChangeRequested bool `json:"key_change_requested,omitempty"`
}

// Challenge is the credential the parent made for the attested key's AK and
// EK; only the TPM holding both opens it (tpmattest.ActivateCredential).
type Challenge struct {
	CredentialBlob  []byte `json:"credential_blob"`
	EncryptedSecret []byte `json:"encrypted_secret"`
}

// QualifyingData binds TPM evidence to the parent it is meant for: SHA-256 of
// the parent key's SPKI DER. The child computes it from its pin.
func QualifyingData(parentSPKIHex string) ([]byte, error) {
	raw, err := hex.DecodeString(parentSPKIHex)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	return sum[:], nil
}

// KeyChangeMessage is what the new key signs: the child's ULID, the new key,
// the parent's key (all SPKI hex) and the time, as canonical JSON.
func KeyChangeMessage(ulid, newSPKIHex, parentSPKIHex string, timestamp int64) []byte {
	b, _ := json.Marshal(struct {
		Purpose string `json:"purpose"`
		ULID    string `json:"ulid"`
		New     string `json:"new_pubkey"`
		Parent  string `json:"parent_pubkey"`
		TS      int64  `json:"timestamp"`
	}{"colca key change", ulid, newSPKIHex, parentSPKIHex, timestamp})
	return b
}

// SignKeyChange signs the key change message with the new key.
func SignKeyChange(newKey crypto.Signer, ulid, parentSPKIHex string, timestamp int64) (*KeyChange, error) {
	h, err := pubkey.Hex(newKey.Public())
	if err != nil {
		return nil, err
	}
	sig, err := signMessage(newKey, KeyChangeMessage(ulid, h, parentSPKIHex, timestamp))
	if err != nil {
		return nil, fmt.Errorf("sign key change: %w", err)
	}
	return &KeyChange{Pubkey: h, Timestamp: timestamp, Signature: sig}, nil
}

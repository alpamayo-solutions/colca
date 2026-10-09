// Node enrollment records: a child's request to join at its parent, a
// pre-approval of one that has not asked yet, and the key store levels both
// speak about. The holding node writes them itself, mirrored as retained
// entities, so every ancestor (and the hub UI above them) sees what waits for
// a decision anywhere in the tree. Nobody else may publish them.

package uns

// The enrollment mirror contracts. Like _EnrolledIdentity they are built in:
// only the node that holds the record writes it, and a bundle may not
// redeclare them.
const (
	EnrollmentRequestContract     = "_EnrollmentRequest"
	EnrollmentPreapprovalContract = "_EnrollmentPreapproval"
)

// IsRegistryContract reports whether contract is one of the records a node
// writes about its own registry and enrollment stores: _EnrolledIdentity,
// _EnrollmentRequest and _EnrollmentPreapproval. No door accepts them from a
// client, not even the admin token's.
func IsRegistryContract(contract string) bool {
	switch contract {
	case "_EnrolledIdentity", EnrollmentRequestContract, EnrollmentPreapprovalContract:
		return true
	}
	return false
}

// Key store levels, from weakest to strongest. KeyStoreTPM is what a node
// claims; KeyStoreTPMAttested is what it proved to its parent with its
// endorsement key.
const (
	KeyStoreFile        = "file"
	KeyStoreTPM         = "tpm"
	KeyStoreTPMAttested = "tpm-attested"
)

// KeyStoreRank orders key store levels for policy checks; an unknown level
// ranks lowest. "any" ranks like a file key, so "any" is met by everything.
func KeyStoreRank(level string) int {
	switch level {
	case KeyStoreTPMAttested:
		return 2
	case KeyStoreTPM:
		return 1
	}
	return 0
}

// Certificate states of a child node's entry (Entry.CertState).
const (
	CertStateNone   = "none"
	CertStateIssued = "issued"
)

// Enrollment request states.
const (
	RequestPending  = "pending"
	RequestRejected = "rejected"
	RequestBlocked  = "blocked"
)

// Pre-approval states as the holder reports them.
const (
	PreapprovalOpen    = "open"
	PreapprovalUsed    = "used"
	PreapprovalExpired = "expired"
)

// EnrollmentAttestation is what TPM attestation established about a request's
// key: the endorsement key's fingerprint and, with a verified certificate, its
// manufacturer and serial.
type EnrollmentAttestation struct {
	EK             string `json:"ek,omitempty"`
	EKManufacturer string `json:"ek_manufacturer,omitempty"`
	EKSerial       string `json:"ek_serial,omitempty"`
}

// EnrollmentKeyChange marks a request that would move an enrolled node to a
// new key: the key it holds now.
type EnrollmentKeyChange struct {
	CurrentFingerprint string `json:"current_fingerprint"`
	CurrentKeyStore    string `json:"current_key_store,omitempty"`
}

// EnrollmentRequest is one key asking to join at this node: the stored p/{fp}
// value and the payload of its retained _EnrollmentRequest mirror. Times are
// RFC 3339 UTC.
type EnrollmentRequest struct {
	Fingerprint    string                 `json:"fingerprint"`
	Pubkey         string                 `json:"pubkey"`
	KeyStore       string                 `json:"key_store"`
	Attestation    *EnrollmentAttestation `json:"attestation,omitempty"`
	ULID           string                 `json:"ulid,omitempty"`
	Name           string                 `json:"name,omitempty"`
	RequestedMount string                 `json:"requested_mount,omitempty"`
	ColcaVersion   string                 `json:"colca_version,omitempty"`
	SourceIP       string                 `json:"source_ip,omitempty"`
	FirstSeen      string                 `json:"first_seen"`
	LastSeen       string                 `json:"last_seen"`
	Count          int                    `json:"count"`
	State          string                 `json:"state"`
	KeyChange      *EnrollmentKeyChange   `json:"key_change,omitempty"`
	// BelowPolicy is the level enrollment.require asks for when this request's
	// key store is below it: it cannot be approved, and the UI says why.
	BelowPolicy string `json:"below_policy,omitempty"`
	// PreapprovalHint is set when the request matched a pre-approval that had
	// expired or was used up, so it fell back to pending.
	PreapprovalHint string `json:"preapproval_hint,omitempty"`
	DecidedBy       string `json:"decided_by,omitempty"`
	DecidedAt       string `json:"decided_at,omitempty"`
	Reason          string `json:"reason,omitempty"`
}

// EnrollmentMatch names the identity a pre-approval expects: exactly one of the
// TPM's endorsement key or the node key, each as a SHA256:… fingerprint.
type EnrollmentMatch struct {
	EK  string `json:"ek,omitempty"`
	Key string `json:"key,omitempty"`
}

// EnrollmentPreapproval is a decision made before the device asked: the stored
// a/{id} value and the payload of its retained _EnrollmentPreapproval mirror.
type EnrollmentPreapproval struct {
	ID        string          `json:"id"`
	Match     EnrollmentMatch `json:"match"`
	Element   string          `json:"element,omitempty"`
	Mount     string          `json:"mount,omitempty"`
	Name      string          `json:"name,omitempty"`
	ExpiresAt string          `json:"expires_at"`
	// Uses is how many more requests it admits.
	Uses      int      `json:"uses"`
	UsedBy    []string `json:"used_by"`
	State     string   `json:"state"`
	CreatedBy string   `json:"created_by,omitempty"`
	CreatedAt string   `json:"created_at"`
	Note      string   `json:"note,omitempty"`
	// ClosedAt is when it was used up or expired; the record is kept for a
	// while after that so people see what it admitted.
	ClosedAt string `json:"closed_at,omitempty"`
}

// EnrollmentRequestPath is the KV path of the mirror of the request filed with
// the key of fingerprint fp (display form, SHA256:…).
func EnrollmentRequestPath(fp string) string { return "_colca/enrollment/" + fp }

// EnrollmentPreapprovalPath is the KV path of a pre-approval's mirror.
func EnrollmentPreapprovalPath(id string) string { return "_colca/enrollment-preapprovals/" + id }

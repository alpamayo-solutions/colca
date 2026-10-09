package enroll

import (
	"crypto"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity/pubkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmattest"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Limits of the pending store (§4.1).
const (
	MaxPending         = 200
	PendingTTL         = 7 * 24 * time.Hour
	RejectedTTL        = 7 * 24 * time.Hour
	NewKeysPerIPHour   = 10
	PreapprovalTTL     = 30 * 24 * time.Hour
	PreapprovalKeep    = 7 * 24 * time.Hour // a used-up or expired pre-approval stays listed this long
	ChallengeTTL       = 5 * time.Minute
	KeyChangeClockSkew = 10 * time.Minute
)

// FindingReason is the _Finding.reason this node raises while requests wait
// for a decision, so the alarm and notification path tells somebody.
const FindingReason = "enrollment_requests"

// FindingAuthor writes the finding.
const FindingAuthor = "colca-enrollment"

// Errors of the decision API, mapped to HTTP and _Ack codes by the doors.
var (
	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("conflict")
	ErrInvalid     = errors.New("invalid")
	ErrBelowPolicy = errors.New("below the enrollment policy")
)

// Actor is the person (or service acting for one) who decides.
type Actor struct{ ID, Label, Kind string }

func (a Actor) name() string {
	if a.Label != "" {
		return a.Label
	}
	if a.ID != "" {
		return a.ID
	}
	return "admin"
}

type challenge struct {
	secret       []byte
	ek           string
	chainOK      bool
	manufacturer string
	serial       string
	expires      time.Time
}

// Manager is a node's side of enrollment: the requests its children file, the
// pre-approvals people made, the decisions and the certificates.
type Manager struct {
	st         *store.Store
	reg        *registry.Manager
	issuer     *Issuer
	parentSPKI string
	qd         []byte
	policy     config.Enrollment
	roots      *TPMRoots
	nodeID     string
	log        *slog.Logger
	now        func() time.Time

	mu           sync.Mutex
	requests     map[string]*uns.EnrollmentRequest // fingerprint ID → record
	preapprovals map[string]*uns.EnrollmentPreapproval
	challenges   map[string]*challenge
	newByIP      map[string][]time.Time
	findingSig   string

	deliver  func(topic string, payload []byte, retain bool)
	author   func(path string) (string, error)
	audit    func(engine.AuditDenial)
	finding  func(topic string, payload []byte) error
	elements uns.Namespace
}

// Options configure a Manager.
type Options struct {
	NodeULID string
	// Signer is this node's key, which issues certificates.
	Signer crypto.Signer
	Policy config.Enrollment
	Roots  *TPMRoots
	Now    func() time.Time
}

// New loads the pending store and the pre-approvals.
func New(st *store.Store, reg *registry.Manager, o Options) (*Manager, error) {
	issuer, err := NewIssuer(o.Signer, o.NodeULID)
	if err != nil {
		return nil, err
	}
	parentSPKI, err := pubkey.Hex(o.Signer.Public())
	if err != nil {
		return nil, err
	}
	qd, _ := QualifyingData(parentSPKI)
	now := o.Now
	if now == nil {
		now = time.Now
	}
	issuer.now = now
	m := &Manager{
		st: st, reg: reg, issuer: issuer, parentSPKI: parentSPKI, qd: qd,
		policy: o.Policy, roots: o.Roots, nodeID: o.NodeULID, now: now,
		log:          slog.Default().With("node", o.NodeULID, "comp", "enroll"),
		requests:     map[string]*uns.EnrollmentRequest{},
		preapprovals: map[string]*uns.EnrollmentPreapproval{},
		challenges:   map[string]*challenge{},
		newByIP:      map[string][]time.Time{},
	}
	reqs, err := st.EnrollmentScan(store.EnrollmentRequests)
	if err != nil {
		return nil, fmt.Errorf("enrollment requests: %w", err)
	}
	for id, raw := range reqs {
		var r uns.EnrollmentRequest
		if err := json.Unmarshal(raw, &r); err != nil {
			return nil, fmt.Errorf("enrollment request %s: %w", id, err)
		}
		m.requests[id] = &r
	}
	pres, err := st.EnrollmentScan(store.EnrollmentPreapprovals)
	if err != nil {
		return nil, fmt.Errorf("enrollment pre-approvals: %w", err)
	}
	for id, raw := range pres {
		var p uns.EnrollmentPreapproval
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("enrollment pre-approval %s: %w", id, err)
		}
		m.preapprovals[id] = &p
	}
	// A finding a previous run left standing is rewritten or retired on the
	// first update.
	topic := m.findingTopic()
	if _, ok, _ := st.KVGet(FindingReason, o.NodeULID, topic); ok {
		m.findingSig = "\x00unknown"
	}
	return m, nil
}

// SetDeliver wires the local-bus mirror of the retained records.
func (m *Manager) SetDeliver(fn func(topic string, payload []byte, retain bool)) { m.deliver = fn }

// SetAuthoring wires element authoring for decisions that name a mount.
func (m *Manager) SetAuthoring(fn func(path string) (string, error)) { m.author = fn }

// SetAudit wires the audit writer.
func (m *Manager) SetAudit(fn func(engine.AuditDenial)) { m.audit = fn }

// SetFinding wires the publisher of the pending-requests finding.
func (m *Manager) SetFinding(fn func(topic string, payload []byte) error) { m.finding = fn }

// SetElements wires the element resolver used to check pre-approval targets.
func (m *Manager) SetElements(ns uns.Namespace) { m.elements = ns }

// Issuer is the certificate issuer, which the replication door checks
// presented certificates against.
func (m *Manager) Issuer() *Issuer { return m.issuer }

// RequireIssuedCert reports enrollment.require_issued_cert.
func (m *Manager) RequireIssuedCert() bool { return m.policy.RequireIssuedCert }

func (m *Manager) stamp(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func parseStamp(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// fingerprintOf returns the display and ID forms of a hex key's fingerprint.
func fingerprintOf(spkiHex string) (display, id string, err error) {
	display, err = pubkey.FingerprintHex(spkiHex)
	if err != nil {
		return "", "", err
	}
	id, err = pubkey.ParseFingerprint(display)
	return display, id, err
}

// displayFingerprint turns an ID-form fingerprint back into the display form.
func displayFingerprint(id string) string {
	h := strings.ToUpper(id)
	var sb strings.Builder
	sb.WriteString("SHA256:")
	for i := 0; i+1 < len(h); i += 2 {
		if i > 0 {
			sb.WriteByte(':')
		}
		sb.WriteString(h[i : i+2])
	}
	return sb.String()
}

// belowPolicy is the level enrollment.require asks for when level is below it.
func (m *Manager) belowPolicy(level string) string {
	want := m.policy.EffectiveRequire()
	if want == config.EnrollmentRequireAny {
		return ""
	}
	if uns.KeyStoreRank(level) < uns.KeyStoreRank(want) {
		return want
	}
	return ""
}

// --- persistence -------------------------------------------------------------

func (m *Manager) requestTopic(fp string) string {
	return uns.Prefix() + uns.EnrollmentRequestContract + "/" + m.nodeID + "/" + uns.EnrollmentRequestPath(fp)
}

func (m *Manager) preapprovalTopic(id string) string {
	return uns.Prefix() + uns.EnrollmentPreapprovalContract + "/" + m.nodeID + "/" + uns.EnrollmentPreapprovalPath(id)
}

func (m *Manager) findingTopic() string {
	return uns.Prefix() + "_Finding/" + m.nodeID + "/" + FindingReason
}

func (m *Manager) putRequestLocked(id string, r *uns.EnrollmentRequest) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return err
	}
	topic := m.requestTopic(r.Fingerprint)
	if _, err := m.st.EnrollmentPut(store.EnrollmentRequests, id, raw, "entities", store.Record{
		Topic: topic, Payload: raw, TS: m.now().UnixMilli(),
		KVPath: uns.EnrollmentRequestPath(r.Fingerprint), KVNode: m.nodeID,
	}); err != nil {
		return fmt.Errorf("store enrollment request: %w", err)
	}
	m.requests[id] = r
	if m.deliver != nil {
		m.deliver(topic, raw, true)
	}
	return nil
}

func (m *Manager) dropRequestLocked(id string) error {
	r, ok := m.requests[id]
	if !ok {
		return nil
	}
	topic := m.requestTopic(r.Fingerprint)
	if _, err := m.st.EnrollmentDelete(store.EnrollmentRequests, id, "entities", store.Record{
		Topic: topic, TS: m.now().UnixMilli(), KVPath: uns.EnrollmentRequestPath(r.Fingerprint), KVNode: m.nodeID,
	}); err != nil {
		return fmt.Errorf("remove enrollment request: %w", err)
	}
	delete(m.requests, id)
	if m.deliver != nil {
		m.deliver(topic, nil, true)
	}
	return nil
}

func (m *Manager) putPreapprovalLocked(p *uns.EnrollmentPreapproval) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	topic := m.preapprovalTopic(p.ID)
	if _, err := m.st.EnrollmentPut(store.EnrollmentPreapprovals, p.ID, raw, "entities", store.Record{
		Topic: topic, Payload: raw, TS: m.now().UnixMilli(),
		KVPath: uns.EnrollmentPreapprovalPath(p.ID), KVNode: m.nodeID,
	}); err != nil {
		return fmt.Errorf("store pre-approval: %w", err)
	}
	m.preapprovals[p.ID] = p
	if m.deliver != nil {
		m.deliver(topic, raw, true)
	}
	return nil
}

func (m *Manager) dropPreapprovalLocked(id string) error {
	topic := m.preapprovalTopic(id)
	if _, err := m.st.EnrollmentDelete(store.EnrollmentPreapprovals, id, "entities", store.Record{
		Topic: topic, TS: m.now().UnixMilli(), KVPath: uns.EnrollmentPreapprovalPath(id), KVNode: m.nodeID,
	}); err != nil {
		return fmt.Errorf("remove pre-approval: %w", err)
	}
	delete(m.preapprovals, id)
	if m.deliver != nil {
		m.deliver(topic, nil, true)
	}
	return nil
}

func (m *Manager) auditLocked(op, outcome string, a Actor, entityType, entityID string, meta map[string]any) {
	if m.audit == nil {
		return
	}
	kind := a.Kind
	if kind == "" {
		kind = "admin"
	}
	m.audit(engine.AuditDenial{
		Operation: op, ReasonCode: outcome,
		ActorID: a.ID, ActorLabel: a.Label, ActorKind: kind,
		EntityType: entityType, EntityID: entityID, Metadata: meta,
	})
}

// updateFindingLocked publishes the pending-requests finding when the set of
// pending requests changed, and retires it when none is left.
func (m *Manager) updateFindingLocked() {
	if m.finding == nil {
		return
	}
	var pending []*uns.EnrollmentRequest
	for _, r := range m.requests {
		if r.State == uns.RequestPending {
			pending = append(pending, r)
		}
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].Fingerprint < pending[j].Fingerprint })
	names := make([]string, len(pending))
	detail := make([]map[string]any, len(pending))
	for i, r := range pending {
		names[i] = r.Fingerprint
		detail[i] = map[string]any{
			"fingerprint": r.Fingerprint, "ulid": r.ULID, "name": r.Name,
			"key_store": r.KeyStore, "key_change": r.KeyChange != nil,
		}
	}
	sig := strings.Join(names, ",")
	if sig == m.findingSig {
		return
	}
	topic := m.findingTopic()
	var payload []byte
	if len(pending) > 0 {
		payload, _ = json.Marshal(map[string]any{
			"reason":             FindingReason,
			"summary":            fmt.Sprintf("%d node(s) ask to join this node and wait for a decision", len(pending)),
			"observed_at":        float64(m.now().UnixMilli()) / 1000,
			"suggested_severity": "warning",
			"detail":             map[string]any{"requests": detail},
			"remedy": "Compare each fingerprint with the device (colcad identity, or /healthz of the node) " +
				"and approve, reject or block the request in the hub's Admin app, with prekit node approve, " +
				"or through POST /enroll/requests/{fingerprint}/approve.",
		})
	}
	if err := m.finding(topic, payload); err != nil {
		m.log.Warn("enrollment finding not written", "err", err)
		return
	}
	m.findingSig = sig
}

// --- the request route ---------------------------------------------------------

func reply(code int, r Response) (int, Response) { return code, r }

// HandleRequest answers one POST /enroll/request. leafPub is the key the TLS
// peer proved it holds; source is the peer's address for the per-source limit.
func (m *Manager) HandleRequest(leafPub crypto.PublicKey, req Request, source string) (int, Response) {
	peerHex, err := pubkey.Hex(leafPub)
	if err != nil {
		return reply(400, Response{Reason: "unsupported key: " + err.Error()})
	}
	fp, fpID, err := fingerprintOf(peerHex)
	if err != nil {
		return reply(400, Response{Reason: err.Error()})
	}
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.requests[fpID]; ok && r.State == uns.RequestBlocked {
		return reply(403, Response{Status: StatusBlocked, Fingerprint: fp, Reason: r.Reason})
	}
	entry, active := m.reg.ByPubkey(peerHex)
	if active && !entry.ReplicatesUp() {
		return reply(403, Response{Status: StatusRejected, Fingerprint: fp,
			Reason: fmt.Sprintf("this key is enrolled here as %s %s, not as a node", entry.Kind, entry.ULID)})
	}
	if req.KeyChange != nil && active {
		return m.keyChangeLocked(entry, req, source, now)
	}
	if req.KeyChange != nil {
		// The current key is not enrolled here. Perhaps the swap already
		// happened and the child did not hear the answer: then the new key is
		// active under the child's ULID, and it gets its certificate.
		if code, resp, ok := m.finishedKeyChangeLocked(req, now); ok {
			return code, resp
		}
	}
	if active {
		level, att, ch := m.attestLocked(fpID, leafPub, req.Attestation, req.Activation, req.KeyStore, now)
		if ch != nil {
			return reply(202, Response{Status: StatusPending, Fingerprint: fp, Challenge: ch})
		}
		return m.issueLocked(entry, leafPub, level, att, now)
	}
	return m.newRequestLocked(peerHex, fp, fpID, leafPub, req, source, now)
}

// attestLocked runs the attestation exchange for key (§6). Without evidence the
// level is what the child claims, at most tpm. With evidence that verifies, it
// returns a challenge; with the challenge's secret, the proven level and the
// endorsement key it was proven with.
func (m *Manager) attestLocked(fpID string, key crypto.PublicKey, ev *tpmattest.Evidence, activation []byte,
	claimed string, now time.Time,
) (string, *uns.EnrollmentAttestation, *Challenge) {
	if c, ok := m.challenges[fpID]; ok && len(activation) > 0 {
		delete(m.challenges, fpID)
		if now.Before(c.expires) && subtle.ConstantTimeCompare(c.secret, activation) == 1 {
			att := &uns.EnrollmentAttestation{EK: c.ek}
			level := uns.KeyStoreTPM
			if c.chainOK {
				level = uns.KeyStoreTPMAttested
				att.EKManufacturer, att.EKSerial = c.manufacturer, c.serial
			}
			return level, att, nil
		}
		m.log.Warn("attestation: the credential was not activated by the TPM it was made for", "fingerprint", displayFingerprint(fpID))
	}
	if ev != nil {
		v, err := tpmattest.VerifyEvidence(ev, key, m.qd)
		if err != nil {
			m.log.Warn("attestation evidence refused; the key store stays a claim", "fingerprint", displayFingerprint(fpID), "err", err)
			return claimedLevel(claimed), nil, nil
		}
		c := &challenge{ek: v.EKFingerprint, expires: now.Add(ChallengeTTL)}
		if v.EKCert != nil {
			man, serial, err := m.roots.Verify(v.EKCert, now)
			if err == nil {
				c.chainOK, c.manufacturer, c.serial = true, man, serial
			} else {
				m.log.Info("attestation: the endorsement key certificate does not chain to a known TPM manufacturer; the key can be proven tpm, not tpm-attested",
					"fingerprint", displayFingerprint(fpID), "reason", err.Error())
			}
		}
		c.secret = make([]byte, 32)
		if _, err := rand.Read(c.secret); err != nil {
			return claimedLevel(claimed), nil, nil
		}
		blob, enc, err := tpmattest.MakeCredential(v, c.secret)
		if err != nil {
			m.log.Warn("attestation: no credential made", "err", err)
			return claimedLevel(claimed), nil, nil
		}
		m.challenges[fpID] = c
		return "", nil, &Challenge{CredentialBlob: blob, EncryptedSecret: enc}
	}
	return claimedLevel(claimed), nil, nil
}

func claimedLevel(claimed string) string {
	if claimed == uns.KeyStoreTPM {
		return uns.KeyStoreTPM
	}
	return uns.KeyStoreFile
}

// issueLocked issues a certificate for an active entry's key and records it.
func (m *Manager) issueLocked(entry *uns.Entry, pub crypto.PublicKey, level string,
	att *uns.EnrollmentAttestation, now time.Time,
) (int, Response) {
	leaf := Leaf{ULID: entry.ULID, Element: entry.Element, Level: level, Pub: pub}
	if att != nil {
		leaf.EKManufacturer, leaf.EKSerial = att.EKManufacturer, att.EKSerial
	}
	chain, notAfter, err := m.issuer.Issue(leaf)
	if err != nil {
		m.log.Error("certificate not issued", "ulid", entry.ULID, "err", err)
		return reply(500, Response{Reason: "certificate not issued"})
	}
	first := entry.CertState != uns.CertStateIssued
	updated, err := m.reg.Update(entry.ULID, func(e *uns.Entry) error {
		e.CertState = uns.CertStateIssued
		e.CertNotAfter = m.stamp(notAfter)
		e.KeyStore = level
		e.LastSeen = m.stamp(now)
		e.EKManufacturer, e.EKSerial = "", ""
		if att != nil {
			e.EKManufacturer, e.EKSerial = att.EKManufacturer, att.EKSerial
		}
		return nil
	})
	if err != nil {
		m.log.Error("certificate issued but not recorded", "ulid", entry.ULID, "err", err)
		return reply(500, Response{Reason: "certificate not recorded"})
	}
	if first {
		m.log.Info("first certificate issued; self-signed certificates are refused for this node from now on",
			"ulid", entry.ULID, "fingerprint", updated.Fingerprint, "key_store", level, "not_after", notAfter)
	} else {
		m.log.Debug("certificate renewed", "ulid", entry.ULID, "key_store", level, "not_after", notAfter)
	}
	return reply(200, Response{
		Status: StatusApproved, Fingerprint: updated.Fingerprint, Certificate: string(chain),
		NotAfter: m.stamp(notAfter), KeyStore: level, KeyChangeRequested: updated.KeyChangeRequested,
	})
}

// newRequestLocked handles a key this node does not know: pre-approved, or
// pending until a person decides.
func (m *Manager) newRequestLocked(peerHex, fp, fpID string, pub crypto.PublicKey, req Request, source string,
	now time.Time,
) (int, Response) {
	if err := validULID(req.ULID); err != nil {
		return reply(400, Response{Fingerprint: fp, Reason: err.Error()})
	}
	rec, known := m.requests[fpID]
	// A source that filed NewKeysPerIPHour new keys within the hour gets no
	// more stored. A pre-approved key is not stored either, so it still joins.
	limited := false
	if !known {
		recent := m.newByIP[source][:0]
		for _, t := range m.newByIP[source] {
			if now.Sub(t) < time.Hour {
				recent = append(recent, t)
			}
		}
		m.newByIP[source] = recent
		limited = len(recent) >= NewKeysPerIPHour
	}

	level, att, ch := m.attestLocked(fpID, pub, req.Attestation, req.Activation, req.KeyStore, now)
	if ch != nil && known {
		level = rec.KeyStore // unchanged until the challenge is answered
	} else if ch != nil {
		level = claimedLevel(req.KeyStore)
	}

	var r uns.EnrollmentRequest
	if known {
		r = *rec
	} else {
		r = uns.EnrollmentRequest{Fingerprint: fp, State: uns.RequestPending, FirstSeen: m.stamp(now)}
	}
	r.Pubkey = peerHex
	r.KeyStore = level
	if att != nil {
		r.Attestation = att
	} else if ch == nil && level != uns.KeyStoreTPMAttested {
		r.Attestation = nil
	}
	r.ULID, r.Name, r.RequestedMount, r.ColcaVersion = req.ULID, req.Name, req.RequestedMount, req.ColcaVersion
	r.SourceIP = source
	r.LastSeen = m.stamp(now)
	r.Count++
	r.BelowPolicy = m.belowPolicy(level)
	r.KeyChange = nil
	if existing, ok := m.reg.Get(req.ULID); ok && existing.Pubkey != peerHex {
		if !existing.ReplicatesUp() {
			return reply(409, Response{Fingerprint: fp,
				Reason: fmt.Sprintf("ulid %s is enrolled here as %s", req.ULID, existing.Kind)})
		}
		// The same node under another key: a re-flashed device, shown as a key
		// change of that entry, never as a new node (§4.1).
		r.KeyChange = &uns.EnrollmentKeyChange{CurrentFingerprint: existing.Fingerprint, CurrentKeyStore: existing.KeyStore}
	}

	if ch != nil {
		if !limited {
			if err := m.storeNewLocked(fpID, &r, known, source, now); err != nil {
				return reply(500, Response{Reason: err.Error()})
			}
		}
		return reply(202, Response{Status: StatusPending, Fingerprint: fp, Challenge: ch})
	}

	if r.State == uns.RequestPending {
		if p, hint := m.matchPreapprovalLocked(fp, &r, now); p != nil {
			if err := m.storeNewLocked(fpID, &r, known || limited, source, now); err != nil {
				return reply(500, Response{Reason: err.Error()})
			}
			actor := Actor{ID: p.CreatedBy, Label: p.CreatedBy, Kind: "preapproval"}
			if err := m.approveLocked(fpID, p.Element, p.Mount, p.Name, actor, p, now); err != nil {
				m.log.Warn("a pre-approval matched but could not be applied; the request stays pending",
					"fingerprint", fp, "preapproval", p.ID, "err", err)
				r.PreapprovalHint = fmt.Sprintf("matched pre-approval %s, which could not be applied: %v", p.ID, err)
				_ = m.putRequestLocked(fpID, &r)
				m.updateFindingLocked()
				return reply(202, Response{Status: StatusPending, Fingerprint: fp})
			}
			entry, _ := m.reg.ByPubkey(peerHex)
			if entry == nil {
				return reply(202, Response{Status: StatusPending, Fingerprint: fp})
			}
			return m.issueLocked(entry, pub, level, att, now)
		} else if hint != "" {
			r.PreapprovalHint = hint
		}
	}
	if limited {
		return reply(429, Response{Fingerprint: fp, Reason: "too many new keys from this address in the last hour"})
	}
	if err := m.storeNewLocked(fpID, &r, known, source, now); err != nil {
		return reply(500, Response{Reason: err.Error()})
	}
	if r.State == uns.RequestRejected {
		return reply(403, Response{Status: StatusRejected, Fingerprint: fp, Reason: r.Reason})
	}
	return reply(202, Response{Status: StatusPending, Fingerprint: fp, KeyStore: level})
}

// storeNewLocked persists a request, counting a new key against its source and
// keeping the pending store within MaxPending.
func (m *Manager) storeNewLocked(fpID string, r *uns.EnrollmentRequest, known bool, source string, now time.Time) error {
	if !known {
		m.newByIP[source] = append(m.newByIP[source], now)
		m.trimPendingLocked(fpID)
		m.log.Info("new enrollment request", "fingerprint", r.Fingerprint, "ulid", r.ULID, "name", r.Name,
			"key_store", r.KeyStore, "key_change", r.KeyChange != nil, "source", source)
	}
	if err := m.putRequestLocked(fpID, r); err != nil {
		return err
	}
	m.updateFindingLocked()
	return nil
}

// trimPendingLocked drops the oldest pending requests so a new one fits.
func (m *Manager) trimPendingLocked(except string) {
	var pending []string
	for id, r := range m.requests {
		if r.State == uns.RequestPending && id != except {
			pending = append(pending, id)
		}
	}
	if len(pending) < MaxPending {
		return
	}
	sort.Slice(pending, func(i, j int) bool {
		return m.requests[pending[i]].LastSeen < m.requests[pending[j]].LastSeen
	})
	for _, id := range pending[:len(pending)-MaxPending+1] {
		m.log.Warn("pending store full: the oldest request is dropped", "fingerprint", m.requests[id].Fingerprint)
		_ = m.dropRequestLocked(id)
	}
}

func validULID(s string) error {
	if s == "" || len(s) > 64 || strings.ContainsAny(s, "/+#: ") {
		return fmt.Errorf("ulid %q: a node names itself with its ULID", s)
	}
	return nil
}

// keyChangeLocked handles a key change an enrolled child files with its
// current key (§7.2).
func (m *Manager) keyChangeLocked(entry *uns.Entry, req Request, source string, now time.Time) (int, Response) {
	kc := req.KeyChange
	newPub, newHex, err := m.checkKeyChange(entry.ULID, kc, now)
	if err != nil {
		return reply(400, Response{Reason: err.Error()})
	}
	fp, fpID, _ := fingerprintOf(newHex)
	if newHex == entry.Pubkey {
		level, att, ch := m.attestLocked(fpID, newPub, kc.Attestation, kc.Activation, kc.KeyStore, now)
		if ch != nil {
			return reply(202, Response{Status: StatusPending, Fingerprint: fp, Challenge: ch})
		}
		return m.issueLocked(entry, newPub, level, att, now)
	}
	if r, ok := m.requests[fpID]; ok && r.State == uns.RequestBlocked {
		return reply(403, Response{Status: StatusBlocked, Fingerprint: fp, Reason: r.Reason})
	}
	if other, ok := m.reg.ByPubkey(newHex); ok {
		return reply(409, Response{Fingerprint: fp, Reason: "the new key is already enrolled for " + other.ULID})
	}
	level, att, ch := m.attestLocked(fpID, newPub, kc.Attestation, kc.Activation, kc.KeyStore, now)
	if ch != nil {
		return reply(202, Response{Status: StatusPending, Fingerprint: fp, Challenge: ch})
	}
	below := m.belowPolicy(level)
	auto := m.policy.EffectiveKeyChange() == config.KeyChangeAuto && uns.KeyStoreRank(level) >= uns.KeyStoreRank(uns.KeyStoreTPM)
	if auto && below == "" {
		previous := entry.Fingerprint
		swapped, err := m.swapKeyLocked(entry.ULID, newHex, level, att)
		if err != nil {
			return reply(409, Response{Fingerprint: fp, Reason: err.Error()})
		}
		_ = m.dropRequestLocked(fpID)
		m.updateFindingLocked()
		m.auditLocked("enroll.key_change", "auto", Actor{ID: entry.ULID, Label: entry.ULID, Kind: "node"},
			uns.EnrollmentRequestContract, fp, map[string]any{
				"ulid": entry.ULID, "fingerprint": fp, "previous": previous, "key_store": level,
			})
		m.log.Info("key change accepted automatically: authenticated by the current key, the new key is in a TPM",
			"ulid", entry.ULID, "from", previous, "to", fp, "key_store", level)
		return m.issueLocked(swapped, newPub, level, att, now)
	}
	r, known := m.requests[fpID]
	var rec uns.EnrollmentRequest
	if known {
		rec = *r
	} else {
		rec = uns.EnrollmentRequest{Fingerprint: fp, State: uns.RequestPending, FirstSeen: m.stamp(now)}
	}
	rec.Pubkey, rec.KeyStore, rec.Attestation = newHex, level, att
	rec.ULID, rec.Name, rec.RequestedMount, rec.ColcaVersion = entry.ULID, req.Name, req.RequestedMount, req.ColcaVersion
	rec.SourceIP, rec.LastSeen = source, m.stamp(now)
	rec.Count++
	rec.BelowPolicy = below
	rec.KeyChange = &uns.EnrollmentKeyChange{CurrentFingerprint: entry.Fingerprint, CurrentKeyStore: entry.KeyStore}
	if err := m.storeNewLocked(fpID, &rec, known, source, now); err != nil {
		return reply(500, Response{Reason: err.Error()})
	}
	if rec.State == uns.RequestRejected {
		return reply(403, Response{Status: StatusRejected, Fingerprint: fp, Reason: rec.Reason})
	}
	return reply(202, Response{Status: StatusPending, Fingerprint: fp, KeyStore: level})
}

// finishedKeyChangeLocked answers a key change whose current key is no longer
// enrolled because the swap already happened: the new key gets its
// certificate.
func (m *Manager) finishedKeyChangeLocked(req Request, now time.Time) (int, Response, bool) {
	kc := req.KeyChange
	newPub, newHex, err := m.checkKeyChange(req.ULID, kc, now)
	if err != nil {
		return 0, Response{}, false
	}
	entry, ok := m.reg.ByPubkey(newHex)
	if !ok || entry.ULID != req.ULID || !entry.ReplicatesUp() {
		return 0, Response{}, false
	}
	_, fpID, _ := fingerprintOf(newHex)
	level, att, ch := m.attestLocked(fpID, newPub, kc.Attestation, kc.Activation, kc.KeyStore, now)
	if ch != nil {
		code, resp := reply(202, Response{Status: StatusPending, Fingerprint: entry.Fingerprint, Challenge: ch})
		return code, resp, true
	}
	if level == uns.KeyStoreFile && entry.KeyStore != "" {
		level = entry.KeyStore // what was approved, when nothing new was proven
	}
	code, resp := m.issueLocked(entry, newPub, level, att, now)
	return code, resp, true
}

// checkKeyChange verifies the proof of possession of a key change for ulid.
func (m *Manager) checkKeyChange(ulid string, kc *KeyChange, now time.Time) (crypto.PublicKey, string, error) {
	newPub, err := pubkey.ParseHex(kc.Pubkey)
	if err != nil {
		return nil, "", fmt.Errorf("key change: new key: %w", err)
	}
	newHex, err := pubkey.Hex(newPub)
	if err != nil {
		return nil, "", err
	}
	at := time.UnixMilli(kc.Timestamp)
	if d := now.Sub(at); d > KeyChangeClockSkew || d < -KeyChangeClockSkew {
		return nil, "", fmt.Errorf("key change: signed at %s, more than %s from now", at.UTC().Format(time.RFC3339), KeyChangeClockSkew)
	}
	if !verifyMessage(newPub, KeyChangeMessage(ulid, newHex, m.parentSPKI, kc.Timestamp), kc.Signature) {
		return nil, "", errors.New("key change: the new key's signature does not verify")
	}
	return newPub, newHex, nil
}

// swapKeyLocked moves an entry to a new key. The old key is refused from the
// moment it returns (Registry.Update kicks the sessions).
func (m *Manager) swapKeyLocked(ulid, newHex, level string, att *uns.EnrollmentAttestation) (*uns.Entry, error) {
	return m.reg.Update(ulid, func(e *uns.Entry) error {
		e.Pubkey = newHex
		e.KeyStore = level
		e.KeyChangeRequested = false
		// The new key has no certificate yet; it gets one on its next request,
		// and only an issued one admits it.
		e.CertState, e.CertNotAfter = "", ""
		e.EKManufacturer, e.EKSerial = "", ""
		if att != nil {
			e.EKManufacturer, e.EKSerial = att.EKManufacturer, att.EKSerial
		}
		return nil
	})
}

// --- pre-approvals -------------------------------------------------------------

// matchPreapprovalLocked finds an open pre-approval for a request: by node key,
// or by endorsement key for a tpm-attested request only. hint names a matching
// one that is no longer usable.
func (m *Manager) matchPreapprovalLocked(fp string, r *uns.EnrollmentRequest, now time.Time) (*uns.EnrollmentPreapproval, string) {
	ids := make([]string, 0, len(m.preapprovals))
	for id := range m.preapprovals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	hint := ""
	for _, id := range ids {
		p := m.preapprovals[id]
		matches := p.Match.Key != "" && p.Match.Key == fp
		if p.Match.EK != "" && r.KeyStore == uns.KeyStoreTPMAttested && r.Attestation != nil && r.Attestation.EK == p.Match.EK {
			matches = true
		}
		if !matches {
			continue
		}
		if p.State != uns.PreapprovalOpen || p.Uses <= 0 || now.After(parseStamp(p.ExpiresAt)) {
			hint = fmt.Sprintf("matched pre-approval %s, which is %s", p.ID, closedState(p, now))
			continue
		}
		if r.BelowPolicy != "" {
			hint = fmt.Sprintf("matched pre-approval %s, but the key store is below enrollment.require (%s)", p.ID, r.BelowPolicy)
			continue
		}
		return p, ""
	}
	return nil, hint
}

func closedState(p *uns.EnrollmentPreapproval, now time.Time) string {
	if p.State == uns.PreapprovalUsed || p.Uses <= 0 {
		return "used up"
	}
	if p.State == uns.PreapprovalExpired || now.After(parseStamp(p.ExpiresAt)) {
		return "expired"
	}
	return p.State
}

// PreapprovalInput is a new pre-approval.
type PreapprovalInput struct {
	// ID names it; empty mints one. The hub mints its own so a retried
	// command finds the one it created.
	ID        string
	Match     uns.EnrollmentMatch
	Element   string
	Mount     string
	Name      string
	ExpiresAt time.Time
	Uses      int
	Note      string
}

// Preapprove records a pre-approval and applies it at once to a pending
// request it matches.
func (m *Manager) Preapprove(in PreapprovalInput, a Actor) (uns.EnrollmentPreapproval, error) {
	now := m.now()
	var match uns.EnrollmentMatch
	switch {
	case in.Match.EK != "" && in.Match.Key != "":
		return uns.EnrollmentPreapproval{}, fmt.Errorf("match names both ek and key, want one: %w", ErrInvalid)
	case in.Match.EK != "":
		id, err := pubkey.ParseFingerprint(in.Match.EK)
		if err != nil {
			return uns.EnrollmentPreapproval{}, fmt.Errorf("%w: %w", err, ErrInvalid)
		}
		match.EK = displayFingerprint(id)
	case in.Match.Key != "":
		id, err := pubkey.ParseFingerprint(in.Match.Key)
		if err != nil {
			return uns.EnrollmentPreapproval{}, fmt.Errorf("%w: %w", err, ErrInvalid)
		}
		match.Key = displayFingerprint(id)
	default:
		return uns.EnrollmentPreapproval{}, fmt.Errorf("match names neither ek nor key: %w", ErrInvalid)
	}
	if (in.Element == "") == (in.Mount == "") {
		return uns.EnrollmentPreapproval{}, fmt.Errorf("name exactly one of element and mount: %w", ErrInvalid)
	}
	if in.Element != "" {
		if err := uns.ValidElementID(in.Element); err != nil {
			return uns.EnrollmentPreapproval{}, fmt.Errorf("%w: %w", err, ErrInvalid)
		}
		if m.elements != nil {
			if _, ok := m.elements.PathOf(in.Element); !ok {
				return uns.EnrollmentPreapproval{}, fmt.Errorf("element %s is not placed at this node: %w", in.Element, ErrInvalid)
			}
		}
	}
	if in.ID != "" && (len(in.ID) > 64 || strings.ContainsAny(in.ID, "/+#")) {
		return uns.EnrollmentPreapproval{}, fmt.Errorf("pre-approval id %q: %w", in.ID, ErrInvalid)
	}
	if in.ID == "" {
		in.ID = registry.NewULID()
	}
	if in.Uses < 0 {
		return uns.EnrollmentPreapproval{}, fmt.Errorf("uses must not be negative: %w", ErrInvalid)
	}
	if in.Uses == 0 {
		in.Uses = 1
	}
	if in.ExpiresAt.IsZero() {
		in.ExpiresAt = now.Add(PreapprovalTTL)
	}
	if !in.ExpiresAt.After(now) {
		return uns.EnrollmentPreapproval{}, fmt.Errorf("expires_at is in the past: %w", ErrInvalid)
	}
	p := uns.EnrollmentPreapproval{
		ID: in.ID, Match: match, Element: in.Element, Mount: strings.Trim(in.Mount, "/"), Name: in.Name,
		ExpiresAt: m.stamp(in.ExpiresAt), Uses: in.Uses, UsedBy: []string{}, State: uns.PreapprovalOpen,
		CreatedBy: a.name(), CreatedAt: m.stamp(now), Note: in.Note,
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.preapprovals[p.ID]; ok {
		if old.State == uns.PreapprovalOpen && old.Match == p.Match && old.Element == p.Element && old.Mount == p.Mount {
			return *old, nil // a retried command
		}
		return uns.EnrollmentPreapproval{}, fmt.Errorf("pre-approval %s exists: %w", p.ID, ErrConflict)
	}
	if err := m.putPreapprovalLocked(&p); err != nil {
		return uns.EnrollmentPreapproval{}, err
	}
	fp := match.Key
	if fp == "" {
		fp = match.EK
	}
	m.auditLocked("enroll.preapprove", "success", a, uns.EnrollmentPreapprovalContract, p.ID,
		map[string]any{"fingerprint": fp, "element": p.Element, "preapproval": p.ID})
	m.log.Info("pre-approval created", "id", p.ID, "match_key", match.Key, "match_ek", match.EK,
		"element", p.Element, "mount", p.Mount, "expires_at", p.ExpiresAt, "uses", p.Uses, "by", a.name())
	// A request that is already waiting is decided now.
	ids := make([]string, 0, len(m.requests))
	for id := range m.requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		r := m.requests[id]
		if r.State != uns.RequestPending {
			continue
		}
		if got, _ := m.matchPreapprovalLocked(r.Fingerprint, r, now); got == nil || got.ID != p.ID {
			continue
		}
		if err := m.approveLocked(id, p.Element, p.Mount, p.Name, Actor{ID: a.ID, Label: a.name(), Kind: "preapproval"}, &p, now); err != nil {
			m.log.Warn("a waiting request matches the new pre-approval but could not be approved", "fingerprint", r.Fingerprint, "err", err)
		}
		if m.preapprovals[p.ID] == nil || m.preapprovals[p.ID].State != uns.PreapprovalOpen {
			break
		}
	}
	return *m.preapprovals[p.ID], nil
}

// usePreapprovalLocked counts one use of p by ulid.
func (m *Manager) usePreapprovalLocked(p *uns.EnrollmentPreapproval, ulid string, now time.Time) {
	cur, ok := m.preapprovals[p.ID]
	if !ok {
		return
	}
	next := *cur
	next.UsedBy = append(append([]string{}, cur.UsedBy...), ulid)
	next.Uses--
	if next.Uses <= 0 {
		next.Uses = 0
		next.State = uns.PreapprovalUsed
		next.ClosedAt = m.stamp(now)
	}
	if err := m.putPreapprovalLocked(&next); err != nil {
		m.log.Error("pre-approval use not recorded", "id", p.ID, "err", err)
	}
	*p = next
}

// Unpreapprove withdraws a pre-approval.
func (m *Manager) Unpreapprove(id string, a Actor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.preapprovals[id]; !ok {
		return fmt.Errorf("pre-approval %s: %w", id, ErrNotFound)
	}
	if err := m.dropPreapprovalLocked(id); err != nil {
		return err
	}
	m.auditLocked("enroll.unpreapprove", "success", a, uns.EnrollmentPreapprovalContract, id, map[string]any{"preapproval": id})
	return nil
}

// Preapprovals lists the pre-approvals ordered by id, one page after the given
// id; next is empty when the list is exhausted.
func (m *Manager) Preapprovals(after string, limit int) (out []uns.EnrollmentPreapproval, next string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.preapprovals))
	for id := range m.preapprovals {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for i, id := range ids {
		if i == limit {
			next = ids[i-1]
			break
		}
		out = append(out, *m.preapprovals[id])
	}
	return out, next
}

// --- decisions -----------------------------------------------------------------

// Requests lists the requests in fingerprint order, one page after the given
// fingerprint ID; state filters when not empty.
func (m *Manager) Requests(state, after string, limit int) (out []uns.EnrollmentRequest, next string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.requests))
	for id, r := range m.requests {
		if id > after && (state == "" || r.State == state) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for i, id := range ids {
		if i == limit {
			next = ids[i-1]
			break
		}
		out = append(out, *m.requests[id])
	}
	return out, next
}

// Request returns one request by fingerprint in either form.
func (m *Manager) Request(fp string) (uns.EnrollmentRequest, error) {
	id, err := pubkey.ParseFingerprint(fp)
	if err != nil {
		return uns.EnrollmentRequest{}, fmt.Errorf("%w: %w", err, ErrInvalid)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.requests[id]
	if !ok {
		return uns.EnrollmentRequest{}, fmt.Errorf("no enrollment request for %s: %w", displayFingerprint(id), ErrNotFound)
	}
	return *r, nil
}

func (m *Manager) lookupLocked(fp string) (string, *uns.EnrollmentRequest, error) {
	id, err := pubkey.ParseFingerprint(fp)
	if err != nil {
		return "", nil, fmt.Errorf("%w: %w", err, ErrInvalid)
	}
	r, ok := m.requests[id]
	if !ok {
		return id, nil, fmt.Errorf("no enrollment request for %s: %w", displayFingerprint(id), ErrNotFound)
	}
	return id, r, nil
}

// Approve admits a request at element, or at mount (authored when missing).
// A key change request moves its entry to the new key instead.
func (m *Manager) Approve(fp, element, mount, name string, a Actor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, _, err := m.lookupLocked(fp)
	if err != nil {
		return err
	}
	return m.approveLocked(id, element, mount, name, a, nil, m.now())
}

func (m *Manager) approveLocked(id, element, mount, name string, a Actor, via *uns.EnrollmentPreapproval, now time.Time) error {
	r := m.requests[id]
	if r.State == uns.RequestBlocked {
		return fmt.Errorf("request %s is blocked; unblock it first: %w", r.Fingerprint, ErrConflict)
	}
	if r.BelowPolicy != "" {
		return fmt.Errorf("request %s holds a %s key; enrollment.require asks for %s: %w", r.Fingerprint, r.KeyStore, r.BelowPolicy, ErrBelowPolicy)
	}
	if r.Pubkey == "" || r.ULID == "" {
		return fmt.Errorf("request %s names no key or ulid: %w", r.Fingerprint, ErrInvalid)
	}
	op := "enroll.approve"
	if r.KeyChange != nil {
		op = "enroll.key_change"
		if _, err := m.swapKeyLocked(r.ULID, r.Pubkey, r.KeyStore, r.Attestation); err != nil {
			if errors.Is(err, registry.ErrNotEnrolled) {
				return fmt.Errorf("node %s is no longer enrolled here: %w", r.ULID, ErrConflict)
			}
			return fmt.Errorf("%w: %w", err, ErrConflict)
		}
	} else {
		if element == "" && mount != "" {
			if m.author == nil {
				return fmt.Errorf("this node cannot author elements: %w", ErrInvalid)
			}
			el, err := m.author(strings.Trim(mount, "/"))
			if err != nil {
				return fmt.Errorf("author element at %s: %w: %w", mount, err, ErrInvalid)
			}
			element = el
		}
		if element == "" {
			return fmt.Errorf("approve %s: name the element (or a mount) to place the node at: %w", r.Fingerprint, ErrInvalid)
		}
		e := uns.Entry{ULID: r.ULID, Pubkey: r.Pubkey, Kind: uns.KindNode, Element: element, Name: name, KeyStore: r.KeyStore}
		if r.Attestation != nil && r.KeyStore == uns.KeyStoreTPMAttested {
			e.EKManufacturer, e.EKSerial = r.Attestation.EKManufacturer, r.Attestation.EKSerial
		}
		raw, _ := json.Marshal(&e)
		if _, _, err := m.reg.Enroll(raw); err != nil {
			switch {
			case errors.Is(err, registry.ErrConflict):
				return fmt.Errorf("%w: %w", err, ErrConflict)
			default:
				return fmt.Errorf("%w: %w", err, ErrInvalid)
			}
		}
	}
	if err := m.dropRequestLocked(id); err != nil {
		return err
	}
	meta := map[string]any{"fingerprint": r.Fingerprint, "key_store": r.KeyStore, "ulid": r.ULID, "element": element}
	if r.Attestation != nil && r.Attestation.EK != "" {
		meta["ek"] = r.Attestation.EK
	}
	outcome := "approved"
	if via != nil {
		outcome = "preapproved"
		meta["preapproval"] = via.ID
		m.usePreapprovalLocked(via, r.ULID, now)
	}
	m.auditLocked(op, outcome, a, uns.EnrollmentRequestContract, r.Fingerprint, meta)
	m.log.Info("enrollment request approved", "fingerprint", r.Fingerprint, "ulid", r.ULID, "element", element,
		"key_store", r.KeyStore, "key_change", r.KeyChange != nil, "by", a.name(), "preapproved", via != nil)
	m.updateFindingLocked()
	return nil
}

// Reject keeps a request as rejected for RejectedTTL; the child stops asking
// until it restarts.
func (m *Manager) Reject(fp, reason string, a Actor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, r, err := m.lookupLocked(fp)
	if err != nil {
		return err
	}
	if r.State == uns.RequestBlocked {
		return fmt.Errorf("request %s is blocked: %w", r.Fingerprint, ErrConflict)
	}
	next := *r
	next.State, next.Reason, next.DecidedBy, next.DecidedAt = uns.RequestRejected, reason, a.name(), m.stamp(m.now())
	if err := m.putRequestLocked(id, &next); err != nil {
		return err
	}
	m.auditLocked("enroll.reject", "rejected", a, uns.EnrollmentRequestContract, r.Fingerprint,
		map[string]any{"fingerprint": r.Fingerprint, "key_store": r.KeyStore, "ulid": r.ULID})
	m.updateFindingLocked()
	return nil
}

// Block refuses a key until it is unblocked. Blocking an active node's key
// revokes the node as well.
func (m *Manager) Block(fp, reason, ulid string, a Actor) error {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var entry *uns.Entry
	if ulid != "" {
		e, ok := m.reg.Get(ulid)
		if !ok {
			return fmt.Errorf("node %s is not enrolled here: %w", ulid, ErrNotFound)
		}
		entry = e
		if fp == "" {
			fp = e.Fingerprint
		}
	}
	id, err := pubkey.ParseFingerprint(fp)
	if err != nil {
		return fmt.Errorf("%w: %w", err, ErrInvalid)
	}
	display := displayFingerprint(id)
	if entry != nil && entry.Fingerprint != display {
		return fmt.Errorf("node %s holds the key %s, not %s: %w", ulid, entry.Fingerprint, display, ErrConflict)
	}
	if entry == nil {
		for _, e := range m.reg.List() {
			if e.Fingerprint == display {
				entry = e
				break
			}
		}
	}
	r, known := m.requests[id]
	var next uns.EnrollmentRequest
	if known {
		next = *r
	} else {
		next = uns.EnrollmentRequest{Fingerprint: display, FirstSeen: m.stamp(now), LastSeen: m.stamp(now)}
	}
	if entry != nil {
		if !entry.ReplicatesUp() {
			return fmt.Errorf("the key belongs to %s %s, not to a node: %w", entry.Kind, entry.ULID, ErrConflict)
		}
		if _, _, err := m.reg.Revoke(entry.ULID); err != nil && !errors.Is(err, registry.ErrNotEnrolled) {
			return fmt.Errorf("revoke %s: %w", entry.ULID, err)
		}
		next.Pubkey, next.ULID = entry.Pubkey, entry.ULID
		if next.KeyStore == "" {
			next.KeyStore = entry.KeyStore
		}
	}
	next.State, next.Reason, next.DecidedBy, next.DecidedAt = uns.RequestBlocked, reason, a.name(), m.stamp(now)
	if err := m.putRequestLocked(id, &next); err != nil {
		return err
	}
	meta := map[string]any{"fingerprint": display, "key_store": next.KeyStore}
	if entry != nil {
		meta["ulid"] = entry.ULID
	}
	m.auditLocked("enroll.block", "blocked", a, uns.EnrollmentRequestContract, display, meta)
	m.log.Warn("key blocked", "fingerprint", display, "revoked", entry != nil, "by", a.name())
	m.updateFindingLocked()
	return nil
}

// Unblock moves a blocked (or rejected) request back to pending. A block
// placed on a key that never asked is simply lifted.
func (m *Manager) Unblock(fp string, a Actor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, r, err := m.lookupLocked(fp)
	if err != nil {
		return err
	}
	if r.State == uns.RequestPending {
		return fmt.Errorf("request %s is not blocked: %w", r.Fingerprint, ErrConflict)
	}
	if r.Pubkey == "" || r.ULID == "" {
		if err := m.dropRequestLocked(id); err != nil {
			return err
		}
	} else {
		next := *r
		next.State, next.Reason, next.DecidedBy, next.DecidedAt = uns.RequestPending, "", a.name(), m.stamp(m.now())
		next.LastSeen = m.stamp(m.now()) // a fresh week to decide in
		if err := m.putRequestLocked(id, &next); err != nil {
			return err
		}
	}
	m.auditLocked("enroll.unblock", "success", a, uns.EnrollmentRequestContract, r.Fingerprint,
		map[string]any{"fingerprint": r.Fingerprint})
	m.updateFindingLocked()
	return nil
}

// RequestKeyChange asks an enrolled node to move to a new key on its next
// renewal.
func (m *Manager) RequestKeyChange(ulid string, a Actor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.reg.Get(ulid)
	if !ok {
		return fmt.Errorf("node %s is not enrolled here: %w", ulid, ErrNotFound)
	}
	if !e.ReplicatesUp() {
		return fmt.Errorf("%s is %s, not a node: %w", ulid, e.Kind, ErrConflict)
	}
	if _, err := m.reg.Update(ulid, func(e *uns.Entry) error { e.KeyChangeRequested = true; return nil }); err != nil {
		return err
	}
	m.auditLocked("enroll.request_key_change", "success", a, "_EnrolledIdentity", ulid,
		map[string]any{"ulid": ulid, "fingerprint": e.Fingerprint})
	return nil
}

// --- housekeeping ----------------------------------------------------------------

// Sweep expires what outlived its time: pending and rejected requests after a
// week, pre-approvals past their expiry, closed pre-approvals a week after they
// closed, and stale attestation challenges.
func (m *Manager) Sweep() {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, r := range m.requests {
		switch r.State {
		case uns.RequestPending:
			if now.Sub(parseStamp(r.LastSeen)) > PendingTTL {
				m.log.Info("pending enrollment request expired", "fingerprint", r.Fingerprint)
				_ = m.dropRequestLocked(id)
			}
		case uns.RequestRejected:
			if now.Sub(parseStamp(r.DecidedAt)) > RejectedTTL {
				_ = m.dropRequestLocked(id)
			}
		}
	}
	for id, p := range m.preapprovals {
		switch {
		case p.State == uns.PreapprovalOpen && now.After(parseStamp(p.ExpiresAt)):
			next := *p
			next.State, next.ClosedAt = uns.PreapprovalExpired, m.stamp(now)
			_ = m.putPreapprovalLocked(&next)
		case p.State != uns.PreapprovalOpen && now.Sub(parseStamp(p.ClosedAt)) > PreapprovalKeep:
			_ = m.dropPreapprovalLocked(id)
		}
	}
	for id, c := range m.challenges {
		if now.After(c.expires) {
			delete(m.challenges, id)
		}
	}
	for ip, ts := range m.newByIP {
		if len(ts) == 0 || now.Sub(ts[len(ts)-1]) > time.Hour {
			delete(m.newByIP, ip)
		}
	}
	m.updateFindingLocked()
}

// Run sweeps once a minute until stop closes.
func (m *Manager) Run(stop <-chan struct{}) {
	m.Sweep()
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			m.Sweep()
		}
	}
}

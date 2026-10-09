package enroll

import (
	"crypto"
	"crypto/x509"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

type elements map[string]string

func (e elements) PathOf(id string) (string, bool) { p, ok := e[id]; return p, ok }

type fixture struct {
	t        *testing.T
	st       *store.Store
	reg      *registry.Manager
	m        *Manager
	now      time.Time
	parent   *identity.Identity
	els      elements
	audits   []engine.AuditDenial
	findings map[string][]byte
	bus      map[string][]byte
	kicked   []string
}

const node = "01PARENT0000000000000000000"

func newFixture(t *testing.T, policy config.Enrollment) *fixture {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg, err := registry.New(st, node)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := identity.Generate(filepath.Join(dir, "parent.key"))
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, st: st, reg: reg, parent: parent, els: elements{"el-hall": "hall", "el-press": "hall/press"},
		now: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC), findings: map[string][]byte{}, bus: map[string][]byte{}}
	reg.SetNamespace(f.els)
	reg.SetKick(func(u string) { f.kicked = append(f.kicked, u) })
	roots, err := LoadTPMRoots("")
	if err != nil {
		t.Fatal(err)
	}
	f.m, err = New(st, reg, Options{NodeULID: node, Signer: parent.Signer, Policy: policy, Roots: roots, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	f.m.SetAudit(func(d engine.AuditDenial) { f.audits = append(f.audits, d) })
	f.m.SetFinding(func(topic string, payload []byte) error { f.findings[topic] = payload; return nil })
	f.m.SetDeliver(func(topic string, payload []byte, _ bool) { f.bus[topic] = payload })
	f.m.SetElements(f.els)
	f.m.SetAuthoring(func(path string) (string, error) {
		id := "el-" + strings.ReplaceAll(path, "/", "-")
		f.els[id] = path
		return id, nil
	})
	return f
}

func (f *fixture) key(name string) *identity.Identity {
	f.t.Helper()
	id, err := identity.Generate(filepath.Join(f.t.TempDir(), name+".key"))
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *fixture) p256(name string) *identity.Identity {
	f.t.Helper()
	id, err := identity.GenerateP256(filepath.Join(f.t.TempDir(), name+".key"))
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *fixture) ask(id *identity.Identity, req Request) (int, Response) {
	f.t.Helper()
	return f.m.HandleRequest(id.Pub, req, "10.0.0.7")
}

var person = Actor{ID: "sub-1", Label: "till", Kind: "human"}

func (f *fixture) leaf(resp Response, id *identity.Identity) *x509.Certificate {
	f.t.Helper()
	certs, err := VerifyChain([]byte(resp.Certificate), id.Pub, f.parent.PublicHex())
	if err != nil {
		f.t.Fatalf("issued chain: %v", err)
	}
	return certs[0]
}

func TestAnUnknownKeyIsPendingAndMirrored(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	code, resp := f.ask(child, Request{ULID: "01CHILD", Name: "press-04", RequestedMount: "hall/press", KeyStore: "file", ColcaVersion: "0.33.0"})
	if code != 202 || resp.Status != StatusPending || resp.Fingerprint != child.Fingerprint() {
		t.Fatalf("first request: %d %+v", code, resp)
	}
	if _, ok := f.reg.ByPubkey(child.PublicHex()); ok {
		t.Fatal("a pending key is in the registry")
	}
	code, _ = f.ask(child, Request{ULID: "01CHILD", KeyStore: "file"})
	if code != 202 {
		t.Fatalf("second request: %d", code)
	}
	r, err := f.m.Request(child.Fingerprint())
	if err != nil {
		t.Fatal(err)
	}
	if r.Count != 2 || r.State != uns.RequestPending || r.KeyStore != uns.KeyStoreFile || r.SourceIP != "10.0.0.7" || r.Pubkey != child.PublicHex() {
		t.Fatalf("record: %+v", r)
	}
	topic := uns.Prefix() + "_EnrollmentRequest/" + node + "/_colca/enrollment/" + child.Fingerprint()
	var mirrored uns.EnrollmentRequest
	if err := json.Unmarshal(f.bus[topic], &mirrored); err != nil || mirrored.Count != 2 || mirrored.Fingerprint != child.Fingerprint() {
		t.Fatalf("mirror at %s: %s %v", topic, f.bus[topic], err)
	}
	kv, ok, err := f.st.KVGet(uns.EnrollmentRequestPath(child.Fingerprint()), node, topic)
	if err != nil || !ok || !strings.Contains(string(kv.Payload), `"state":"pending"`) {
		t.Fatalf("KV mirror: %s %v %v", kv.Payload, ok, err)
	}
	if f.findings[uns.Prefix()+"_Finding/"+node+"/"+FindingReason] == nil {
		t.Fatal("no finding while a request is pending")
	}
	// The store survives a restart.
	m2, err := New(f.st, f.reg, Options{NodeULID: node, Signer: f.parent.Signer, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := m2.Request(child.Fingerprint()); err != nil || r.Count != 2 {
		t.Fatalf("after reload: %+v %v", r, err)
	}
}

func TestApproveIssuesTheCertificateOnTheNextRequest(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	f.ask(child, Request{ULID: "01CHILD", KeyStore: "file"})
	if err := f.m.Approve(child.Fingerprint(), "el-press", "", "", person); err != nil {
		t.Fatal(err)
	}
	e, ok := f.reg.ByPubkey(child.PublicHex())
	if !ok || e.ULID != "01CHILD" || e.Element != "el-press" || e.Kind != uns.KindNode || e.KeyStore != uns.KeyStoreFile {
		t.Fatalf("entry: %+v %v", e, ok)
	}
	if _, err := f.m.Request(child.Fingerprint()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the decided request is still held: %v", err)
	}
	if len(f.audits) != 1 || f.audits[0].Operation != "enroll.approve" || f.audits[0].ActorLabel != "till" ||
		f.audits[0].Metadata["fingerprint"] != child.Fingerprint() {
		t.Fatalf("audit: %+v", f.audits)
	}
	if f.findings[uns.Prefix()+"_Finding/"+node+"/"+FindingReason] != nil {
		t.Fatal("the finding stands with nothing pending")
	}
	code, resp := f.ask(child, Request{ULID: "01CHILD", KeyStore: "file"})
	if code != 200 || resp.Status != StatusApproved {
		t.Fatalf("after approval: %d %+v", code, resp)
	}
	leaf := f.leaf(resp, child)
	if leaf.Subject.CommonName != "01CHILD" || leaf.NotAfter.Sub(leaf.NotBefore) < CertValidity {
		t.Fatalf("leaf: %v %v–%v", leaf.Subject, leaf.NotBefore, leaf.NotAfter)
	}
	var uris []string
	for _, u := range leaf.URIs {
		uris = append(uris, u.String())
	}
	if strings.Join(uris, " ") != "colca:node:01CHILD colca:element:el-press colca:keystore:file" {
		t.Fatalf("SAN URIs: %v", uris)
	}
	if err := f.m.Issuer().Check(leaf, "01CHILD", f.now); err != nil {
		t.Fatalf("the parent does not admit its own certificate: %v", err)
	}
	if err := f.m.Issuer().Check(leaf, "01OTHER", f.now); !errors.Is(err, ErrCertWrongNode) {
		t.Fatalf("wrong node: %v", err)
	}
	if err := f.m.Issuer().Check(leaf, "01CHILD", f.now.Add(CertValidity+time.Hour)); !errors.Is(err, ErrCertExpired) {
		t.Fatalf("expired: %v", err)
	}
	e, _ = f.reg.Get("01CHILD")
	if e.CertState != uns.CertStateIssued || e.CertNotAfter == "" || e.LastSeen == "" {
		t.Fatalf("entry after issue: %+v", e)
	}
	// Renewal at two thirds, and an expired certificate of an active entry is
	// renewed, not refused.
	if got := RenewAt(leaf); got.Sub(leaf.NotBefore) != leaf.NotAfter.Sub(leaf.NotBefore)*2/3 {
		t.Fatalf("RenewAt %v", got)
	}
	f.now = f.now.Add(40 * 24 * time.Hour)
	if code, _ := f.ask(child, Request{ULID: "01CHILD", KeyStore: "file"}); code != 200 {
		t.Fatalf("renewal after expiry: %d", code)
	}
}

func TestApproveAtAMountAuthorsTheElement(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	f.ask(child, Request{ULID: "01CHILD", RequestedMount: "hall/new"})
	if err := f.m.Approve(child.Fingerprint(), "", "hall/new", "Presse 5", person); err != nil {
		t.Fatal(err)
	}
	if e, ok := f.reg.Get("01CHILD"); !ok || e.Element != "el-hall-new" || e.Name != "Presse 5" {
		t.Fatalf("entry: %+v", e)
	}
	if err := f.m.Approve(child.Fingerprint(), "el-press", "", "", person); !errors.Is(err, ErrNotFound) {
		t.Fatalf("approving twice: %v", err)
	}
}

func TestRejectAndBlock(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	f.ask(child, Request{ULID: "01CHILD"})
	if err := f.m.Reject(child.Fingerprint(), "unknown device", person); err != nil {
		t.Fatal(err)
	}
	code, resp := f.ask(child, Request{ULID: "01CHILD"})
	if code != 403 || resp.Status != StatusRejected || resp.Reason != "unknown device" {
		t.Fatalf("rejected: %d %+v", code, resp)
	}
	// A rejected record expires after a week.
	f.now = f.now.Add(RejectedTTL + time.Hour)
	f.m.Sweep()
	if _, err := f.m.Request(child.Fingerprint()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected record after a week: %v", err)
	}

	thief := f.key("thief")
	f.ask(thief, Request{ULID: "01THIEF"})
	if err := f.m.Block(thief.Fingerprint(), "stolen", "", person); err != nil {
		t.Fatal(err)
	}
	r, _ := f.m.Request(thief.Fingerprint())
	code, resp = f.ask(thief, Request{ULID: "01THIEF"})
	r2, _ := f.m.Request(thief.Fingerprint())
	if code != 403 || resp.Status != StatusBlocked || r2.Count != r.Count {
		t.Fatalf("blocked: %d %+v, count %d→%d (a blocked key adds nothing)", code, resp, r.Count, r2.Count)
	}
	f.now = f.now.Add(30 * 24 * time.Hour)
	f.m.Sweep()
	if _, err := f.m.Request(thief.Fingerprint()); err != nil {
		t.Fatalf("a block expired: %v", err)
	}
	if err := f.m.Approve(thief.Fingerprint(), "el-press", "", "", person); !errors.Is(err, ErrConflict) {
		t.Fatalf("approving a blocked key: %v", err)
	}
	if err := f.m.Unblock(thief.Fingerprint(), person); err != nil {
		t.Fatal(err)
	}
	if code, _ := f.ask(thief, Request{ULID: "01THIEF"}); code != 202 {
		t.Fatalf("after unblock: %d", code)
	}
}

func TestBlockingAnActiveNodeRevokesIt(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	f.ask(child, Request{ULID: "01CHILD"})
	if err := f.m.Approve(child.Fingerprint(), "el-press", "", "", person); err != nil {
		t.Fatal(err)
	}
	if err := f.m.Block("", "lost", "01CHILD", person); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.reg.Get("01CHILD"); ok {
		t.Fatal("a blocked node stays enrolled")
	}
	if len(f.kicked) == 0 || f.kicked[len(f.kicked)-1] != "01CHILD" {
		t.Fatalf("the revoked node's sessions were not kicked: %v", f.kicked)
	}
	if code, resp := f.ask(child, Request{ULID: "01CHILD"}); code != 403 || resp.Status != StatusBlocked {
		t.Fatalf("after revoke and block: %d %+v", code, resp)
	}
}

func TestPendingStoreLimits(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	for i := range NewKeysPerIPHour {
		if code, _ := f.ask(f.key("k"), Request{ULID: "01K" + string(rune('A'+i))}); code != 202 {
			t.Fatalf("key %d: %d", i, code)
		}
	}
	extra := f.key("extra")
	if code, _ := f.ask(extra, Request{ULID: "01EXTRA"}); code != 429 {
		t.Fatalf("the eleventh new key within the hour: %d, want 429", code)
	}
	if _, err := f.m.Request(extra.Fingerprint()); !errors.Is(err, ErrNotFound) {
		t.Fatal("a refused key was stored")
	}
	// Another source is not limited by the first one's keys; an hour later the
	// first one isn't either.
	if code, _ := f.m.HandleRequest(extra.Pub, Request{ULID: "01EXTRA"}, "10.0.0.8"); code != 202 {
		t.Fatalf("other source: %d", code)
	}
	f.now = f.now.Add(61 * time.Minute)
	if code, _ := f.ask(f.key("later"), Request{ULID: "01LATER"}); code != 202 {
		t.Fatalf("an hour later: %d", code)
	}
	// Pending requests expire after a week without a request.
	f.now = f.now.Add(PendingTTL + time.Hour)
	f.m.Sweep()
	if items, _ := f.m.Requests("", "", 1000); len(items) != 0 {
		t.Fatalf("%d requests left after a week", len(items))
	}
}

func TestThePendingStoreDropsTheOldestBeyondItsLimit(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	var first *identity.Identity
	for i := range MaxPending + 1 {
		k := f.key("k")
		if i == 0 {
			first = k
		}
		f.now = f.now.Add(time.Second)
		if code, _ := f.m.HandleRequest(k.Pub, Request{ULID: "01K"}, "10.1."+string(rune('a'+i%26))+"."+string(rune('a'+i/26))); code != 202 {
			t.Fatalf("key %d: %d", i, code)
		}
	}
	items, _ := f.m.Requests(uns.RequestPending, "", 1000)
	if len(items) != MaxPending {
		t.Fatalf("%d pending, want %d", len(items), MaxPending)
	}
	if _, err := f.m.Request(first.Fingerprint()); !errors.Is(err, ErrNotFound) {
		t.Fatal("the oldest request was kept")
	}
}

func TestAPolicyBelowTheRequestIsShownAndRefused(t *testing.T) {
	f := newFixture(t, config.Enrollment{Require: "tpm"})
	child := f.key("child")
	f.ask(child, Request{ULID: "01CHILD", KeyStore: "file"})
	r, _ := f.m.Request(child.Fingerprint())
	if r.BelowPolicy != "tpm" {
		t.Fatalf("below_policy %q", r.BelowPolicy)
	}
	if err := f.m.Approve(child.Fingerprint(), "el-press", "", "", person); !errors.Is(err, ErrBelowPolicy) {
		t.Fatalf("approve below policy: %v", err)
	}
	tpm := f.p256("tpm")
	f.ask(tpm, Request{ULID: "01TPM", KeyStore: "tpm"})
	if r, _ := f.m.Request(tpm.Fingerprint()); r.BelowPolicy != "" {
		t.Fatalf("a tpm claim is below tpm: %+v", r)
	}
}

func TestAPreapprovedKeyJoinsAtOnce(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	p, err := f.m.Preapprove(PreapprovalInput{Match: uns.EnrollmentMatch{Key: child.FingerprintID()}, Element: "el-press", Uses: 1}, person)
	if err != nil {
		t.Fatal(err)
	}
	if p.State != uns.PreapprovalOpen || p.ExpiresAt != f.now.Add(PreapprovalTTL).Format(time.RFC3339) || p.Match.Key != child.Fingerprint() {
		t.Fatalf("pre-approval: %+v", p)
	}
	code, resp := f.ask(child, Request{ULID: "01CHILD"})
	if code != 200 || resp.Certificate == "" {
		t.Fatalf("pre-approved request: %d %+v", code, resp)
	}
	got, _ := f.m.Preapprovals("", 10)
	if len(got) != 1 || got[0].State != uns.PreapprovalUsed || got[0].Uses != 0 || len(got[0].UsedBy) != 1 || got[0].UsedBy[0] != "01CHILD" {
		t.Fatalf("after use: %+v", got)
	}
	last := f.audits[len(f.audits)-1]
	if last.ReasonCode != "preapproved" || last.Metadata["preapproval"] != p.ID {
		t.Fatalf("audit: %+v", last)
	}
	// A used-up pre-approval stays listed a week, then goes.
	f.now = f.now.Add(PreapprovalKeep + time.Hour)
	f.m.Sweep()
	if got, _ := f.m.Preapprovals("", 10); len(got) != 0 {
		t.Fatalf("closed pre-approval after a week: %+v", got)
	}
}

func TestAnExpiredPreapprovalFallsBackToPendingWithAHint(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	if _, err := f.m.Preapprove(PreapprovalInput{ID: "P1", Match: uns.EnrollmentMatch{Key: child.Fingerprint()}, Mount: "hall/new",
		ExpiresAt: f.now.Add(time.Hour)}, person); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(2 * time.Hour)
	f.m.Sweep()
	code, _ := f.ask(child, Request{ULID: "01CHILD"})
	r, _ := f.m.Request(child.Fingerprint())
	if code != 202 || !strings.Contains(r.PreapprovalHint, "P1") || !strings.Contains(r.PreapprovalHint, "expired") {
		t.Fatalf("%d %+v", code, r)
	}
	pre, _ := f.m.Preapprovals("", 10)
	if pre[0].State != uns.PreapprovalExpired {
		t.Fatalf("state %q", pre[0].State)
	}
}

func TestAPreapprovalDecidesARequestThatIsAlreadyWaiting(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	f.ask(child, Request{ULID: "01CHILD"})
	if _, err := f.m.Preapprove(PreapprovalInput{ID: "P1", Match: uns.EnrollmentMatch{Key: child.Fingerprint()}, Mount: "hall/new"}, person); err != nil {
		t.Fatal(err)
	}
	if e, ok := f.reg.Get("01CHILD"); !ok || e.Element != "el-hall-new" {
		t.Fatalf("not approved by the new pre-approval: %+v %v", e, ok)
	}
	// The same command again (a retry) is no error; a different one under the
	// same id is.
	if _, err := f.m.Preapprove(PreapprovalInput{ID: "P1", Match: uns.EnrollmentMatch{Key: child.Fingerprint()}, Mount: "hall/other"}, person); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting pre-approval id: %v", err)
	}
}

func TestAnEKPreapprovalDoesNotMatchAClaim(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.p256("child")
	if _, err := f.m.Preapprove(PreapprovalInput{Match: uns.EnrollmentMatch{EK: child.Fingerprint()}, Element: "el-press"}, person); err != nil {
		t.Fatal(err)
	}
	if code, _ := f.ask(child, Request{ULID: "01CHILD", KeyStore: "tpm"}); code != 202 {
		t.Fatalf("an unattested request matched an EK pre-approval: %d", code)
	}
}

func TestPreapprovalValidation(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	fp := f.key("k").Fingerprint()
	for name, in := range map[string]PreapprovalInput{
		"no match":      {Element: "el-press"},
		"both":          {Match: uns.EnrollmentMatch{EK: fp, Key: fp}, Element: "el-press"},
		"bad fp":        {Match: uns.EnrollmentMatch{Key: "SHA256:12:34"}, Element: "el-press"},
		"no placement":  {Match: uns.EnrollmentMatch{Key: fp}},
		"unknown":       {Match: uns.EnrollmentMatch{Key: fp}, Element: "el-nowhere"},
		"past":          {Match: uns.EnrollmentMatch{Key: fp}, Element: "el-press", ExpiresAt: f.now.Add(-time.Hour)},
		"path as an id": {ID: "a/b", Match: uns.EnrollmentMatch{Key: fp}, Element: "el-press"},
	} {
		if _, err := f.m.Preapprove(in, person); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
	if err := f.m.Unpreapprove("nope", person); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unpreapprove unknown: %v", err)
	}
}

// enrolled approves child as 01CHILD and fetches its certificate.
func (f *fixture) enrolled(child *identity.Identity) {
	f.t.Helper()
	f.ask(child, Request{ULID: "01CHILD"})
	if err := f.m.Approve(child.Fingerprint(), "el-press", "", "", person); err != nil {
		f.t.Fatal(err)
	}
	if code, _ := f.ask(child, Request{ULID: "01CHILD"}); code != 200 {
		f.t.Fatalf("certificate: %d", code)
	}
}

func (f *fixture) keyChange(signer crypto.Signer, ulid string, at time.Time, store string) *KeyChange {
	f.t.Helper()
	kc, err := SignKeyChange(signer, ulid, f.parent.PublicHex(), at.UnixMilli())
	if err != nil {
		f.t.Fatal(err)
	}
	kc.KeyStore = store
	return kc
}

func TestAKeyChangeIntoTheTPMIsAcceptedAutomatically(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	old := f.key("old")
	f.enrolled(old)
	f.kicked = nil
	next := f.p256("next")
	code, resp := f.ask(old, Request{ULID: "01CHILD", KeyChange: f.keyChange(next.Signer, "01CHILD", f.now, "tpm")})
	if code != 200 {
		t.Fatalf("key change: %d %+v", code, resp)
	}
	f.leaf(resp, next) // the certificate is for the new key
	if _, ok := f.reg.ByPubkey(old.PublicHex()); ok {
		t.Fatal("the old key still authenticates")
	}
	e, ok := f.reg.ByPubkey(next.PublicHex())
	if !ok || e.ULID != "01CHILD" || e.Element != "el-press" || e.KeyStore != uns.KeyStoreTPM || e.CertState != uns.CertStateIssued {
		t.Fatalf("entry: %+v", e)
	}
	if len(f.kicked) != 1 {
		t.Fatalf("kicked %v: the old key's sessions must end", f.kicked)
	}
	if f.audits[len(f.audits)-1].Operation != "enroll.key_change" || f.audits[len(f.audits)-1].ReasonCode != "auto" {
		t.Fatalf("audit: %+v", f.audits[len(f.audits)-1])
	}
	// The child did not hear the answer and asks again with the old key: the
	// new one gets its certificate.
	if code, resp := f.ask(old, Request{ULID: "01CHILD", KeyChange: f.keyChange(next.Signer, "01CHILD", f.now, "tpm")}); code != 200 {
		t.Fatalf("repeated key change: %d %+v", code, resp)
	}
}

func TestAKeyChangeWaitsForAPersonUnderApprove(t *testing.T) {
	for name, tc := range map[string]struct {
		policy config.Enrollment
		store  string
	}{
		"policy approve":  {config.Enrollment{KeyChange: "approve"}, "tpm"},
		"down to a file":  {config.Enrollment{}, "file"},
		"unclaimed store": {config.Enrollment{}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, tc.policy)
			old := f.key("old")
			f.enrolled(old)
			next := f.p256("next")
			code, _ := f.ask(old, Request{ULID: "01CHILD", KeyChange: f.keyChange(next.Signer, "01CHILD", f.now, tc.store)})
			if code != 202 {
				t.Fatalf("key change: %d", code)
			}
			r, err := f.m.Request(next.Fingerprint())
			if err != nil || r.KeyChange == nil || r.KeyChange.CurrentFingerprint != old.Fingerprint() || r.ULID != "01CHILD" {
				t.Fatalf("key change record: %+v %v", r, err)
			}
			if _, ok := f.reg.ByPubkey(old.PublicHex()); !ok {
				t.Fatal("the old key stopped working before a decision")
			}
			if err := f.m.Approve(next.Fingerprint(), "", "", "", person); err != nil {
				t.Fatal(err)
			}
			if e, ok := f.reg.ByPubkey(next.PublicHex()); !ok || e.ULID != "01CHILD" || e.Element != "el-press" {
				t.Fatalf("after approval: %+v %v", e, ok)
			}
			// The child polls with its old key, which is gone: the new key gets
			// its certificate.
			code, resp := f.ask(old, Request{ULID: "01CHILD", KeyChange: f.keyChange(next.Signer, "01CHILD", f.now, tc.store)})
			if code != 200 {
				t.Fatalf("after approval: %d %+v", code, resp)
			}
			f.leaf(resp, next)
		})
	}
}

func TestAKeyChangeNeedsProofOfPossession(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	old := f.key("old")
	f.enrolled(old)
	next, other := f.p256("next"), f.p256("other")
	forged := f.keyChange(other.Signer, "01CHILD", f.now, "tpm")
	forged.Pubkey = next.PublicHex()
	stale := f.keyChange(next.Signer, "01CHILD", f.now.Add(-time.Hour), "tpm")
	wrongNode := f.keyChange(next.Signer, "01OTHER", f.now, "tpm")
	for name, kc := range map[string]*KeyChange{"forged": forged, "stale": stale, "other ulid": wrongNode} {
		if code, resp := f.ask(old, Request{ULID: "01CHILD", KeyChange: kc}); code != 400 {
			t.Errorf("%s: %d %+v", name, code, resp)
		}
	}
	if _, ok := f.reg.ByPubkey(old.PublicHex()); !ok {
		t.Fatal("a refused key change changed the key")
	}
}

func TestARequestedKeyChangeTravelsWithTheNextCertificate(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	f.enrolled(child)
	if err := f.m.RequestKeyChange("01CHILD", person); err != nil {
		t.Fatal(err)
	}
	if _, resp := f.ask(child, Request{ULID: "01CHILD"}); !resp.KeyChangeRequested {
		t.Fatalf("renewal does not ask for the key change: %+v", resp)
	}
	next := f.p256("next")
	f.ask(child, Request{ULID: "01CHILD", KeyChange: f.keyChange(next.Signer, "01CHILD", f.now, "tpm")})
	if e, _ := f.reg.ByPubkey(next.PublicHex()); e == nil || e.KeyChangeRequested {
		t.Fatalf("after the key change: %+v", e)
	}
}

func TestTheSameULIDUnderAnotherKeyIsAKeyChangeRequest(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	old := f.key("old")
	f.enrolled(old)
	reflashed := f.p256("reflashed")
	if code, _ := f.ask(reflashed, Request{ULID: "01CHILD", KeyStore: "tpm"}); code != 202 {
		t.Fatalf("%d", code)
	}
	r, _ := f.m.Request(reflashed.Fingerprint())
	if r.KeyChange == nil || r.KeyChange.CurrentFingerprint != old.Fingerprint() {
		t.Fatalf("not shown as a key change: %+v", r)
	}
	if err := f.m.Approve(reflashed.Fingerprint(), "", "", "", person); err != nil {
		t.Fatal(err)
	}
	if code, _ := f.ask(reflashed, Request{ULID: "01CHILD", KeyStore: "tpm"}); code != 200 {
		t.Fatalf("re-flashed device after approval: %d", code)
	}
	if _, ok := f.reg.ByPubkey(old.PublicHex()); ok {
		t.Fatal("the old key survived the key change")
	}
}

func TestAnExternalKeyIsNoNode(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	m := f.key("machine")
	raw, _ := json.Marshal(uns.Entry{ULID: "01M", Pubkey: m.PublicHex(), Kind: uns.KindExternal, Element: "el-press"})
	if _, _, err := f.reg.Enroll(raw); err != nil {
		t.Fatal(err)
	}
	if code, resp := f.ask(m, Request{ULID: "01M"}); code != 403 {
		t.Fatalf("%d %+v", code, resp)
	}
	child := f.key("child")
	if code, _ := f.ask(child, Request{ULID: "01M"}); code != 409 {
		t.Fatalf("a node claiming a machine's ulid: %d", code)
	}
}

func TestDecideMapsTheVerbs(t *testing.T) {
	f := newFixture(t, config.Enrollment{})
	child := f.key("child")
	f.ask(child, Request{ULID: "01CHILD"})
	ctx := uns.CommandContext{ActorID: "sub-1", ActorLabel: "till", ActorKind: "human"}
	cmd := func(verb string, body map[string]any) (int, string) {
		raw, _ := json.Marshal(body)
		code, msg, _, handled := f.m.Decide(ctx, verb, raw)
		if !handled {
			t.Fatalf("%s not handled", verb)
		}
		return code, msg
	}
	if code, msg := cmd("approve", map[string]any{"fingerprint": child.Fingerprint()}); code != 422 {
		t.Fatalf("approve without placement: %d %s", code, msg)
	}
	if code, msg := cmd("approve", map[string]any{"fingerprint": child.Fingerprint(), "mount": "hall/new", "name": "P4"}); code != 200 {
		t.Fatalf("approve: %d %s", code, msg)
	}
	if code, _ := cmd("reject", map[string]any{"fingerprint": child.Fingerprint()}); code != 404 {
		t.Fatalf("reject a decided request: %d", code)
	}
	if code, msg := cmd("preapprove", map[string]any{"id": "P9", "match": map[string]any{"ek": child.Fingerprint()},
		"element": "el-press", "valid_until": f.now.Add(48 * time.Hour).Format(time.RFC3339), "uses": 2, "note": "bench"}); code != 200 {
		t.Fatalf("preapprove: %d %s", code, msg)
	}
	if p, _ := f.m.Preapprovals("", 10); len(p) != 1 || p[0].Uses != 2 || p[0].ExpiresAt != f.now.Add(48*time.Hour).Format(time.RFC3339) || p[0].CreatedBy != "till" {
		t.Fatalf("pre-approval: %+v", p)
	}
	if code, _ := cmd("unpreapprove", map[string]any{"id": "P9"}); code != 200 {
		t.Fatal("unpreapprove")
	}
	if code, _ := cmd("request-key-change", map[string]any{"ulid": "01CHILD"}); code != 200 {
		t.Fatal("request-key-change")
	}
	if code, _ := cmd("block", map[string]any{"fingerprint": child.Fingerprint(), "ulid": "01CHILD", "reason": "lost"}); code != 200 {
		t.Fatal("block")
	}
	if _, ok := f.reg.Get("01CHILD"); ok {
		t.Fatal("block of an active node did not revoke it")
	}
	if code, _ := cmd("unblock", map[string]any{"fingerprint": child.Fingerprint()}); code != 200 {
		t.Fatal("unblock")
	}
	if _, _, _, handled := f.m.Decide(ctx, "enroll", nil); handled {
		t.Fatal("enroll is not an enrollment verb")
	}
}

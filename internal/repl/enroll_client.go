package repl

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/alpamayo-solutions/colca/internal/enroll"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmattest"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
)

// Backoff of a child whose request waits for a decision (§4).
var (
	enrollBackoffMin = 30 * time.Second
	enrollBackoffMax = 5 * time.Minute
)

// EnrollOptions describe the child to its parent.
type EnrollOptions struct {
	ULID           string
	Name           string
	RequestedMount string
	ColcaVersion   string
	// Key locates the node key (identity block); the issued certificate and a
	// key change in progress are kept next to it.
	Key identity.Options
	// HoldsChildren reports whether children are enrolled at this node. Their
	// configs pin its key, so it never changes its key on its own then.
	HoldsChildren func() bool
}

// LoadIssuedCert presents the certificate a parent issued earlier, when the
// file next to the key still names the key, chains to the pinned parent and is
// valid. Anything else is ignored: the next request fetches a new one.
func (c *Client) LoadIssuedCert(path string) {
	raw, err := os.ReadFile(path) //nolint:gosec // next to the node key
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			c.log.Warn("issued certificate not readable; a new one is requested", "path", path, "err", err)
		}
		return
	}
	if err := c.useChain(raw, c.Identity(), time.Now()); err != nil {
		c.log.Info("stored certificate not used; a new one is requested", "path", path, "reason", err.Error())
	}
}

// useChain verifies an issued chain for id and presents it from now on.
func (c *Client) useChain(chainPEM []byte, id *identity.Identity, now time.Time) error {
	certs, err := enroll.VerifyChain(chainPEM, id.Pub, c.pin)
	if err != nil {
		return err
	}
	leaf := certs[0]
	if now.After(leaf.NotAfter) {
		return errors.New("expired")
	}
	tc := &tls.Certificate{PrivateKey: id.Signer, Leaf: leaf}
	for _, cert := range certs {
		tc.Certificate = append(tc.Certificate, cert.Raw)
	}
	c.id.Store(id)
	c.cert.Store(tc)
	c.leaf.Store(leaf)
	c.transport.renew()
	return nil
}

// enrollOutcome is what one round with the parent ended in.
type enrollOutcome int

const (
	outcomeApproved enrollOutcome = iota
	outcomePending
	outcomeRejected
	outcomeBlocked
	outcomeUnsupported
	outcomeFailed
)

// attestSession keeps the AK of a request until its challenge is answered.
type attestSession struct {
	ek *tpmattest.EK
	ak *tpmattest.AK
}

func (a *attestSession) close() {
	if a != nil && a.ak != nil {
		_ = a.ak.Close()
	}
}

// evidenceFor collects TPM evidence that id's key lives in its TPM, or nil for
// a file key or a TPM that cannot certify it.
func (c *Client) evidenceFor(id *identity.Identity) (*tpmattest.Evidence, *attestSession) {
	key, ok := id.TPMKey()
	if !ok {
		return nil, nil
	}
	qd, err := enroll.QualifyingData(c.pin)
	if err != nil {
		return nil, nil
	}
	ek, err := tpmattest.ReadEK(key.Device())
	if err != nil {
		c.log.Info("no TPM attestation: the endorsement key cannot be read", "err", err)
		return nil, nil
	}
	ak, err := tpmattest.CreateAK(key.Device())
	if err != nil {
		c.log.Info("no TPM attestation: no attestation key", "err", err)
		return nil, nil
	}
	ev, err := tpmattest.Collect(ek, ak, key, qd)
	if err != nil {
		_ = ak.Close()
		c.log.Info("no TPM attestation: the node key was not certified", "err", err)
		return nil, nil
	}
	return ev, &attestSession{ek: ek, ak: ak}
}

// post sends one enrollment request.
func (c *Client) postEnroll(ctx context.Context, req enroll.Request) (int, enroll.Response, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return 0, enroll.Response{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, transferGrace)
	defer cancel()
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+enroll.RequestRoute, bytes.NewReader(body))
	if err != nil {
		return 0, enroll.Response{}, err
	}
	hr.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(hr)
	if err != nil {
		return 0, enroll.Response{}, err
	}
	defer resp.Body.Close()
	var out enroll.Response
	if resp.Header.Get("Content-Type") == "application/json" {
		_ = json.NewDecoder(resp.Body).Decode(&out)
	} else {
		out.Reason = readReason(resp)
	}
	return resp.StatusCode, out, nil
}

// enrollRound is one exchange with the parent, answering an attestation
// challenge at once. keyChange is the new key when the round moves the node to
// it. The response is returned for its key_change_requested flag.
func (c *Client) enrollRound(ctx context.Context, o EnrollOptions, next *identity.Identity) (enrollOutcome, enroll.Response) {
	cur := c.Identity()
	req := enroll.Request{
		ULID: o.ULID, Name: o.Name, RequestedMount: o.RequestedMount,
		KeyStore: cur.Store, ColcaVersion: o.ColcaVersion,
	}
	certFor := cur
	var session *attestSession
	defer func() { session.close() }()
	if next != nil {
		kc, err := enroll.SignKeyChange(next.Signer, o.ULID, c.pin, time.Now().UnixMilli())
		if err != nil {
			c.log.Error("key change not signed", "err", err)
			return outcomeFailed, enroll.Response{}
		}
		kc.KeyStore = next.Store
		kc.Attestation, session = c.evidenceFor(next)
		req.KeyChange = kc
		certFor = next
	} else {
		req.Attestation, session = c.evidenceFor(cur)
	}
	for attempt := 0; ; attempt++ {
		code, resp, err := c.postEnroll(ctx, req)
		if err != nil {
			c.log.Debug("enrollment request failed", "err", err)
			return outcomeFailed, resp
		}
		switch {
		case code == http.StatusAccepted && resp.Challenge != nil && session != nil && attempt == 0:
			secret, err := tpmattest.ActivateCredential(session.ak, session.ek, resp.Challenge.CredentialBlob, resp.Challenge.EncryptedSecret)
			if err != nil {
				c.log.Warn("the parent's attestation challenge could not be answered", "err", err)
				return outcomePending, resp
			}
			if req.KeyChange != nil {
				req.KeyChange.Attestation, req.KeyChange.Activation = nil, secret
			} else {
				req.Attestation, req.Activation = nil, secret
			}
			continue
		case code == http.StatusAccepted:
			return outcomePending, resp
		case code == http.StatusOK:
			if err := c.acceptCert(o, resp, certFor, next != nil); err != nil {
				c.log.Error("the parent's certificate was not accepted", "err", err)
				return outcomeFailed, resp
			}
			return outcomeApproved, resp
		case code == http.StatusForbidden && resp.Status == enroll.StatusRejected:
			return outcomeRejected, resp
		case code == http.StatusForbidden && resp.Status == enroll.StatusBlocked:
			return outcomeBlocked, resp
		case code == http.StatusNotFound || code == http.StatusMethodNotAllowed:
			return outcomeUnsupported, resp
		default:
			c.log.Warn("enrollment request refused", "status", code, "reason", resp.Reason)
			return outcomeFailed, resp
		}
	}
}

// acceptCert verifies and stores an issued certificate and presents it. For a
// key change it first makes the new key the node key.
func (c *Client) acceptCert(o EnrollOptions, resp enroll.Response, id *identity.Identity, keyChange bool) error {
	chain := []byte(resp.Certificate)
	if _, err := enroll.VerifyChain(chain, id.Pub, c.pin); err != nil {
		return err
	}
	if keyChange {
		if err := identity.PromotePending(o.Key); err != nil {
			return err
		}
		old := c.Identity()
		c.log.Info("key change accepted by the parent: the node key is now the new key; the old key file is gone",
			"fingerprint", id.Fingerprint(), "key_store", id.Store, "previous", old.Fingerprint())
		c.log.Warn("restart colcad to serve the new key on its own doors as well; the uplink uses it already")
	}
	if err := writeAtomic(o.Key.CertFile(), chain); err != nil {
		c.log.Warn("issued certificate not stored; it is requested again after a restart", "err", err)
	}
	return c.useChain(chain, id, time.Now())
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// pendingKey returns the key a key change moves to: one already in progress,
// a new TPM key when the configuration asks for the TPM and the key is a file
// key, or a new key when the parent asked for one. nil means no key change.
func (c *Client) pendingKey(o EnrollOptions, requested bool) *identity.Identity {
	if next, err := identity.OpenPending(o.Key); err == nil {
		return next
	} else if !errors.Is(err, fs.ErrNotExist) {
		c.log.Error("the pending key of a key change cannot be loaded", "path", o.Key.PendingKeyFile(), "err", err)
		return nil
	}
	cur := c.Identity()
	wantTPM := o.Key.KeyStore == identity.StoreTPM && cur.Store == identity.StoreFile
	if !wantTPM && !requested {
		return nil
	}
	if o.HoldsChildren != nil && o.HoldsChildren() {
		c.log.Warn("this node does not change its key on its own: its children pin it. Re-pin them after a manual key change.",
			"key_store", cur.Store, "wanted", o.Key.KeyStore, "requested_by_parent", requested)
		return nil
	}
	store := identity.StoreFile
	if wantTPM || cur.Store == identity.StoreTPM {
		store = identity.StoreTPM
	} else if o.Key.KeyStore != identity.StoreFile {
		if dev, err := tpmkey.Open(tpmDevice(o.Key)); err == nil {
			_ = dev.Close()
			store = identity.StoreTPM
		}
	}
	next, err := identity.MintPending(o.Key, store)
	if err != nil {
		c.log.Error("no new key for the key change", "key_store", store, "err", err)
		return nil
	}
	c.log.Info("key change: a new key was created and waits for the parent", "fingerprint", next.Fingerprint(),
		"key_store", next.Store, "current", cur.Fingerprint())
	return next
}

func tpmDevice(o identity.Options) string {
	if o.TPMDevice == "" {
		return identity.DefaultTPMDevice
	}
	return o.TPMDevice
}

// RunEnrollment keeps this node enrolled at its parent (§4, §7.2, §8): it asks
// for a certificate when it has none, renews it at two thirds of its lifetime,
// asks again when the parent refuses the key, moves the key into the TPM when
// the configuration asks for it, and waits with backoff while a person
// decides. It returns when stop closes.
func RunEnrollment(c *Client, o EnrollOptions, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	l := &enrollLoop{c: c, o: o, backoff: enrollBackoffMin}
	for {
		wait, done := l.step(ctx)
		if done {
			<-stop
			return
		}
		wait = max(wait, time.Second)
		changes := c.StatusChanges()
		timer := time.NewTimer(wait)
		select {
		case <-stop:
			timer.Stop()
			return
		case <-timer.C:
		case <-changes:
			timer.Stop()
		}
	}
}

type enrollLoop struct {
	c       *Client
	o       EnrollOptions
	backoff time.Duration
	// requested is set when the parent asked for a key change.
	requested bool
	last      time.Time
	// logged keeps a waiting or blocked child from logging every retry.
	pendingLogged, blockedLogged bool
}

// step runs one round when one is due and returns how long to wait before
// the next; done means never again (rejected) until a restart.
func (l *enrollLoop) step(ctx context.Context) (time.Duration, bool) {
	c := l.c
	next := c.pendingKey(l.o, l.requested)
	leaf := c.leaf.Load()
	due := next != nil || leaf == nil || time.Now().After(enroll.RenewAt(leaf)) || c.Status().State == UplinkUnauthorized
	if !due {
		return time.Until(enroll.RenewAt(leaf)), false
	}
	// Never faster than the minimum backoff, also when the parent keeps
	// refusing a certificate it just issued.
	if since := time.Since(l.last); !l.last.IsZero() && since < enrollBackoffMin && !l.requested {
		closeUnused(next)
		return enrollBackoffMin - since, false
	}
	l.last, l.requested = time.Now(), false
	outcome, resp := c.enrollRound(ctx, l.o, next)
	if outcome != outcomeApproved {
		closeUnused(next)
	}
	switch outcome {
	case outcomeApproved:
		l.backoff, l.pendingLogged, l.blockedLogged = enrollBackoffMin, false, false
		c.log.Info("certificate from the parent", "not_after", c.CertNotAfter(), "key_store", resp.KeyStore,
			"fingerprint", c.Identity().Fingerprint())
		if resp.KeyChangeRequested {
			l.requested = true
			return 0, false
		}
		return time.Until(enroll.RenewAt(c.leaf.Load())), false
	case outcomePending:
		if !l.pendingLogged {
			c.log.Warn("waiting for a person to approve this node at its parent — compare the fingerprint",
				"fingerprint", resp.Fingerprint, "parent", c.base, "key_store", resp.KeyStore)
			l.pendingLogged = true
		}
		return l.grow(), false
	case outcomeRejected:
		c.log.Error("the parent rejected this node; it does not ask again until colcad restarts",
			"reason", resp.Reason, "fingerprint", resp.Fingerprint, "parent", c.base)
		return 0, true
	case outcomeBlocked:
		if !l.blockedLogged {
			c.log.Error("the parent blocked this node's key; it keeps asking rarely in case the block is lifted",
				"reason", resp.Reason, "fingerprint", resp.Fingerprint, "parent", c.base)
			l.blockedLogged = true
		}
		return enrollBackoffMax, false
	case outcomeUnsupported:
		c.log.Info("the parent serves no enrollment requests (an older release); the node keeps its self-signed certificate",
			"parent", c.base)
		return time.Hour, false
	}
	return l.grow(), false
}

func (l *enrollLoop) grow() time.Duration {
	wait := l.backoff
	l.backoff = min(l.backoff*2, enrollBackoffMax)
	return wait
}

func closeUnused(id *identity.Identity) {
	if id != nil {
		_ = id.Close()
	}
}

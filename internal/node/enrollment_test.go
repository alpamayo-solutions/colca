package node

import (
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-tpm/tpm2"

	"github.com/alpamayo-solutions/colca/internal/authtest"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmattest"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmkey"
	"github.com/alpamayo-solutions/colca/internal/identity/tpmtest"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"github.com/alpamayo-solutions/colca/internal/tokenauth/tokentest"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// call is one request at a node's API door with the given auth header.
func call(t *testing.T, n *Node, method, path, header, value string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = strings.NewReader(string(raw))
	}
	req, err := http.NewRequest(method, "https://"+n.APIAddr+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if header != "" {
		req.Header.Set(header, value)
	}
	resp, err := httpsClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func healthz(t *testing.T, n *Node) map[string]any {
	t.Helper()
	code, out := call(t, n, http.MethodGet, "/healthz", "", "", nil)
	if code != http.StatusOK {
		t.Fatalf("/healthz %d", code)
	}
	return out
}

func eventually(t *testing.T, what string, within time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A child nobody enrolled asks by itself, a person approves it with their own
// token (the admin token is refused for the decision), the child fetches its
// certificate, presents it, keeps it across a restart, and a revoke ends its
// session at once.
func TestAChildAsksAPersonApprovesAndItReplicates(t *testing.T) {
	t.Cleanup(repl.SetEnrollBackoff(200*time.Millisecond, time.Second))
	base := t.TempDir()
	iss := tokentest.NewIssuer(t)
	parentKey, childKey := filepath.Join(base, "parent.key"), filepath.Join(base, "child.key")
	parentID, childID := genKey(t, parentKey), genKey(t, childKey)
	parent := mustStart(t, &config.Config{
		ULID: "n-parent", DataDir: filepath.Join(base, "parent-data"),
		Identity: config.Identity{KeyFile: parentKey},
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		Repl:     config.Endpoint{Addr: "127.0.0.1:0"},
		Auth:     &config.Auth{Issuers: []config.AuthIssuer{{URL: iss.Iss()}}, Audience: iss.Aud(), JWKSURL: iss.JWKSURL()},
	})
	element := authtest.Place(t, parent.Engine, "hall/press")
	childCfg := &config.Config{
		ULID: "n-child", DataDir: filepath.Join(base, "child-data"),
		Identity: config.Identity{KeyFile: childKey},
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		Parent:   &config.Parent{URL: "https://" + parent.ReplAddr, Pubkey: parentID.PublicHex(), Mount: "hall/press"},
	}
	child := mustStart(t, childCfg)
	if h := healthz(t, child); h["fingerprint"] != childID.Fingerprint() || h["cert_not_after"] != nil {
		t.Fatalf("child /healthz before approval: %v", h)
	}

	admin := "Bearer " + iss.Mint("till", []string{"admin:#"}, time.Now().Add(time.Hour))
	var listed []any
	eventually(t, "the child's request at the parent", 10*time.Second, func() bool {
		_, out := call(t, parent, http.MethodGet, "/enroll/requests?state=pending", "Authorization", admin, nil)
		listed, _ = out["requests"].([]any)
		return len(listed) == 1
	})
	req := listed[0].(map[string]any)
	if req["fingerprint"] != childID.Fingerprint() || req["ulid"] != "n-child" || req["requested_mount"] != "hall/press" || req["key_store"] != "file" {
		t.Fatalf("listed request: %v", req)
	}
	fpPath := "/enroll/requests/" + childID.FingerprintID()
	if code, _ := call(t, parent, http.MethodGet, "/enroll/requests", "X-Colca-Token", tok, nil); code != http.StatusForbidden {
		t.Fatalf("the admin token listed requests: %d", code)
	}
	if code, _ := call(t, parent, http.MethodPost, fpPath+"/approve", "X-Colca-Token", tok, map[string]any{"element": element}); code != http.StatusForbidden {
		t.Fatalf("the admin token approved: %d", code)
	}
	reader := "Bearer " + iss.Mint("reader", []string{"read:#"}, time.Now().Add(time.Hour))
	if code, _ := call(t, parent, http.MethodPost, fpPath+"/approve", "Authorization", reader, map[string]any{"element": element}); code != http.StatusForbidden {
		t.Fatalf("a person without admin:# approved: %d", code)
	}
	// A node is never enrolled by entry through the admin token.
	if code, _ := call(t, parent, http.MethodPost, "/enroll", "X-Colca-Token", tok, map[string]any{
		"ulid": "n-other", "pubkey": parentID.PublicHex(), "kind": "node", "element": element,
	}); code != http.StatusUnprocessableEntity {
		t.Fatalf("POST /enroll of a node: %d", code)
	}
	if code, out := call(t, parent, http.MethodPost, fpPath+"/approve", "Authorization", admin, map[string]any{"element": element}); code != http.StatusOK {
		t.Fatalf("approve: %d %v", code, out)
	}

	eventually(t, "the child's certificate", 10*time.Second, func() bool { return healthz(t, child)["cert_not_after"] != nil })
	code, out := apiCall(t, child, http.MethodPost, "/publish", map[string]any{"topic": "colca/v1/_Metric/n-child/m1/temp", "payload": map[string]any{"v": 1}})
	if code != http.StatusOK {
		t.Fatalf("publish: %d %v", code, out)
	}
	eventually(t, "the metric at the parent", 15*time.Second, func() bool { return len(mustKVScan(t, parent.Store, "hall/press/m1/temp")) == 1 })
	e, _ := parent.Registry.Get("n-child")
	if e.CertState != uns.CertStateIssued || e.Fingerprint != childID.Fingerprint() || e.KeyStore != uns.KeyStoreFile {
		t.Fatalf("entry: %+v", e)
	}
	raw, err := os.ReadFile(childKey + ".crt")
	if err != nil || strings.Count(string(raw), "BEGIN CERTIFICATE") != 2 {
		t.Fatalf("certificate next to the key: %v %q", err, raw)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatal("no PEM")
	}

	// A restarted child presents its stored certificate at once.
	notAfter := healthz(t, child)["cert_not_after"]
	child.Stop()
	child = mustStart(t, childCfg)
	if got := healthz(t, child)["cert_not_after"]; got != notAfter {
		t.Fatalf("after restart cert_not_after %v, want the stored %v", got, notAfter)
	}
	eventually(t, "the restarted child connected", 10*time.Second, func() bool {
		up, _ := healthz(t, child)["uplink"].(map[string]any)
		return up["state"] == "connected"
	})

	// Revoke: the waiting downlink poll ends at once, the child is refused.
	if code, _ := call(t, parent, http.MethodDelete, "/enroll/n-child", "Authorization", admin, nil); code != http.StatusOK {
		t.Fatalf("revoke: %d", code)
	}
	eventually(t, "the revoked child refused", 5*time.Second, func() bool {
		up, _ := healthz(t, child)["uplink"].(map[string]any)
		return up["state"] == "unauthorized"
	})
	// It asks again, as a pending request.
	eventually(t, "the revoked child asking again", 10*time.Second, func() bool {
		_, out := call(t, parent, http.MethodGet, "/enroll/requests", "Authorization", admin, nil)
		l, _ := out["requests"].([]any)
		return len(l) == 1
	})
}

// A revoke ends a child's waiting downlink poll within moments, not after the
// 20-second long poll (node enrollment spec §8.1).
func TestRevokeEndsTheWaitingDownlinkPollAtOnce(t *testing.T) {
	t.Cleanup(repl.SetEnrollBackoff(time.Hour, time.Hour)) // the child must not re-enroll here
	base := t.TempDir()
	parentKey, childKey := filepath.Join(base, "parent.key"), filepath.Join(base, "child.key")
	parentID, childID := genKey(t, parentKey), genKey(t, childKey)
	parent := mustStart(t, &config.Config{
		ULID: "n-parent", DataDir: filepath.Join(base, "parent-data"),
		Identity: config.Identity{KeyFile: parentKey},
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		Repl:     config.Endpoint{Addr: "127.0.0.1:0"},
	})
	authtest.EnrollNodeAt(t, parent.Registry, parent.Engine, "n-child", childID.PublicHex(), "child1")
	child := mustStart(t, &config.Config{
		ULID: "n-child", DataDir: filepath.Join(base, "child-data"),
		Identity: config.Identity{KeyFile: childKey},
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		Parent:   &config.Parent{URL: "https://" + parent.ReplAddr, Pubkey: parentID.PublicHex()},
	})
	eventually(t, "the child connected", 10*time.Second, func() bool {
		up, _ := healthz(t, child)["uplink"].(map[string]any)
		return up["state"] == "connected"
	})
	time.Sleep(300 * time.Millisecond) // the poll is waiting at the parent now
	start := time.Now()
	if _, _, err := parent.Registry.Revoke("n-child"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the downlink poll ended", 5*time.Second, func() bool {
		up, _ := healthz(t, child)["uplink"].(map[string]any)
		return up["state"] == "unauthorized"
	})
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("the revoked child's poll ended after %s", took)
	}
}

// An IPC upgraded to key_store tpm while its key is a file moves the key into
// the TPM by itself: TPM attestation against the parent (an EK certificate
// from a manufacturer the parent trusts), an automatic key change signed by
// the new key, a certificate for the new key; the entry, its element and the
// cursors stay, so nothing is sent twice and nothing is lost (spec §7.2, §11).
func TestATPMChildMovesItsFileKeyIntoTheTPMWithoutLosingData(t *testing.T) {
	sock := tpmtest.Start(t)
	dev, err := tpmkey.Open(sock)
	if err != nil {
		t.Fatal(err)
	}
	_, manufacturer := tpmtest.ProvisionEKCert(t, dev, tpm2.ECCEKTemplate, tpmattest.EKCertIndexECC)
	_ = dev.Close()

	base := t.TempDir()
	roots := filepath.Join(base, "tpm-roots.pem")
	if err := os.WriteFile(roots, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: manufacturer.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	parentKey, childKey := filepath.Join(base, "parent.key"), filepath.Join(base, "child.key")
	parentID, fileID := genKey(t, parentKey), genKey(t, childKey)
	parent := mustStart(t, &config.Config{
		ULID: "n-parent", DataDir: filepath.Join(base, "parent-data"),
		Identity:   config.Identity{KeyFile: parentKey},
		API:        config.API{Addr: "127.0.0.1:0", Token: tok},
		Repl:       config.Endpoint{Addr: "127.0.0.1:0"},
		Enrollment: config.Enrollment{TPMRoots: roots},
	})
	// The child was enrolled by the release before: adopted, cert_state none.
	authtest.EnrollNodeAt(t, parent.Registry, parent.Engine, "n-child", fileID.PublicHex(), "child1")
	if _, err := parent.Registry.Update("n-child", func(e *uns.Entry) error { e.CertState = uns.CertStateNone; return nil }); err != nil {
		t.Fatal(err)
	}
	childCfg := &config.Config{
		ULID: "n-child", DataDir: filepath.Join(base, "child-data"),
		Identity: config.Identity{KeyFile: childKey, KeyStore: identity.StoreFile},
		API:      config.API{Addr: "127.0.0.1:0", Token: tok},
		Parent:   &config.Parent{URL: "https://" + parent.ReplAddr, Pubkey: parentID.PublicHex()},
	}
	child := mustStart(t, childCfg)
	publish := func(n *Node, from, to int) {
		for v := from; v <= to; v++ {
			if code, out := apiCall(t, n, http.MethodPost, "/publish", map[string]any{
				"topic": "colca/v1/_Metric/n-child/m1/temp", "payload": map[string]any{"v": v},
			}); code != http.StatusOK {
				t.Fatalf("publish %d: %d %v", v, code, out)
			}
		}
	}
	countAtParent := func() int {
		recs, _, err := parent.Store.Read("metrics", 1, 10000, func(topic string) bool { return strings.HasSuffix(topic, "/child1/m1/temp") })
		if err != nil {
			t.Fatal(err)
		}
		return len(recs)
	}
	publish(child, 1, 5)
	eventually(t, "five metrics at the parent", 15*time.Second, func() bool { return countAtParent() == 5 })
	eventually(t, "the file key's certificate", 10*time.Second, func() bool { return healthz(t, child)["cert_not_after"] != nil })
	child.Stop()

	// The upgrade: the deployment now declares the TPM.
	childCfg.Identity.KeyStore = identity.StoreTPM
	childCfg.Identity.TPMDevice = sock
	child = mustStart(t, childCfg)
	var e *uns.Entry
	eventually(t, "the key change at the parent", 20*time.Second, func() bool {
		var ok bool
		e, ok = parent.Registry.Get("n-child")
		return ok && e.Fingerprint != fileID.Fingerprint() && e.CertState == uns.CertStateIssued
	})
	if e.KeyStore != uns.KeyStoreTPMAttested || e.EKManufacturer != "Infineon" || e.Element == "" {
		t.Fatalf("entry after the key change: %+v", e)
	}
	if _, ok := parent.Registry.ByPubkey(fileID.PublicHex()); ok {
		t.Fatal("the file key still authenticates")
	}
	eventually(t, "the child presenting the TPM key", 10*time.Second, func() bool {
		h := healthz(t, child)
		return h["key_store"] == "tpm" && h["fingerprint"] == e.Fingerprint
	})
	raw, err := os.ReadFile(childKey)
	if err != nil || !tpmkey.IsBlob(raw) {
		t.Fatalf("the key file is not the TPM blob: %v", err)
	}
	if _, err := os.Stat(childKey + ".next"); !os.IsNotExist(err) {
		t.Fatalf("the pending key is still there: %v", err)
	}

	publish(child, 6, 10)
	eventually(t, "ten metrics at the parent", 15*time.Second, func() bool { return countAtParent() >= 10 })
	time.Sleep(500 * time.Millisecond)
	if n := countAtParent(); n != 10 {
		t.Fatalf("%d metric records at the parent, want exactly 10: the uplink restarted or skipped", n)
	}

	// A restart loads the TPM key from the blob and keeps going.
	child.Stop()
	child = mustStart(t, childCfg)
	if h := healthz(t, child); h["key_store"] != "tpm" || h["fingerprint"] != e.Fingerprint {
		t.Fatalf("after restart: %v", h)
	}
	publish(child, 11, 11)
	eventually(t, "eleven metrics at the parent", 15*time.Second, func() bool { return countAtParent() == 11 })
}

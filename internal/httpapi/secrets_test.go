package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/registry"
	"github.com/alpamayo-solutions/colca/internal/secretstore"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/secrets"
)

func secretTestHandler(t *testing.T, local bool) http.Handler {
	t.Helper()
	state, err := store.Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	secretDB, err := secretstore.Open(filepath.Join(t.TempDir(), "secrets"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secretDB.Close() })
	cfg := &config.Config{ULID: "n-test", API: config.API{Token: "tok"}}
	reg, err := registry.New(state, cfg.ULID)
	if err != nil {
		t.Fatal(err)
	}
	m := metrics.New(state, config.Retention{}, nil)
	reg.SetMetrics(m)
	eng := engine.New(state, cfg, reg, nil, m, nil)
	reg.SetNamespace(eng.Elements())
	return Handler(eng, cfg, reg, nil, m, testBlobs(t, cfg), "deadbeef", local, nil, secretDB)
}

func sealedWriteBody(t *testing.T, plaintext string, expires *time.Time, expected *uint64) []byte {
	t.Helper()
	ring, err := secrets.OpenKeyring(filepath.Join(t.TempDir(), "keys"))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := ring.SealForActive([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"envelope": envelope, "expires_at": expires, "expected_revision": expected})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func secretRequest(t *testing.T, h http.Handler, service, token, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if service != "" {
		req.Header.Set("X-Colca-Service", service)
	}
	if token != "" {
		req.Header.Set("X-Colca-Token", token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestLocalServicesStoreAndRetrieveOnlyTheirOwnCiphertext(t *testing.T) {
	h := secretTestHandler(t, true)
	body := sealedWriteBody(t, "openai-key", nil, nil)
	put := secretRequest(t, h, "assistant", "", http.MethodPut, "/secrets/model-providers/alpha", body)
	if put.Code != http.StatusOK {
		t.Fatalf("assistant put = %d: %s", put.Code, put.Body.String())
	}
	get := secretRequest(t, h, "assistant", "", http.MethodGet, "/secrets/model-providers/alpha", nil)
	if get.Code != http.StatusOK {
		t.Fatalf("assistant get = %d: %s", get.Code, get.Body.String())
	}
	var record secretstore.Record
	if err := json.Unmarshal(get.Body.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record.Owner != "assistant" || record.Envelope.Ciphertext == "" {
		t.Fatalf("record = %+v", record)
	}
	other := secretRequest(t, h, "notifications", "", http.MethodGet, "/secrets/model-providers/alpha", nil)
	if other.Code != http.StatusNotFound {
		t.Fatalf("other service get = %d, want 404", other.Code)
	}
}

func TestAdminCanProvisionButReadsMetadataWithoutCiphertext(t *testing.T) {
	h := secretTestHandler(t, false)
	body := sealedWriteBody(t, "beta-key", nil, nil)
	put := secretRequest(t, h, "", "tok", http.MethodPut, "/secrets/assistant/model-providers/beta", body)
	if put.Code != http.StatusOK {
		t.Fatalf("admin put = %d: %s", put.Code, put.Body.String())
	}
	if bytes.Contains(put.Body.Bytes(), []byte("ciphertext")) {
		t.Fatalf("admin put echoed ciphertext: %s", put.Body.String())
	}
	get := secretRequest(t, h, "", "tok", http.MethodGet, "/secrets/assistant/model-providers/beta", nil)
	if get.Code != http.StatusOK || bytes.Contains(get.Body.Bytes(), []byte("ciphertext")) {
		t.Fatalf("admin metadata get = %d: %s", get.Code, get.Body.String())
	}
	unauthorized := secretRequest(t, h, "", "", http.MethodGet, "/secrets/assistant", nil)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized list = %d, want 401", unauthorized.Code)
	}
}

func TestExpiredSecretMetadataRemainsVisibleButCiphertextIsGone(t *testing.T) {
	h := secretTestHandler(t, true)
	expires := time.Now().Add(-time.Minute)
	body := sealedWriteBody(t, "expired", &expires, nil)
	if rec := secretRequest(t, h, "assistant", "", http.MethodPut, "/secrets/expired", body); rec.Code != http.StatusOK {
		t.Fatalf("put expired = %d: %s", rec.Code, rec.Body.String())
	}
	get := secretRequest(t, h, "assistant", "", http.MethodGet, "/secrets/expired", nil)
	if get.Code != http.StatusGone || bytes.Contains(get.Body.Bytes(), []byte("ciphertext")) {
		t.Fatalf("expired get = %d: %s", get.Code, get.Body.String())
	}
	list := secretRequest(t, h, "assistant", "", http.MethodGet, "/secrets", nil)
	if list.Code != http.StatusOK || !bytes.Contains(list.Body.Bytes(), []byte(`"expired":true`)) {
		t.Fatalf("expired list = %d: %s", list.Code, list.Body.String())
	}
}

func TestSecretRoutesDoNotExistWhenStoreIsDisabled(t *testing.T) {
	h := newTestHandler(t, &config.Config{ULID: "n-test", API: config.API{Token: "tok"}})
	rec := secretRequest(t, h, "", "tok", http.MethodGet, "/secrets/assistant", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("disabled secret route = %d, want 404", rec.Code)
	}
}

// Package door is the client for a colca node's HTTP API.
//
// One client for one door. `colca-grantsync` reads the KV and publishes through
// it; `colca-historian` follows a stream through it; whatever comes next uses
// the same one. A second client for the same six routes would be two places for
// a header, a status-code rule or a retry to be wrong.
//
// Where the door is and what it wants for a credential is CONFIGURATION: an
// admin token on the published API door today, a service NAME and no credential
// on the unpublished local door once that lands
// (2026-08-19-colca-local-service-trust-design.md §4).
package door

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/secrets"
)

// Client talks to one node.
type Client struct {
	BaseURL string
	Token   string // admin token, for the published API door
	Service string // service name, for the local door (no credential)
	HTTP    *http.Client
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.Token != "" {
		req.Header.Set("X-Colca-Token", c.Token)
	}
	if c.Service != "" {
		req.Header.Set("X-Colca-Service", c.Service)
	}
	return c.http().Do(req)
}

// Record is one stored record as the door returns it.
type Record struct {
	Offset       int64           `json:"offset"`
	OriginOffset int64           `json:"origin_offset"`
	Topic        string          `json:"topic"`
	Payload      json.RawMessage `json:"payload"`
	TS           int64           `json:"ts"`
	WrittenBy    string          `json:"written_by"`
	ActorID      string          `json:"actor_id"`
	ActorLabel   string          `json:"actor_label"`
	ActorKind    string          `json:"actor_kind"`
}

// Gap is the contiguous pruned stream prefix a cursor can no longer read.
type Gap struct {
	Stream     string `json:"stream"`
	FromOffset int64  `json:"from_offset"`
	ToOffset   int64  `json:"to_offset"`
	FirstTS    int64  `json:"first_ts"`
	LastTS     int64  `json:"last_ts"`
	Approx     bool   `json:"approx"`
}

// Page is one /fetch response.
type Page struct {
	Records []Record `json:"records"`
	Next    int64    `json:"next"`
	Gap     *Gap     `json:"gap,omitempty"`
}

// FetchOptions are the server-side view applied to one side-effect-free read.
type FetchOptions struct {
	Stream    string
	Cursor    string
	Max       int
	Prefix    string
	SignalIDs []string
}

// KVEntry is one retained record returned by /kv.
type KVEntry struct {
	Path    string          `json:"path"`
	NodeID  string          `json:"node_id"`
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
	TS      int64           `json:"ts"`
	Offset  int64           `json:"offset"`
}

// Self describes the registry identity resolved for a local service request.
type Self struct {
	ULID    string `json:"ulid"`
	Name    string `json:"name"`
	Node    string `json:"node"`
	Element string `json:"element"`
	Mount   string `json:"mount"`
}

// SecretRecord is returned only to the owning service on the local door.
type SecretRecord struct {
	Owner     string           `json:"owner"`
	Name      string           `json:"name"`
	Envelope  secrets.Envelope `json:"envelope"`
	Revision  uint64           `json:"revision"`
	ExpiresAt *time.Time       `json:"expires_at,omitempty"`
	UpdatedAt time.Time        `json:"updated_at"`
}

// SecretMetadata is safe for administration views and never carries ciphertext.
type SecretMetadata struct {
	Owner     string     `json:"owner"`
	Name      string     `json:"name"`
	Revision  uint64     `json:"revision"`
	KeyID     string     `json:"key_id"`
	Algorithm string     `json:"algorithm"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	UpdatedAt time.Time  `json:"updated_at"`
	Expired   bool       `json:"expired"`
}

// SecretWrite supplies an already-sealed envelope. ExpectedRevision nil is an
// unconditional upsert, 0 is create-only, and a positive value is CAS.
type SecretWrite struct {
	Envelope         secrets.Envelope `json:"envelope"`
	ExpiresAt        *time.Time       `json:"expires_at,omitempty"`
	ExpectedRevision *uint64          `json:"expected_revision,omitempty"`
}

// Fetch reads one page. It does NOT move the cursor: reading is side-effect
// free, which is what lets a consumer that dies mid-batch re-read exactly what
// it had not acked.
func (c *Client) Fetch(ctx context.Context, stream, cursor string, limit int) (Page, error) {
	return c.FetchWithOptions(ctx, FetchOptions{Stream: stream, Cursor: cursor, Max: limit})
}

// FetchWithOptions reads one page using optional hierarchy and metric signal
// filters. It does NOT move the cursor; /ack remains the only cursor mutation.
func (c *Client) FetchWithOptions(ctx context.Context, options FetchOptions) (Page, error) {
	q := url.Values{
		"stream": {options.Stream},
		"cursor": {options.Cursor},
		"max":    {strconv.Itoa(options.Max)},
	}
	if options.Prefix != "" {
		q.Set("prefix", options.Prefix)
	}
	for _, signalID := range options.SignalIDs {
		q.Add("signal_id", signalID)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/fetch?"+q.Encode(), nil)
	if err != nil {
		return Page{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return Page{}, fmt.Errorf("fetching %s: %w", options.Stream, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return Page{}, fmt.Errorf("fetching %s: HTTP %d: %s", options.Stream, resp.StatusCode, truncate(reason, 300))
	}
	var page Page
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return Page{}, fmt.Errorf("fetching %s: %w", options.Stream, err)
	}
	return page, nil
}

// KV returns retained entries visible below prefix, narrowed to the named uns
// contracts when any are given (none means every contract, as the door does).
//
// It follows the door's paging to the end and returns entries only for a
// listing it read completely: a page that cannot be read fails the whole call,
// and a caller never sees a short listing dressed as a small one. That is
// load-bearing for a consumer that converges on absence — colca-grantsync
// retires the Keycloak resource of any element the listing does not hold, and
// Keycloak does not restore that resource's permissions when it comes back.
func (c *Client) KV(ctx context.Context, prefix string, contracts ...string) ([]KVEntry, error) {
	var entries []KVEntry
	after := ""
	for {
		q := url.Values{"max": {"10000"}}
		if prefix != "" {
			q.Set("prefix", prefix)
		}
		for _, contract := range contracts {
			q.Add("contract", contract)
		}
		if after != "" {
			q.Set("after", after)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/kv?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.do(req)
		if err != nil {
			return nil, fmt.Errorf("reading retained state: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			reason, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("reading retained state: HTTP %d: %s", resp.StatusCode, truncate(reason, 300))
		}
		var page struct {
			Entries []KVEntry `json:"entries"`
			Next    string    `json:"next"`
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading retained state: %w", err)
		}
		entries = append(entries, page.Entries...)
		if page.Next == "" {
			return entries, nil
		}
		if page.Next == after {
			return nil, fmt.Errorf("reading retained state: server repeated page token")
		}
		after = page.Next
	}
}

// Self reads the authoritative registry identity for a local service.
func (c *Client) Self(ctx context.Context) (Self, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/self", nil)
	if err != nil {
		return Self{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return Self{}, fmt.Errorf("reading local identity: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return Self{}, fmt.Errorf("reading local identity: HTTP %d: %s", resp.StatusCode, truncate(reason, 300))
	}
	var self Self
	if err := json.NewDecoder(resp.Body).Decode(&self); err != nil {
		return Self{}, fmt.Errorf("reading local identity: %w", err)
	}
	if self.ULID == "" || self.Node == "" {
		return Self{}, fmt.Errorf("reading local identity: response has no service or node identity")
	}
	return self, nil
}

// PutSecret stores ciphertext under the calling local service's namespace.
func (c *Client) PutSecret(ctx context.Context, name string, value SecretWrite) (SecretMetadata, error) {
	return c.putSecret(ctx, "/secrets/"+escapeSecretPath(name), name, value)
}

// PutSecretFor provisions ciphertext for owner through the admin door.
func (c *Client) PutSecretFor(ctx context.Context, owner, name string, value SecretWrite) (SecretMetadata, error) {
	return c.putSecret(ctx, "/secrets/"+url.PathEscape(owner)+"/"+escapeSecretPath(name), owner+"/"+name, value)
}

func (c *Client) putSecret(ctx context.Context, endpoint, label string, value SecretWrite) (SecretMetadata, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return SecretMetadata{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.BaseURL+endpoint, bytes.NewReader(body))
	if err != nil {
		return SecretMetadata{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return SecretMetadata{}, fmt.Errorf("storing secret %s: %w", label, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return SecretMetadata{}, fmt.Errorf("storing secret %s: HTTP %d: %s", label, resp.StatusCode, truncate(reason, 300))
	}
	var out struct {
		Secret SecretMetadata `json:"secret"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return SecretMetadata{}, fmt.Errorf("storing secret %s: %w", label, err)
	}
	return out.Secret, nil
}

// GetSecret returns ciphertext to the owning service on the local door.
func (c *Client) GetSecret(ctx context.Context, name string) (SecretRecord, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/secrets/"+escapeSecretPath(name), nil)
	if err != nil {
		return SecretRecord{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return SecretRecord{}, fmt.Errorf("reading secret %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return SecretRecord{}, fmt.Errorf("reading secret %s: HTTP %d: %s", name, resp.StatusCode, truncate(reason, 300))
	}
	var out SecretRecord
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return SecretRecord{}, fmt.Errorf("reading secret %s: %w", name, err)
	}
	return out, nil
}

// SecretInfoFor reads metadata through the admin door without ciphertext.
func (c *Client) SecretInfoFor(ctx context.Context, owner, name string) (SecretMetadata, error) {
	endpoint := c.BaseURL + "/secrets/" + url.PathEscape(owner) + "/" + escapeSecretPath(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return SecretMetadata{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return SecretMetadata{}, fmt.Errorf("reading secret metadata %s/%s: %w", owner, name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return SecretMetadata{}, fmt.Errorf("reading secret metadata %s/%s: HTTP %d: %s", owner, name, resp.StatusCode, truncate(reason, 300))
	}
	var out struct {
		Secret SecretMetadata `json:"secret"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return SecretMetadata{}, fmt.Errorf("reading secret metadata %s/%s: %w", owner, name, err)
	}
	return out.Secret, nil
}

// ListSecrets lists metadata under the calling local service's namespace.
func (c *Client) ListSecrets(ctx context.Context) ([]SecretMetadata, error) {
	return c.listSecrets(ctx, "/secrets", "local service")
}

// ListSecretsFor lists one service's metadata through the admin door.
func (c *Client) ListSecretsFor(ctx context.Context, owner string) ([]SecretMetadata, error) {
	return c.listSecrets(ctx, "/secrets/"+url.PathEscape(owner), owner)
}

func (c *Client) listSecrets(ctx context.Context, endpoint, label string) ([]SecretMetadata, error) {
	var secrets []SecretMetadata
	after := ""
	for {
		q := url.Values{"max": {"10000"}}
		if after != "" {
			q.Set("after", after)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+endpoint+"?"+q.Encode(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.do(req)
		if err != nil {
			return nil, fmt.Errorf("listing secrets for %s: %w", label, err)
		}
		if resp.StatusCode != http.StatusOK {
			reason, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("listing secrets for %s: HTTP %d: %s", label, resp.StatusCode, truncate(reason, 300))
		}
		var page struct {
			Secrets []SecretMetadata `json:"secrets"`
			Next    string           `json:"next"`
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("listing secrets for %s: %w", label, err)
		}
		secrets = append(secrets, page.Secrets...)
		if page.Next == "" {
			return secrets, nil
		}
		if page.Next == after {
			return nil, fmt.Errorf("listing secrets for %s: server repeated page token", label)
		}
		after = page.Next
	}
}

// DeleteSecret deletes one secret in the calling service's namespace.
func (c *Client) DeleteSecret(ctx context.Context, name string, expectedRevision *uint64) error {
	return c.deleteSecret(ctx, "/secrets/"+escapeSecretPath(name), name, expectedRevision)
}

// DeleteSecretFor deletes one service's secret through the admin door.
func (c *Client) DeleteSecretFor(ctx context.Context, owner, name string, expectedRevision *uint64) error {
	return c.deleteSecret(ctx, "/secrets/"+url.PathEscape(owner)+"/"+escapeSecretPath(name), owner+"/"+name, expectedRevision)
}

func (c *Client) deleteSecret(ctx context.Context, endpoint, label string, expectedRevision *uint64) error {
	if expectedRevision != nil {
		endpoint += "?expected_revision=" + strconv.FormatUint(*expectedRevision, 10)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.BaseURL+endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return fmt.Errorf("deleting secret %s: %w", label, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deleting secret %s: HTTP %d: %s", label, resp.StatusCode, truncate(reason, 300))
	}
	return nil
}

func escapeSecretPath(name string) string {
	segments := strings.Split(name, "/")
	for i := range segments {
		segments[i] = url.PathEscape(segments[i])
	}
	return strings.Join(segments, "/")
}

// Ack moves a cursor to offset. Monotonic: acking backwards reports false and
// changes nothing, so a replaying consumer cannot rewind its own progress.
func (c *Client) Ack(ctx context.Context, stream, cursor string, offset int64) (bool, error) {
	body, err := json.Marshal(map[string]any{"cursor": cursor, "stream": stream, "offset": offset})
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/ack", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return false, fmt.Errorf("acking %s@%d: %w", cursor, offset, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("acking %s@%d: HTTP %d: %s", cursor, offset, resp.StatusCode,
			truncate(reason, 300))
	}
	var out struct {
		Moved bool `json:"moved"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, fmt.Errorf("acking %s@%d: %w", cursor, offset, err)
	}
	return out.Moved, nil
}

// CursorDelete retires a cursor instead of moving it. It is what a consumer
// that mints a fresh cursor name whenever it rebuilds (colca cursors only
// move forward, so re-reading a stream needs a new name) uses to retire the
// generation it is replacing — otherwise the old cursor lingers forever and
// holds back retention pruning for every node that ever read it. Deleting an
// absent or already-deleted cursor is a no-op, so a caller may call this
// unconditionally during cleanup without first checking whether the cursor
// still exists.
func (c *Client) CursorDelete(ctx context.Context, stream, cursor string) error {
	body, err := json.Marshal(map[string]any{"cursor": cursor, "stream": stream, "delete": true})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/ack", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return fmt.Errorf("deleting cursor %s@%s: %w", cursor, stream, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deleting cursor %s@%s: HTTP %d: %s", cursor, stream, resp.StatusCode,
			truncate(reason, 300))
	}
	return nil
}

// Publish posts one record through the node's publish door.
func (c *Client) Publish(ctx context.Context, topic string, payload any) error {
	body, err := json.Marshal(map[string]any{"topic": topic, "payload": payload})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/publish", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return fmt.Errorf("publishing %s: %w", topic, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		reason, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("publishing %s: HTTP %d: %s", topic, resp.StatusCode, truncate(reason, 300))
	}
	return nil
}

// ULID asks the node who it is, so a service never has to be told.
func (c *Client) ULID(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/healthz", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.do(req)
	if err != nil {
		return "", fmt.Errorf("reading %s/healthz: %w", c.BaseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("reading %s/healthz: HTTP %d", c.BaseURL, resp.StatusCode)
	}
	var payload struct {
		ULID string `json:"ulid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", fmt.Errorf("reading %s/healthz: %w", c.BaseURL, err)
	}
	if payload.ULID == "" {
		return "", fmt.Errorf("reading %s/healthz: no ulid in the response", c.BaseURL)
	}
	return payload.ULID, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

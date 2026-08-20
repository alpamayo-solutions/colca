// Package grantsync carries authorization between the two stores that own it:
// Keycloak, where an administrator authors grants at runtime, and the colca
// tree, where nodes resolve them offline.
//
// It lives in colca's module but never inside colcad. Keycloak must stay out of
// the node core and out of the message path, and a separate process is what
// keeps that true. What being here buys is the GRAMMAR: this package validates
// every grant with uns.ParseGrant and writes the same _Group records the nodes
// read, so there is no second implementation of authorization to drift.
//
// Stateless by construction — both stores are durable and every cycle reads
// them whole — so the service owns no database, two instances racing produce
// the same writes, and a crash mid-cycle is just a cycle that gets redone.
package grantsync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/alpamayo-solutions/colca/internal/door"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	elementContract = "_SystemElement"
	groupContract   = "_Group"
)

// KVEntry is one row of a node's /kv projection.
type KVEntry struct {
	Path    string          `json:"path"`
	NodeID  string          `json:"node_id"`
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

// HeldGroup is a _Group definition the tree already holds, with the node that
// authored it. The author is the load-bearing part: the service converges only
// over definitions it wrote itself, because records are keyed by (path, author)
// and a tombstone written here would not remove another node's record anyway.
type HeldGroup struct {
	ID     string
	Grants []string
	Author string
}

// TreeView is what the root node currently holds, in the two shapes this
// service needs: which elements exist (so they can be registered as authz
// resources) and which groups are already defined (so it knows what to change).
type TreeView struct {
	Elements map[string]string // element id → path at the root
	Groups   map[string]HeldGroup
}

// element is the part of a _SystemElement record this needs.
type element struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// group is the part of a _Group record this needs. It mirrors uns's own struct
// deliberately: what this package writes is what a node reads.
type group struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Grants []string `json:"grants"`
}

// ParseKV folds a /kv read into the view.
//
// The root is the right node to ask because it holds the whole tree: records
// replicate upward with the mount inserted at each hop, so at the root every
// element already carries its full path — which is exactly the display name an
// administrator needs to recognise it by.
func ParseKV(entries []KVEntry) TreeView {
	view := TreeView{Elements: map[string]string{}, Groups: map[string]HeldGroup{}}
	for _, entry := range entries {
		p, err := uns.Parse(entry.Topic)
		if err != nil {
			continue
		}
		switch p.Contract {
		case elementContract:
			// An empty payload is a tombstone: the position is retired, and a
			// retired element must not keep a resource somebody can grant on.
			var e element
			if json.Unmarshal(entry.Payload, &e) != nil || e.ID == "" {
				continue
			}
			view.Elements[e.ID] = p.Path
		case groupContract:
			var g group
			if json.Unmarshal(entry.Payload, &g) != nil || g.ID == "" {
				continue
			}
			author := entry.NodeID
			if author == "" {
				author = p.NodeID
			}
			view.Groups[g.ID] = HeldGroup{ID: g.ID, Grants: g.Grants, Author: author}
		}
	}
	return view
}

// NodeClient talks to one colca node.
//
// Tree is grantsync's own — it parses the KV projection into a TreeView. Every
// generic call (publish, ulid) comes from the shared door client, so there is
// one implementation of the node's HTTP contract rather than a copy per
// service.
type NodeClient struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func (c *NodeClient) door() *door.Client {
	return &door.Client{BaseURL: c.BaseURL, Token: c.Token, HTTP: c.HTTP}
}

func (c *NodeClient) client() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// Tree reads the node's KV projection.
//
// Every non-200 is an error, and callers must treat it as one: a read that did
// not succeed must never be mistaken for a tree with nothing in it.
func (c *NodeClient) Tree(ctx context.Context) (TreeView, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/kv", nil)
	if err != nil {
		return TreeView{}, err
	}
	req.Header.Set("X-Colca-Token", c.Token)
	resp, err := c.client().Do(req)
	if err != nil {
		return TreeView{}, fmt.Errorf("reading %s/kv: %w", c.BaseURL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return TreeView{}, fmt.Errorf("reading %s/kv: %w", c.BaseURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return TreeView{}, fmt.Errorf("reading %s/kv: HTTP %d: %s",
			c.BaseURL, resp.StatusCode, truncate(body, 300))
	}
	var payload struct {
		Entries []KVEntry `json:"entries"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return TreeView{}, fmt.Errorf("reading %s/kv: %w", c.BaseURL, err)
	}
	return ParseKV(payload.Entries), nil
}

// ULID asks the node who it is, so the service does not have to be told.
func (c *NodeClient) ULID(ctx context.Context) (string, error) {
	return c.door().ULID(ctx)
}

// Publish posts one record through the node's admin publish door — the same
// door a human in a UI would use.
func (c *NodeClient) Publish(ctx context.Context, topic string, payload any) error {
	return c.door().Publish(ctx, topic, payload)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

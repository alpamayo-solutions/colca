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
	"net/http"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	elementContract = "_SystemElement"
	groupContract   = "_Group"
)

// treeContracts is every contract ParseKV consumes, and therefore everything
// Tree asks the node for. The filter is not an optimisation: a node's KV also
// holds every retained metric, catalogue and Edit operation it has ever
// seen, and the door pages that listing. Asking for the whole projection and
// reading one page of it is how a hub with a few thousand entries once showed
// this service eight of its thirty-one elements — and it retired the rest.
var treeContracts = []string{elementContract, groupContract}

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
//
// A TreeView only ever comes from a COMPLETE listing. Tree returns one after
// the door's paging has run to its end and not before; a read that fails on
// any page yields an error and no view at all. That is the evidence the
// resource plan stands on when it retires an element: absent from a listing
// that was read whole, not merely absent from the part of it that arrived.
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
func ParseKV(entries []door.KVEntry) TreeView {
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
// Every call goes through the shared door client, so there is one
// implementation of the node's HTTP contract — headers, status rule, paging —
// rather than a copy per service. Tree adds only the parse into a TreeView.
type NodeClient struct {
	BaseURL string
	Token   string
	Service string
	HTTP    *http.Client
}

func (c *NodeClient) door() *door.Client {
	return &door.Client{
		BaseURL: c.BaseURL, Token: c.Token, Service: c.Service, HTTP: c.HTTP,
	}
}

// Tree reads the node's KV projection — only the contracts this service
// consumes, and every page of them.
//
// Every failure is an error, and callers must treat it as one: a read that did
// not succeed, or did not finish, must never be mistaken for a tree with less
// in it. The door client enforces the second half — it returns entries only
// once the listing's last page has answered with an empty `next`.
func (c *NodeClient) Tree(ctx context.Context) (TreeView, error) {
	entries, err := c.door().KV(ctx, "", treeContracts...)
	if err != nil {
		return TreeView{}, fmt.Errorf("reading %s/kv: %w", c.BaseURL, err)
	}
	return ParseKV(entries), nil
}

// ULID asks the node who it is, so the service does not have to be told.
func (c *NodeClient) ULID(ctx context.Context) (string, error) {
	return c.door().ULID(ctx)
}

// Publish posts one record through the configured node door.
func (c *NodeClient) Publish(ctx context.Context, topic string, payload any) error {
	return c.door().Publish(ctx, topic, payload)
}

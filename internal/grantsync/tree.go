// Package grantsync keeps authorization in step between Keycloak, where
// administrators author grants, and the Colca tree, where nodes resolve them
// offline.
//
// It runs as its own process so Keycloak stays out of the node, validates
// grants with uns.ParseGrant and writes the same _Group records nodes read. It
// keeps no state: both stores are read whole every cycle, so two instances
// write the same thing and a crashed cycle is simply redone.
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

// treeContracts are the contracts ParseKV reads. Asking only for these matters:
// the full KV listing is large and paged, and reading only part of it would look
// like elements had disappeared.
var treeContracts = []string{elementContract, groupContract}

// HeldGroup is a _Group definition the tree holds, with its author. The service
// only converges definitions it wrote itself.
type HeldGroup struct {
	ID     string
	Grants []string
	Author string
}

// TreeView is what the root node holds: the elements, registered as authz
// resources, and the groups already defined. It only ever comes from a complete
// listing, which is what makes retiring an absent element safe.
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

// ParseKV folds a /kv read into the view. The root holds the whole tree with
// full paths, which are the names administrators recognise elements by.
func ParseKV(entries []door.KVEntry) TreeView {
	view := TreeView{Elements: map[string]string{}, Groups: map[string]HeldGroup{}}
	for _, entry := range entries {
		p, err := uns.Parse(entry.Topic)
		if err != nil {
			continue
		}
		switch p.Contract {
		case elementContract:
			// An empty payload is a tombstone: a retired element must not keep a grantable
			// resource.
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

// NodeClient talks to one Colca node through the shared door client.
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

// Tree reads the node's KV projection for the contracts this service uses, every
// page of it. Any failure is an error: an incomplete read must never look like a
// smaller tree.
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

package uns

import (
	"encoding/json"
	"fmt"

	"strings"
)

// ConfigExec answers `_CmdConfigure`: editing the node's data model.
//
// The node executes these because a human may command but may not author state
// (human-authz §5.2) — so every path that creates a binding, whether a person in
// a UI, a preprovisioned model file, or autobind, arrives here as the same
// command and produces the same records. That is what makes "define it once"
// true by construction rather than by discipline.
type ConfigExec struct {
	store EntityStore
	// autobindNew binds a connector's catalogue the first time the node sees
	// one, without waiting for anyone to ask (settings key
	// "autobind" = "on_new_connector").
	autobindNew bool
}

// NewConfigExec builds the executor. settings is the node's opaque plugin bag;
// unknown keys are ignored, so an operator's typo disables a feature rather
// than stopping a node.
func NewConfigExec(s EntityStore, settings map[string]string) *ConfigExec {
	return &ConfigExec{store: s, autobindNew: settings["autobind"] == "on_new_connector"}
}

func (c *ConfigExec) Handles(contract string) bool { return contract == "_CmdConfigure" }

// Observe reacts to a record the node just persisted.
//
// The only reaction is the lifecycle trigger: a connector's catalogue arriving
// with nothing bound to it yet gets bound, running the exact code the
// `signal/autobind` verb runs. Autobind is idempotent by invariant, so this
// path needs no coordination with the people and commands that may also invoke
// it — the second caller simply finds nothing left to do.
func (c *ConfigExec) Observe(contract, topic string, payload []byte) {
	if !c.autobindNew || contract != "_DataTags" {
		return
	}
	p, err := Parse(topic)
	if err != nil || len(payload) == 0 {
		return // unparseable, or the catalogue was retired
	}
	if len(c.boundTags(p.NodeID)) > 0 {
		return // already bound: this is a republish, not a new connector
	}
	c.autobind(mustBody(autobindBody{Connector: p.NodeID}))
}

func mustBody(v autobindBody) []byte {
	b, _ := json.Marshal(v)
	return b
}

// signalRef is one record to write: where it goes, and what goes there.
type signalRef struct {
	Path   string          `json:"path"`
	Signal json.RawMessage `json:"signal"`
}

type upsertBody struct {
	Signals []signalRef `json:"signals"`
}

type deleteBody struct {
	Paths []string `json:"paths"`
}

type autobindBody struct {
	Connector string `json:"connector"`
	Under     string `json:"under"`
}

// catalogue is the part of a connector's _DataTags record this needs.
type catalogue struct {
	Connector string `json:"connector"`
	DataTags  []struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		DataType string `json:"data_type"`
	} `json:"data_tags"`
}

// boundSignal is the part of a _Signal record that identifies its binding.
type boundSignal struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Connector string `json:"connector"`
	TagID     string `json:"tag_id"`
}

func (c *ConfigExec) Execute(contract, verb string, payload []byte) (int, string, string) {
	switch verb {
	case "signal/upsert":
		return c.upsert(payload)
	case "signal/delete":
		return c.delete(payload)
	case "signal/autobind":
		return c.autobind(payload)
	default:
		return 422, fmt.Sprintf("unknown configure verb %q", verb), "invalid"
	}
}

func (c *ConfigExec) upsert(payload []byte) (int, string, string) {
	var body upsertBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/upsert: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Signals) == 0 {
		return 422, "signal/upsert: no signals given", "invalid"
	}
	for i, ref := range body.Signals {
		if ref.Path == "" {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no path", i), "invalid"
		}
		if len(ref.Signal) == 0 {
			return 422, fmt.Sprintf("signal/upsert: entry %d has no signal", i), "invalid"
		}
		if err := c.store.Publish(c.signalTopic(ref.Path), ref.Signal); err != nil {
			// The bundle rejected it, or the store did. Either way the caller
			// learns which entry and why rather than a bare failure.
			return 422, fmt.Sprintf("signal/upsert: %s rejected: %v", ref.Path, err), "invalid"
		}
	}
	return 200, fmt.Sprintf("upserted %d", len(body.Signals)), "ok"
}

func (c *ConfigExec) delete(payload []byte) (int, string, string) {
	var body deleteBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/delete: unreadable payload: " + err.Error(), "invalid"
	}
	if len(body.Paths) == 0 {
		return 422, "signal/delete: no paths given", "invalid"
	}
	var missing []string
	for _, path := range body.Paths {
		topic := c.signalTopic(path)
		if _, ok := c.store.KVGet(topic); !ok {
			missing = append(missing, path)
			continue
		}
		// An empty payload is the tombstone: the path is retired, not blanked.
		if err := c.store.Publish(topic, nil); err != nil {
			return 500, fmt.Sprintf("signal/delete: %s failed: %v", path, err), "error"
		}
	}
	if len(missing) > 0 {
		return 404, "signal/delete: no signal at " + strings.Join(missing, ", "), "invalid"
	}
	return 200, fmt.Sprintf("deleted %d", len(body.Paths)), "ok"
}

// autobind creates one signal per unbound tag of a connector's catalogue.
//
// Idempotent by invariant: a tag that already has a binding is skipped and an
// existing binding is never overwritten, so re-running changes nothing. That is
// what lets the same verb be issued by a person, replayed from the commands
// stream after an offline period, or fired by a node lifecycle trigger, without
// any of those paths needing to know about the others.
func (c *ConfigExec) autobind(payload []byte) (int, string, string) {
	var body autobindBody
	if err := json.Unmarshal(payload, &body); err != nil {
		return 422, "signal/autobind: unreadable payload: " + err.Error(), "invalid"
	}
	if body.Connector == "" {
		return 422, "signal/autobind: no connector given", "invalid"
	}
	under := body.Under
	if under == "" {
		under = body.Connector
	}

	records := c.store.KVScan("_DataTags", body.Connector)
	if len(records) == 0 {
		// Nothing to bind against yet — the connector has not published its
		// catalogue. A retry after it does will succeed, so this is a conflict
		// with the current state, not a bad request.
		return 409, "signal/autobind: " + body.Connector + " has published no catalogue", "conflict"
	}
	var cat catalogue
	if err := json.Unmarshal(records[0].Payload, &cat); err != nil {
		return 422, "signal/autobind: unreadable catalogue: " + err.Error(), "invalid"
	}

	bound := c.boundTags(body.Connector)
	taken := c.takenPaths()

	created, skipped := 0, 0
	for _, tag := range cat.DataTags {
		if bound[tag.ID] {
			skipped++
			continue
		}
		leaf := uniquePath(sanitize(tag.Name), under, taken)
		path := under + "/" + leaf
		signal := map[string]any{
			"id":           body.Connector + ":" + tag.ID,
			"name":         leaf,
			"connector":    body.Connector,
			"tag_id":       tag.ID,
			"is_published": true,
			"metadata":     map[string]any{"tag_name": tag.Name},
		}
		if tag.DataType != "" {
			signal["data_type"] = tag.DataType
		}
		encoded, err := json.Marshal(signal)
		if err != nil {
			return 500, "signal/autobind: encode failed: " + err.Error(), "error"
		}
		if err := c.store.Publish(c.signalTopic(path), encoded); err != nil {
			return 422, fmt.Sprintf("signal/autobind: %s rejected: %v", path, err), "invalid"
		}
		taken[path] = true
		created++
	}
	return 200, fmt.Sprintf(`{"created":%d,"skipped":%d}`, created, skipped), "ok"
}

func (c *ConfigExec) signalTopic(path string) string {
	return "colca/v1/_Signal/" + c.store.NodeID() + "/" + path
}

// boundTags is the set of tag ids of one connector that already have a signal.
func (c *ConfigExec) boundTags(connector string) map[string]bool {
	bound := map[string]bool{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		var s boundSignal
		if json.Unmarshal(rec.Payload, &s) != nil {
			continue
		}
		if s.Connector == connector && s.TagID != "" {
			bound[s.TagID] = true
		}
	}
	return bound
}

func (c *ConfigExec) takenPaths() map[string]bool {
	taken := map[string]bool{}
	for _, rec := range c.store.KVScan("_Signal", c.store.NodeID()) {
		taken[rec.Path] = true
	}
	return taken
}

// sanitize turns a tag name into one topic segment. MQTT separators and
// wildcards cannot survive in a path, and a name that sanitizes to nothing
// still needs an addressable place to live.
func sanitize(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r == '/' || r == '+' || r == '#' || r == ' ' || r < 0x20:
			b.WriteByte('_')
		default:
			b.WriteRune(r)
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "tag"
	}
	return out
}

// uniquePath suffixes until the path is free. Two tags can sanitize to the same
// segment (or repeat across branches), and silently dropping one would lose data
// no one asked to lose.
func uniquePath(leaf, under string, taken map[string]bool) string {
	if !taken[under+"/"+leaf] {
		return leaf
	}
	for n := 2; ; n++ {
		candidate := fmt.Sprintf("%s-%d", leaf, n)
		if !taken[under+"/"+candidate] {
			return candidate
		}
	}
}

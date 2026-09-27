// Package clockwork coordinates HTTP consumers of bounded application time.
// It shares the broker's ClockDefinition projection and existing service
// metadata. It never changes OS time or creates another time authority.
package clockwork

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Door is the existing local HTTP door, not a separate transport.
type Door interface {
	Self(context.Context) (door.Self, error)
	KV(context.Context, string, ...string) ([]door.KVEntry, error)
	Publish(context.Context, string, any) error
}

// Gate waits for upstream commits before draining and acknowledging a window.
// Callers must make Drain idempotent and durable before it returns true. After
// a restart it runs again; the consumer's existing durable cursor owns replay.
// A Gate is used by one worker goroutine only.
type Gate struct {
	Fresh        func(string) bool // Live heartbeat lease owned by the subscription.
	Asynchronous bool
	// State supplies the live subscription; coordinated consumers require it.
	State func(context.Context) ([]door.KVEntry, error)
	// ReportDetails lets the service health announcer own the full record.
	ReportDetails func(context.Context, map[string]any) error
	definition    *uns.ClockDefinition
	Door          Door
	Topic         string
	Dependencies  []string
	Name          string
	Metadata      map[string]any
	Details       map[string]any
	Drain         func(context.Context, float64) (bool, error)
	self          *door.Self
	run           string
	completed     *float64
	lastReport    time.Time
}

// Dependencies parses the shared deployment setting. An empty string disables
// coordination; [] explicitly enables a worker without upstream dependencies.
func Dependencies(raw, topic string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[1] != "v1" || parts[2] != "_ClockDefinition" || strings.ContainsAny(topic, "+# \t\n") {
		return nil, fmt.Errorf("coordinated execution requires an exact FACTORY_CLOCK_TOPIC")
	}
	var deps []string
	if err := json.Unmarshal([]byte(raw), &deps); err != nil || deps == nil {
		return nil, fmt.Errorf("FACTORY_STEP_DEPENDENCIES must be a JSON array")
	}
	for _, dep := range deps {
		if strings.HasPrefix(dep, "./") && len(dep) > 2 && !strings.ContainsAny(dep[2:], "/+# \t\n") {
			continue
		}
		p := strings.Split(dep, "/")
		if len(p) < 5 || p[2] != "_ServiceDetails" || strings.ContainsAny(dep, "+# \t\n") {
			return nil, fmt.Errorf("invalid dependency %q", dep)
		}
	}
	return deps, nil
}

// Register announces a waiting consumer before a clock definition exists.
// Deployment tools can discover its exact placed topic without guessing paths.
func (g *Gate) Register(ctx context.Context) error {
	self, err := g.Door.Self(ctx)
	if err != nil {
		return err
	}
	g.self = &self
	return g.report(ctx, 0)
}

// Once checks a window, drains downstream effects, then publishes completion.
// realNow must come from the configured real-time authority (host or the
// broker's latest fetch response). Zero/unavailable authority holds the run.
func (g *Gate) Once(ctx context.Context, realNow float64) (bool, error) {
	if realNow <= 0 || math.IsNaN(realNow) || math.IsInf(realNow, 0) {
		return false, nil
	}
	if g.self == nil {
		self, err := g.Door.Self(ctx)
		if err != nil {
			return false, err
		}
		g.self = &self
	}
	entries, err := g.state(ctx)
	if err != nil {
		return false, err
	}
	var definition *uns.ClockDefinition
	for _, entry := range entries {
		if entry.Topic == g.Topic {
			d, err := uns.DecodeClockDefinition(entry.Payload)
			if err != nil {
				return false, err
			}
			definition = &d
		}
	}
	if definition == nil {
		return false, nil
	}
	if g.run != "" && definition.RunID != g.run {
		return false, fmt.Errorf("clock run changed; use a fresh deployment")
	}
	g.run = definition.RunID
	g.definition = definition
	if g.completed != nil && definition.At(realNow) < *g.completed {
		return false, fmt.Errorf("clock would rewind completed work")
	}
	if g.completed != nil && time.Since(g.lastReport) >= 5*time.Second {
		if err := g.report(ctx, realNow); err != nil {
			return false, err
		}
	}
	if definition.StopAt == nil {
		return false, nil
	}
	target := *definition.StopAt
	if g.Asynchronous && len(g.Dependencies) > 0 {
		for _, dep := range g.Dependencies {
			found := false
			for _, entry := range entries {
				var row struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(entry.Payload, &row) != nil {
					continue
				}
				matches := dep == entry.Topic || (strings.HasPrefix(dep, "./") && row.Name == dep[2:] && strings.HasPrefix(entry.Topic, uns.Prefix()+"_ServiceDetails/"+g.self.Node+"/"))
				if !matches {
					continue
				}
				topic := strings.Replace(entry.Topic, "/_ServiceDetails/", "/_ClockProgress/", 1)
				for _, marker := range entries {
					if marker.Topic != topic {
						continue
					}
					var progress struct {
						Run       string  `json:"run_id"`
						Processed float64 `json:"processed_at"`
					}
					if json.Unmarshal(marker.Payload, &progress) == nil && progress.Run == g.run {
						target = min(target, progress.Processed)
						found = true
					}
				}
			}
			if !found {
				return false, nil
			}
		}
	}
	if definition.At(realNow) < target {
		return false, nil
	}
	if g.completed != nil && *g.completed >= target {
		return false, nil
	}
	for _, dep := range g.Dependencies {
		found := false
		for _, entry := range entries {
			var row struct {
				Name     string `json:"name"`
				Active   bool   `json:"is_active"`
				Metadata struct {
					Clock struct {
						Run       string   `json:"run_id"`
						Ready     bool     `json:"ready"`
						Observed  float64  `json:"observed_at"`
						Processed *float64 `json:"processed_at"`
					} `json:"application_clock"`
				} `json:"metadata"`
			}
			if json.Unmarshal(entry.Payload, &row) != nil {
				continue
			}
			matches := dep == entry.Topic || (strings.HasPrefix(dep, "./") && row.Name == dep[2:] && strings.HasPrefix(entry.Topic, uns.Prefix()+"_ServiceDetails/"+g.self.Node+"/"))
			p := row.Metadata.Clock
			barrierReady := false
			if matches {
				barrierTopic := strings.Replace(entry.Topic, "/_ServiceDetails/", "/_ClockProgress/", 1)
				for _, barrier := range entries {
					if barrier.Topic != barrierTopic {
						continue
					}
					var progress struct {
						Run       string  `json:"run_id"`
						Processed float64 `json:"processed_at"`
					}
					if json.Unmarshal(barrier.Payload, &progress) == nil && progress.Run == g.run && progress.Processed >= target {
						barrierReady = true
					}
				}
			}
			if matches && barrierReady && row.Active && p.Ready && p.Run == g.run && g.Fresh != nil && g.Fresh(entry.Topic) {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	complete, err := g.Drain(ctx, target)
	if err != nil || !complete {
		return false, err
	}
	g.completed = &target
	if err := g.report(ctx, realNow); err != nil {
		// Replay the idempotent drain if publishing the acknowledgement fails.
		g.completed = nil
		return false, err
	}
	return true, nil
}

func (g *Gate) report(ctx context.Context, realNow float64) error {
	metadata := map[string]any{}
	for k, v := range g.Metadata {
		metadata[k] = v
	}
	if g.completed != nil {
		metadata["application_clock"] = map[string]any{"ready": true, "run_id": g.run, "processed_at": g.completed, "observed_at": realNow}
	}
	hierarchy := uns.ServiceContext(g.self.Mount, g.Name)
	payload := map[string]any{"id": g.self.ULID, "name": g.Name, "service_type": "other", "display_name": g.Name, "colca_node_id": g.self.Node, "hierarchy": hierarchy, "is_active": true, "metadata": metadata}
	for key, value := range g.Details {
		if key != "metadata" {
			payload[key] = value
		}
	}
	topic := uns.Prefix() + "_ServiceDetails/" + g.self.Node + "/" + strings.Join(hierarchy, "/") + "/_service"
	// Ordered before service liveness. Repeating the marker is idempotent;
	// it also repairs delivery after a process or MQTT-session restart.
	if g.completed != nil {
		marker := map[string]any{"run_id": g.run, "processed_at": g.completed}
		if err := g.Door.Publish(ctx, strings.Replace(topic, "/_ServiceDetails/", "/_ClockProgress/", 1), marker); err != nil {
			return err
		}
	}
	// Full service records drive database projections; completion has its own
	// ordered marker above. Keep details at a real-time heartbeat cadence.
	if time.Since(g.lastReport) < 5*time.Second {
		return nil
	}
	if g.ReportDetails != nil {
		if err := g.ReportDetails(ctx, metadata); err != nil {
			return err
		}
	} else if err := g.Door.Publish(ctx, topic, payload); err != nil {
		return err
	}
	g.lastReport = time.Now()
	return nil
}

// State must be supplied by the shared subscription.
func (g *Gate) state(ctx context.Context) ([]door.KVEntry, error) {
	if g.State == nil {
		return nil, fmt.Errorf("clock coordination requires a subscription")
	}
	return g.State(ctx)
}

// WaitDelay schedules real deadlines; no per-window polling interval is used.
// Five seconds is the service health heartbeat, including while paused.
func (g *Gate) WaitDelay(realNow float64) time.Duration {
	delay := 5.0
	d := g.definition
	if realNow > 0 && d != nil && d.StopAt != nil && d.At(realNow) < *d.StopAt {
		if d.RealAnchor > realNow {
			delay = math.Min(delay, d.RealAnchor-realNow)
		}
		if d.Rate > 0 {
			due := d.RealAnchor + (*d.StopAt-d.FactoryAnchor)/d.Rate
			if d.CatchUp {
				due = math.Max(due, *d.StopAt)
			}
			if due > realNow {
				delay = math.Min(delay, due-realNow)
			}
		}
	}
	return time.Duration(delay * float64(time.Second))
}

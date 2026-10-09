// Package clockwork coordinates HTTP consumers of bounded application time.
// It shares the broker's ClockDefinition projection and existing service
// discovery and ordered progress control. It never changes OS time.
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
	// Telemetry observes runtime progress without publishing registration.
	Telemetry    func(map[string]any)
	definition   *uns.ClockDefinition
	Door         Door
	Topic        string
	Dependencies []string
	Name         string
	Metadata     map[string]any
	Details      map[string]any
	Drain        func(context.Context, float64) (bool, error)
	self         *door.Self
	run          string
	completed    *float64
	lastReport   time.Time
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
	return g.register(ctx)
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
				Name   string `json:"name"`
				Active bool   `json:"is_active"`
			}
			if json.Unmarshal(entry.Payload, &row) != nil {
				continue
			}
			matches := dep == entry.Topic || (strings.HasPrefix(dep, "./") && row.Name == dep[2:] && strings.HasPrefix(entry.Topic, uns.Prefix()+"_ServiceDetails/"+g.self.Node+"/"))
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
						Ready     bool    `json:"ready"`
					}
					if json.Unmarshal(barrier.Payload, &progress) == nil && progress.Ready && progress.Run == g.run && progress.Processed >= target && g.Fresh != nil && g.Fresh(barrierTopic) {
						barrierReady = true
					}
				}
			}
			if matches && barrierReady && row.Active {
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

func (g *Gate) serviceTopic() string {
	hierarchy := uns.ServiceContext(g.self.Mount, g.Name)
	return uns.Prefix() + "_ServiceDetails/" + g.self.Node + "/" + strings.Join(hierarchy, "/") + "/_service"
}

func (g *Gate) register(ctx context.Context) error {
	metadata := map[string]any{}
	for k, v := range g.Metadata {
		metadata[k] = v
	}
	payload := map[string]any{"id": g.self.ULID, "name": g.Name, "service_type": "other", "display_name": g.Name, "colca_node_id": g.self.Node, "hierarchy": uns.ServiceContext(g.self.Mount, g.Name), "is_active": true, "metadata": metadata}
	for key, value := range g.Details {
		if key != "metadata" {
			payload[key] = value
		}
	}
	return g.Door.Publish(ctx, g.serviceTopic(), payload)
}

func (g *Gate) report(ctx context.Context, realNow float64) error {
	if g.completed == nil {
		return nil
	}
	var observed any
	if realNow > 0 {
		observed = realNow
	}
	marker := map[string]any{"run_id": g.run, "processed_at": *g.completed, "ready": realNow > 0, "observed_at": observed}
	if g.Telemetry != nil {
		g.Telemetry(marker)
	}
	// Functional readiness remains ordered behind samples and priority events.
	if err := g.Door.Publish(ctx, strings.Replace(g.serviceTopic(), "/_ServiceDetails/", "/_ClockProgress/", 1), marker); err != nil {
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

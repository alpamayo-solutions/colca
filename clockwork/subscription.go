package clockwork

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/plugins/uns"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Subscription supplies a Gate with pushed retained state. The existing service
// MQTT connection owns it; durable work still uses the HTTP fetch/ack door.
type heartbeatReceipt struct {
	observed float64
	received time.Time
}

type Subscription struct {
	Now          func() time.Time
	heartbeats   map[string]heartbeatReceipt
	Node, Topic  string
	Dependencies []string
	Notify       func()
	mu           sync.Mutex
	rows         map[string]door.KVEntry
	ignored      map[string]bool
	changed      chan struct{}
	epoch        float64
	received     time.Time
}

func (s *Subscription) signal() {
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
	if s.Notify != nil {
		s.Notify()
	}
}

// Reset discards pre-disconnect state; retained records must arrive again.
func (s *Subscription) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = make(map[string]door.KVEntry)
	s.heartbeats = make(map[string]heartbeatReceipt)
	s.ignored = make(map[string]bool)
	s.epoch, s.received = 0, time.Time{}
	s.signal()
}

// Changes must be captured BEFORE checking state, so a racing commit is not lost.
func (s *Subscription) Changes() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}

func (s *Subscription) State(context.Context) ([]door.KVEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([]door.KVEntry, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, row)
	}
	return rows, nil
}

func (s *Subscription) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Fresh uses local elapsed time, independent of clock skew and factory speed.
// Retained state and duplicate delivery cannot renew a heartbeat's lease.
func (s *Subscription) Fresh(topic string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.heartbeats[topic]
	age := s.now().Sub(h.received)
	return ok && age >= 0 && age <= 15*time.Second
}

func (s *Subscription) heartbeat(topic string, payload []byte, retained bool) bool {
	if !strings.Contains(topic, "/_ServiceDetails/") {
		return false
	}
	if len(payload) == 0 {
		delete(s.heartbeats, topic)
		return false
	}
	if retained {
		return false
	}
	var row struct {
		Metadata struct {
			Clock struct {
				Observed *float64 `json:"observed_at"`
			} `json:"application_clock"`
		} `json:"metadata"`
	}
	if json.Unmarshal(payload, &row) != nil || row.Metadata.Clock.Observed == nil {
		return false
	}
	observed := *row.Metadata.Clock.Observed
	if math.IsNaN(observed) || math.IsInf(observed, 0) {
		return false
	}
	if s.heartbeats == nil {
		s.heartbeats = make(map[string]heartbeatReceipt)
	}
	prior, ok := s.heartbeats[topic]
	if ok && prior.observed == observed {
		return false
	}
	s.heartbeats[topic] = heartbeatReceipt{observed: observed, received: s.now()}
	return true
}

// RealNow uses either host time (optionally NTP) OR live _TimeSync beacons.
func (s *Subscription) RealNow(source string) float64 {
	if source == "local" {
		return float64(time.Now().UnixMicro()) / 1e6
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.received.IsZero() || time.Since(s.received) > 120*time.Second {
		return 0
	}
	return s.epoch + time.Since(s.received).Seconds()
}

func (s *Subscription) observe(topic string, payload []byte, retained bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if topic == uns.Prefix()+"_TimeSync/"+s.Node {
		var beacon struct {
			NowMS float64 `json:"now_ms"`
		}
		if retained || json.Unmarshal(payload, &beacon) != nil || beacon.NowMS <= 0 || math.IsNaN(beacon.NowMS) || math.IsInf(beacon.NowMS, 0) {
			return
		}
		s.epoch, s.received = beacon.NowMS/1000, time.Now()
	} else {
		if s.rows == nil {
			s.rows = make(map[string]door.KVEntry)
		}
		if s.ignored == nil {
			s.ignored = make(map[string]bool)
		}
		relevant := topic == s.Topic
		for _, dep := range s.Dependencies {
			if topic == dep || topic == strings.Replace(dep, "/_ServiceDetails/", "/_ClockProgress/", 1) {
				relevant = true
			}
		}
		if !relevant {
			detailsTopic := strings.Replace(topic, "/_ClockProgress/", "/_ServiceDetails/", 1)
			local := strings.HasPrefix(detailsTopic, uns.Prefix()+"_ServiceDetails/"+s.Node+"/")
			hasAliases := false
			for _, dep := range s.Dependencies {
				hasAliases = hasAliases || strings.HasPrefix(dep, "./")
			}
			if !local || !hasAliases {
				return
			}
			if strings.Contains(topic, "/_ClockProgress/") {
				if s.ignored[detailsTopic] {
					return
				}
				_, relevant = s.rows[detailsTopic]
			} else {
				_, relevant = s.rows[topic] // Retiring a known dependency also wakes it.
				if len(payload) > 0 {
					var identity struct {
						Name string `json:"name"`
					}
					if json.Unmarshal(payload, &identity) != nil {
						return
					}
					relevant = false
					for _, dep := range s.Dependencies {
						relevant = relevant || dep == "./"+identity.Name
					}
				}
				if !relevant {
					s.ignored[topic] = true
					delete(s.rows, topic)
					delete(s.rows, strings.Replace(topic, "/_ServiceDetails/", "/_ClockProgress/", 1))
					return
				}
				delete(s.ignored, topic)
			}
		}
		healthChanged := s.heartbeat(topic, payload, retained)
		if prior, ok := s.rows[topic]; ok && bytes.Equal(prior.Payload, payload) && !healthChanged {
			return
		}
		if len(payload) == 0 {
			delete(s.rows, topic)
		} else {
			s.rows[topic] = door.KVEntry{Topic: topic, Payload: append(json.RawMessage(nil), payload...)}
		}
		if !relevant {
			return
		} // Retained progress may precede service identity.
	}
	s.signal()
}

// Attach is called on every connection, including reconnects. The MQTT broker
// supplies retained configuration and progress; no KV discovery loop is needed.
func (s *Subscription) Attach(client mqtt.Client) error {
	s.Reset()
	filters := map[string]byte{s.Topic: 1, uns.Prefix() + "_TimeSync/" + s.Node: 0}
	for _, dep := range s.Dependencies {
		if strings.HasPrefix(dep, "./") {
			dep = uns.Prefix() + "_ServiceDetails/" + s.Node + "/#"
		}
		filters[dep] = 1
		filters[strings.Replace(dep, "/_ServiceDetails/", "/_ClockProgress/", 1)] = 1
	}
	token := client.SubscribeMultiple(filters, func(_ mqtt.Client, message mqtt.Message) {
		s.observe(message.Topic(), message.Payload(), message.Retained())
	})
	if !token.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("clock subscriptions timed out")
	}
	return token.Error()
}

// Wait wakes on a pushed change, a scheduled/health deadline, or shutdown.
func Wait(ctx context.Context, changed <-chan struct{}, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-changed:
	case <-timer.C:
	}
}

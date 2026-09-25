package historian

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Service statuses as a health view reads them from
// architecture_metadata.status.
const (
	StatusStarting  = "starting"
	StatusHealthy   = "healthy"
	StatusUnhealthy = "unhealthy"
)

// Announcer keeps this service's retained _ServiceDetails current over the
// node's local MQTT door: is_active true while connected, and a last will that
// sets it false if the process dies without saying goodbye. The bridge itself
// only uses HTTP, which has no will, so the record needs a connection of its
// own.
type Announcer struct {
	Door    *door.Client // resolves the identity through /self
	MQTTURL string       // the local MQTT door, e.g. tcp://colca:1883
	Version string       // the release, announced as metadata.version; empty announces none
	Log     *slog.Logger

	// Dial replaces the network dial; tests use it to cut the connection.
	Dial func(ctx context.Context, addr string) (net.Conn, error)

	mu      sync.Mutex
	status  string
	detail  string
	details *serviceDetails // nil until the identity is resolved
	topic   string
	client  pahomqtt.Client
}

type serviceDetails struct {
	ID                   string           `json:"id"`
	Name                 string           `json:"name"`
	DisplayName          string           `json:"display_name"`
	Description          string           `json:"description"`
	ServiceType          string           `json:"service_type"`
	ColcaNodeID          string           `json:"colca_node_id"`
	SystemElementID      string           `json:"system_element_id,omitempty"`
	Hierarchy            []string         `json:"hierarchy"`
	IsActive             bool             `json:"is_active"`
	Metadata             map[string]any   `json:"metadata"`
	ArchitectureMetadata map[string]any   `json:"architecture_metadata"`
	HealthMetrics        []map[string]any `json:"health_metrics"`
}

func (a *Announcer) logger() *slog.Logger {
	if a.Log != nil {
		return a.Log
	}
	return slog.Default()
}

// Run resolves the identity, connects and keeps the record current until ctx
// ends, then marks the service inactive. It never fails the caller: history
// keeps flowing whether or not the record can be published.
func (a *Announcer) Run(ctx context.Context) {
	self, ok := a.resolve(ctx)
	if !ok {
		return
	}
	details, topic := a.record(self)
	a.mu.Lock()
	a.details, a.topic = &details, topic
	a.mu.Unlock()
	// The will is fixed at connect time: a process that dies unannounced is down.
	details.IsActive = false
	details.ArchitectureMetadata = map[string]any{"status": StatusUnhealthy}
	down, err := json.Marshal(details)
	if err != nil {
		a.logger().Error("cannot build the service record", "err", err)
		return
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker(a.MQTTURL).
		SetClientID("historian-"+self.ULID).
		SetUsername(a.Door.Service).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5*time.Second).
		SetMaxReconnectInterval(time.Minute).
		SetBinaryWill(topic, down, 1, true).
		SetOnConnectHandler(func(c pahomqtt.Client) {
			// A reconnect follows a drop, after which the will said inactive.
			a.publish(c, true)
		}).
		SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
			a.logger().Warn("local MQTT connection lost, reconnecting", "err", err)
		})
	if a.Dial != nil {
		dial := a.Dial
		opts.SetCustomOpenConnectionFn(func(uri *url.URL, _ pahomqtt.ClientOptions) (net.Conn, error) {
			return dial(ctx, uri.Host)
		})
	}

	client := pahomqtt.NewClient(opts)
	a.mu.Lock()
	a.client = client
	a.mu.Unlock()
	client.Connect()

	<-ctx.Done()
	a.mu.Lock()
	a.client = nil
	a.mu.Unlock()
	if client.IsConnectionOpen() {
		// A clean DISCONNECT does not send the will, so say it ourselves.
		a.publish(client, false)
	}
	client.Disconnect(250)
}

// Report sets the status the record carries: healthy, or unhealthy with the
// reason. Only a change of status is published; a repeat of the same status
// is not, and its detail stays the one that came with the change.
func (a *Announcer) Report(ok bool, detail string) {
	status := StatusHealthy
	if !ok {
		status = StatusUnhealthy
	} else {
		detail = ""
	}
	a.mu.Lock()
	if a.current() == status {
		a.mu.Unlock()
		return
	}
	a.status, a.detail = status, detail
	client := a.client
	a.mu.Unlock()
	if status == StatusUnhealthy {
		a.logger().Warn("reporting the historian unhealthy", "detail", detail)
	} else {
		a.logger().Info("reporting the historian healthy")
	}
	if client != nil && client.IsConnectionOpen() {
		a.publish(client, true)
	}
}

// current is the status to publish; the caller holds mu.
func (a *Announcer) current() string {
	if a.status == "" {
		return StatusStarting
	}
	return a.status
}

// publish writes the record with the current status. The payload is built and
// handed to the client under mu, so two publishes leave in the order their
// statuses were set.
func (a *Announcer) publish(c pahomqtt.Client, active bool) {
	a.mu.Lock()
	if a.details == nil {
		a.mu.Unlock()
		return
	}
	details := *a.details
	details.IsActive = active
	details.ArchitectureMetadata = map[string]any{"status": a.current()}
	if a.detail != "" {
		details.ArchitectureMetadata["detail"] = a.detail
	}
	topic := a.topic
	payload, err := json.Marshal(details)
	if err != nil {
		a.mu.Unlock()
		a.logger().Error("cannot build the service record", "err", err)
		return
	}
	tok := c.Publish(topic, 1, true, payload)
	a.mu.Unlock()
	if tok.WaitTimeout(10*time.Second) && tok.Error() != nil {
		a.logger().Warn("could not publish the service record", "topic", topic, "err", tok.Error())
	}
}

func (a *Announcer) resolve(ctx context.Context) (door.Self, bool) {
	for {
		self, err := a.Door.Self(ctx)
		if err == nil {
			return self, true
		}
		a.logger().Warn("local identity not available yet, retrying", "err", err)
		select {
		case <-ctx.Done():
			return door.Self{}, false
		case <-time.After(5 * time.Second):
		}
	}
}

// record returns the service record without its status, and its topic.
// app_class "core" marks it as the node's own service, not an app.
func (a *Announcer) record(self door.Self) (serviceDetails, string) {
	context := uns.ServiceContext(self.Mount, self.Name)
	metadata := map[string]any{"consumer": Consumer, "app_class": "core"}
	if a.Version != "" {
		metadata["version"] = a.Version
	}
	details := serviceDetails{
		ID:                   self.ULID,
		Name:                 self.Name,
		DisplayName:          "Colca historian",
		Description:          "Writes the metrics stream into TimescaleDB.",
		ServiceType:          "dataops",
		ColcaNodeID:          self.Node,
		SystemElementID:      self.Element,
		Hierarchy:            context,
		IsActive:             true,
		Metadata:             metadata,
		ArchitectureMetadata: map[string]any{},
		HealthMetrics:        []map[string]any{},
	}
	topic := fmt.Sprintf("%s_ServiceDetails/%s/%s/_service", uns.Prefix(), self.Node, strings.Join(context, "/"))
	return details, topic
}

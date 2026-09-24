package historian

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/alpamayo-solutions/colca/door"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Announcer keeps this service's retained _ServiceDetails current over the
// node's local MQTT door: is_active true while connected, and a last will that
// sets it false if the process dies without saying goodbye. The bridge itself
// only uses HTTP, which has no will, so the record needs a connection of its
// own.
type Announcer struct {
	Door    *door.Client // resolves the identity through /self
	MQTTURL string       // the local MQTT door, e.g. tcp://colca:1883
	Log     *slog.Logger

	// Dial replaces the network dial; tests use it to cut the connection.
	Dial func(ctx context.Context, addr string) (net.Conn, error)
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
	up, down, topic, err := a.records(self)
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
			tok := c.Publish(topic, 1, true, up)
			if tok.WaitTimeout(10*time.Second) && tok.Error() != nil {
				a.logger().Warn("could not publish the service record", "topic", topic, "err", tok.Error())
			}
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
	client.Connect()

	<-ctx.Done()
	if client.IsConnectionOpen() {
		// A clean DISCONNECT does not send the will, so say it ourselves.
		tok := client.Publish(topic, 1, true, down)
		tok.WaitTimeout(5 * time.Second)
	}
	client.Disconnect(250)
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

// records returns the active and inactive payloads and their topic.
func (a *Announcer) records(self door.Self) (up, down []byte, topic string, err error) {
	context := uns.ServiceContext(self.Mount, self.Name)
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
		Metadata:             map[string]any{"consumer": Consumer},
		ArchitectureMetadata: map[string]any{},
		HealthMetrics:        []map[string]any{},
	}
	if up, err = json.Marshal(details); err != nil {
		return nil, nil, "", err
	}
	details.IsActive = false
	if down, err = json.Marshal(details); err != nil {
		return nil, nil, "", err
	}
	topic = fmt.Sprintf("%s_ServiceDetails/%s/%s/_service", uns.Prefix(), self.Node, strings.Join(context, "/"))
	return up, down, topic, nil
}

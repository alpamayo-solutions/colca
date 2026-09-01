// Command colca-service publishes the `_ServiceDetails` record for every
// service on this node that cannot publish its own observed state. It uses
// Colca's deployment-local HTTP door: a service name selects a stable registry
// entry and no key, certificate, token, or enrollment step exists.
//
// ONE process for the whole node, not one per service. A generated hub ran 13
// containers whose entire job was one retained record each plus a five-minute
// tick — 13 images to pull, 13 restart policies, 13 things that can wedge, and
// one that did (a `service_type` the contract refuses, restarting forever).
// Nothing about the records changed: each is still published under its own
// local identity, to its own topic, with its own payload.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

type serviceDetails struct {
	ID                   string           `json:"id"`
	Name                 string           `json:"name"`
	DisplayName          string           `json:"display_name"`
	Description          string           `json:"description"`
	ServiceType          string           `json:"service_type"`
	ColcaNodeID         string           `json:"colca_node_id"`
	SystemElementID      string           `json:"system_element_id,omitempty"`
	Hierarchy            []string         `json:"hierarchy"`
	IsActive             bool             `json:"is_active"`
	Metadata             map[string]any   `json:"metadata"`
	ArchitectureMetadata map[string]any   `json:"architecture_metadata"`
	HealthMetrics        []map[string]any `json:"health_metrics"`
}

// registration is one service this process speaks for: where it declares it is
// mounted, and the record it publishes.
type registration struct {
	Mount   string         `json:"mount"`
	Details serviceDetails `json:"details"`
}

type localIdentity struct {
	ULID    string `json:"ulid"`
	Name    string `json:"name"`
	Node    string `json:"node"`
	Element string `json:"element"`
	Mount   string `json:"mount"`
}

// rejected marks a record the node refused on its merits — a contract
// violation in what the generator emitted, which retrying cannot fix.
type rejected struct{ err error }

func (r rejected) Error() string { return r.err.Error() }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "colca-service:", err)
		os.Exit(1)
	}
}

func run() error {
	registrations, err := parseRegistrations(os.Getenv("SERVICE_REGISTRATIONS"))
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := os.Getenv("COLCA_URL")
	if baseURL == "" {
		baseURL = "http://colca"
	}

	deadline := time.Now().Add(2 * time.Minute)
	live := make([]*publisher, 0, len(registrations))
	var refusals []string
	for _, reg := range registrations {
		p, err := start(client, baseURL, reg, deadline)
		var refusal rejected
		switch {
		case err == nil:
			live = append(live, p)
			fmt.Printf("published %s as %s\n", reg.Details.Name, p.details.ID)
		case asRejected(err, &refusal):
			// One bad record must not cost the other twelve their
			// registration, so keep going and report every refusal at the
			// end rather than dying on the first.
			refusals = append(refusals, fmt.Sprintf("%s: %v", reg.Details.Name, refusal.err))
		default:
			// The door itself is unreachable. Restarting is the right answer.
			return fmt.Errorf("%s: %w", reg.Details.Name, err)
		}
	}
	if len(refusals) > 0 {
		return fmt.Errorf(
			"the node refused %d of %d service records (the generated payload is "+
				"invalid, retrying will not help):\n  %s",
			len(refusals), len(registrations), strings.Join(refusals, "\n  "))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, p := range live {
				if err := p.publish(p.details); err != nil {
					fmt.Fprintf(os.Stderr, "colca-service: refresh %s: %v\n", p.details.Name, err)
				}
			}
		case <-ctx.Done():
			for _, p := range live {
				_ = p.publish(nil)
			}
			return nil
		}
	}
}

func parseRegistrations(raw string) ([]registration, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("SERVICE_REGISTRATIONS is required and must be a JSON array")
	}
	var registrations []registration
	if err := json.Unmarshal([]byte(raw), &registrations); err != nil {
		return nil, fmt.Errorf("SERVICE_REGISTRATIONS: %w", err)
	}
	if len(registrations) == 0 {
		return nil, fmt.Errorf("SERVICE_REGISTRATIONS names no services")
	}
	for i := range registrations {
		if registrations[i].Details.Name == "" || registrations[i].Details.ServiceType == "" {
			return nil, fmt.Errorf(
				"SERVICE_REGISTRATIONS[%d]: service name and service_type are required", i)
		}
	}
	return registrations, nil
}

// publisher holds one service's resolved identity and the closure that writes
// its record.
type publisher struct {
	details serviceDetails
	publish func(payload any) error
}

func start(
	client *http.Client, baseURL string, reg registration, deadline time.Time,
) (*publisher, error) {
	details := reg.Details
	details.IsActive = true
	if details.Hierarchy == nil {
		details.Hierarchy = []string{}
	}
	if details.Metadata == nil {
		details.Metadata = map[string]any{}
	}
	if details.ArchitectureMetadata == nil {
		details.ArchitectureMetadata = map[string]any{}
	}
	if details.HealthMetrics == nil {
		details.HealthMetrics = []map[string]any{}
	}

	identity, err := self(client, baseURL, details.Name, reg.Mount, deadline)
	if err != nil {
		return nil, fmt.Errorf("resolve local identity: %w", err)
	}
	details.ID = identity.ULID
	details.ColcaNodeID = identity.Node
	details.SystemElementID = identity.Element
	// Mount plus this service's NAME — the rule lives in the domain package
	// because two languages need it and they must not each keep a version.
	// Without the name every unplaced service publishes to one topic and
	// erases the others (uns.ServiceContext).
	recordContext := uns.ServiceContext(identity.Mount, details.Name)
	details.Hierarchy = recordContext
	topic := "colca/v1/_ServiceDetails/" + identity.Node + "/" +
		strings.Join(recordContext, "/") + "/_service"

	p := &publisher{details: details}
	p.publish = func(payload any) error {
		body := map[string]any{"topic": topic}
		if payload != nil {
			body["payload"] = payload
		}
		return postJSON(client, baseURL+"/publish", details.Name, reg.Mount, body)
	}

	for {
		err = p.publish(details)
		if err == nil {
			return p, nil
		}
		var refusal rejected
		if asRejected(err, &refusal) || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(time.Second)
	}
}

func asRejected(err error, into *rejected) bool {
	r, ok := err.(rejected)
	if ok {
		*into = r
	}
	return ok
}

func self(
	client *http.Client, baseURL, name, mount string, deadline time.Time,
) (localIdentity, error) {
	for {
		identity, err := selfOnce(client, baseURL, name, mount)
		if err == nil {
			return identity, nil
		}
		var refusal rejected
		if asRejected(err, &refusal) || time.Now().After(deadline) {
			return localIdentity{}, err
		}
		time.Sleep(time.Second)
	}
}

func selfOnce(client *http.Client, baseURL, name, mount string) (localIdentity, error) {
	req, err := http.NewRequest(http.MethodGet, baseURL+"/self", nil)
	if err != nil {
		return localIdentity{}, err
	}
	localHeaders(req, name, mount)
	resp, err := client.Do(req)
	if err != nil {
		return localIdentity{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return localIdentity{}, statusError(resp.StatusCode, body)
	}
	var identity localIdentity
	if err := json.NewDecoder(resp.Body).Decode(&identity); err != nil {
		return localIdentity{}, err
	}
	if identity.ULID == "" || identity.Node == "" || identity.Name != name {
		return localIdentity{}, fmt.Errorf("invalid /self response for %q", name)
	}
	return identity, nil
}

func localHeaders(req *http.Request, name, mount string) {
	req.Header.Set("X-Colca-Service", name)
	if mount != "" {
		req.Header.Set("X-Colca-Mount", mount)
	}
}

func postJSON(client *http.Client, url, name, mount string, body any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	localHeaders(req, name, mount)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return statusError(resp.StatusCode, responseBody)
	}
	return nil
}

// statusError separates "the node said no" from "the node did not answer".
// A 4xx is a judgement on the record itself; retrying an invalid payload just
// burns, which is precisely what the per-service sidecars used to do.
func statusError(status int, body []byte) error {
	err := fmt.Errorf("HTTP %d: %s", status, body)
	if status >= 400 && status < 500 {
		return rejected{err: err}
	}
	return err
}

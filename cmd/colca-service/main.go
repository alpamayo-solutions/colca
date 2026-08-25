// Command colca-service publishes one _ServiceDetails record for a service
// that cannot publish its own observed state. It uses Colca's deployment-local
// HTTP door: the service name selects a stable registry entry and no key,
// certificate, token, or enrollment step exists.
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
	ID                   string         `json:"id"`
	Name                 string         `json:"name"`
	DisplayName          string         `json:"display_name"`
	Description          string         `json:"description"`
	ServiceType          string         `json:"service_type"`
	ColcaNodeID         string         `json:"colca_node_id"`
	SystemElementID      string         `json:"system_element_id,omitempty"`
	Hierarchy            []string       `json:"hierarchy"`
	IsActive             bool           `json:"is_active"`
	Metadata             map[string]any `json:"metadata"`
	ArchitectureMetadata map[string]any `json:"architecture_metadata"`
}

type localIdentity struct {
	ULID    string `json:"ulid"`
	Name    string `json:"name"`
	Node    string `json:"node"`
	Element string `json:"element"`
	Mount   string `json:"mount"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "colca-service:", err)
		os.Exit(1)
	}
}

func run() error {
	var details serviceDetails
	if err := json.Unmarshal([]byte(os.Getenv("SERVICE_DETAILS_JSON")), &details); err != nil {
		return fmt.Errorf("SERVICE_DETAILS_JSON: %w", err)
	}
	if details.Name == "" || details.ServiceType == "" {
		return fmt.Errorf("service name and service_type are required")
	}
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

	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := os.Getenv("COLCA_URL")
	if baseURL == "" {
		baseURL = "http://colca"
	}
	declaredMount := os.Getenv("COLCA_SERVICE_MOUNT")

	identity, err := self(client, baseURL, details.Name, declaredMount)
	if err != nil {
		return fmt.Errorf("resolve local identity: %w", err)
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
	publish := func(payload any) error {
		body := map[string]any{"topic": topic}
		if payload != nil {
			body["payload"] = payload
		}
		return postJSON(client, baseURL+"/publish", details.Name, declaredMount, body)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for {
		err = publish(details)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("publish: %w", err)
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("published %s as %s\n", details.Name, details.ID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := publish(details); err != nil {
				fmt.Fprintln(os.Stderr, "colca-service: refresh:", err)
			}
		case <-ctx.Done():
			_ = publish(nil)
			return nil
		}
	}
}

func self(client *http.Client, baseURL, name, mount string) (localIdentity, error) {
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
		return localIdentity{}, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, responseBody)
	}
	return nil
}

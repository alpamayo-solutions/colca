// Command colca-service publishes one _ServiceDetails record using an already
// provisioned machine identity. Enrollment and key creation remain explicit
// operator actions; this process receives neither an admin token nor authority
// to alter the registry.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alpamayo-solutions/colca/internal/identity"
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
	if details.ID == "" || details.Name == "" || details.ColcaNodeID == "" {
		return fmt.Errorf("service id, name and colca_node_id are required")
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

	keyPath := os.Getenv("COLCA_SERVICE_KEY")
	if keyPath == "" {
		keyPath = "/identity/service.key"
	}
	id, err := identity.Load(keyPath)
	if err != nil {
		return fmt.Errorf("load provisioned identity: %w", err)
	}
	cert, err := id.SelfSignedCert(details.ID)
	if err != nil {
		return err
	}
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true, // #nosec G402 -- node-local API may use its self-signed key container
		MinVersion:         tls.VersionTLS13,
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{TLSClientConfig: tlsConfig},
	}
	baseURL := os.Getenv("COLCA_URL")
	if baseURL == "" {
		baseURL = "https://colca:8080"
	}
	mount := os.Getenv("COLCA_SERVICE_MOUNT")
	if mount != "" {
		mount += "/"
	}
	// Local trust pins level 4 to the Colca node. The authenticated machine
	// identity remains the payload id and immutable written_by attribution.
	topic := "colca/v1/_ServiceDetails/" + details.ColcaNodeID + "/" + mount + "_service"
	publish := func(payload any) error {
		body := map[string]any{"topic": topic}
		if payload != nil {
			body["payload"] = payload
		}
		return postJSON(client, baseURL+"/publish", body)
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

func postJSON(client *http.Client, url string, body any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := client.Post(url, "application/json", bytes.NewReader(encoded))
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

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLocalDoorUsesServiceAndMountWithoutCredential(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Colca-Service"); got != "projector" {
			t.Errorf("service header = %q", got)
		}
		if got := r.Header.Get("X-Colca-Mount"); got != "plant/line" {
			t.Errorf("mount header = %q", got)
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Colca-Token") != "" {
			t.Error("local request carried a credential")
		}
		switch r.URL.Path {
		case "/self":
			_ = json.NewEncoder(w).Encode(localIdentity{
				ULID: "service-id", Name: "projector", Node: "node-id",
				Element: "element-id", Mount: "plant/line",
			})
		case "/publish":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	identity, err := self(
		server.Client(), server.URL, "projector", "plant/line", time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if identity.ULID != "service-id" || identity.Node != "node-id" {
		t.Fatalf("identity = %#v", identity)
	}
	if err := postJSON(
		server.Client(), server.URL+"/publish", "projector", "plant/line",
		map[string]any{"topic": "colca/v1/_ServiceDetails/node-id/plant/line/_service"},
	); err != nil {
		t.Fatal(err)
	}
}

// ── one process, every service ───────────────────────────────────────────────
//
// This binary used to speak for exactly one service, and a generated node ran
// one container per service. These tests pin what has to stay true now that it
// speaks for all of them: each still gets its own identity and its own topic,
// and one bad record cannot cost the others their registration.

type recordingDoor struct {
	server    *httptest.Server
	mu        sync.Mutex
	published map[string]string // service name -> topic
	refuse    map[string]bool   // service names the node rejects on their merits
	selfCalls []string
}

func newRecordingDoor(t *testing.T, refuse ...string) *recordingDoor {
	t.Helper()
	door := &recordingDoor{
		published: map[string]string{},
		refuse:    map[string]bool{},
	}
	for _, name := range refuse {
		door.refuse[name] = true
	}
	door.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.Header.Get("X-Colca-Service")
		mount := r.Header.Get("X-Colca-Mount")
		door.mu.Lock()
		defer door.mu.Unlock()
		switch r.URL.Path {
		case "/self":
			door.selfCalls = append(door.selfCalls, name)
			_ = json.NewEncoder(w).Encode(localIdentity{
				ULID:  "ulid-" + name,
				Name:  name,
				Node:  "node-id",
				Mount: mount,
			})
		case "/publish":
			if door.refuse[name] {
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = fmt.Fprint(w, "at '/service_type': value must be one of ...")
				return
			}
			var body struct {
				Topic   string `json:"topic"`
				Payload *struct {
					Name string `json:"name"`
				} `json:"payload"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Payload != nil {
				door.published[body.Payload.Name] = body.Topic
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(door.server.Close)
	return door
}

func registrationsJSON(t *testing.T, entries ...registration) string {
	t.Helper()
	encoded, err := json.Marshal(entries)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func entry(name, serviceType, mount string) registration {
	return registration{
		Mount: mount,
		Details: serviceDetails{
			Name: name, DisplayName: name, ServiceType: serviceType,
		},
	}
}

func TestEveryServiceGetsItsOwnIdentityAndItsOwnTopic(t *testing.T) {
	t.Parallel()
	door := newRecordingDoor(t)

	raw := registrationsJSON(t,
		entry("projector", "other", ""),
		entry("grafana", "grafana", ""),
		entry("keycloak", "auth-service", "plant/line"),
	)
	registrations, err := parseRegistrations(raw)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for _, reg := range registrations {
		if _, err := start(door.server.Client(), door.server.URL, reg, deadline); err != nil {
			t.Fatalf("%s: %v", reg.Details.Name, err)
		}
	}

	door.mu.Lock()
	defer door.mu.Unlock()
	if len(door.published) != 3 {
		t.Fatalf("published = %#v", door.published)
	}
	// The name is part of the topic, so unplaced services do not erase each
	// other — the whole reason one process may speak for all of them.
	if got := door.published["projector"]; got != "colca/v1/_ServiceDetails/node-id/projector/_service" {
		t.Errorf("projector topic = %q", got)
	}
	if got := door.published["grafana"]; got != "colca/v1/_ServiceDetails/node-id/grafana/_service" {
		t.Errorf("grafana topic = %q", got)
	}
	if got := door.published["keycloak"]; got != "colca/v1/_ServiceDetails/node-id/plant/line/keycloak/_service" {
		t.Errorf("keycloak topic = %q", got)
	}
	if len(door.selfCalls) != 3 {
		t.Errorf("each service resolves its OWN identity; /self calls = %v", door.selfCalls)
	}
}

func TestARefusedRecordDoesNotCostTheOthersTheirRegistration(t *testing.T) {
	door := newRecordingDoor(t, "pgbouncer")

	t.Setenv("COLCA_URL", door.server.URL)
	t.Setenv("SERVICE_REGISTRATIONS", registrationsJSON(t,
		entry("pgbouncer", "connection-pooler", ""),
		entry("projector", "other", ""),
		entry("grafana", "grafana", ""),
	))

	err := run()
	if err == nil {
		t.Fatal("a record the node refused must be reported, not swallowed")
	}
	// It names the offender and says retrying will not help — the sidecar it
	// replaces printed the 422 and exited, forever, every two minutes.
	if !strings.Contains(err.Error(), "pgbouncer") {
		t.Errorf("the error does not name the refused service: %v", err)
	}
	if !strings.Contains(err.Error(), "retrying will not help") {
		t.Errorf("the error does not say a retry is pointless: %v", err)
	}

	door.mu.Lock()
	defer door.mu.Unlock()
	if _, ok := door.published["projector"]; !ok {
		t.Error("projector lost its registration to another service's bad record")
	}
	if _, ok := door.published["grafana"]; !ok {
		t.Error("grafana lost its registration to another service's bad record")
	}
}

func TestAnEmptyOrMalformedRegistrationListIsRefusedAtStartup(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", "   ", "[]", "not json", `[{"details":{"name":"x"}}]`} {
		if _, err := parseRegistrations(raw); err == nil {
			t.Errorf("parseRegistrations(%q) accepted a list it cannot publish", raw)
		}
	}
	// The denominator: the same parser accepts a well-formed list, so the
	// refusals above are refusals and not a parser that rejects everything.
	if _, err := parseRegistrations(
		`[{"mount":"","details":{"name":"x","service_type":"other"}}]`,
	); err != nil {
		t.Errorf("a well-formed list was refused: %v", err)
	}
}

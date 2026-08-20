package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalDoorUsesServiceAndMountWithoutCredential(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Colca-Service"); got != "projector" {
			t.Fatalf("service header = %q", got)
		}
		if got := r.Header.Get("X-Colca-Mount"); got != "plant/line" {
			t.Fatalf("mount header = %q", got)
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("X-Colca-Token") != "" {
			t.Fatal("local request carried a credential")
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

	identity, err := self(server.Client(), server.URL, "projector", "plant/line")
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

func TestSplitMountDropsEmptySegments(t *testing.T) {
	t.Parallel()
	got := splitMount("/plant//line/")
	if len(got) != 2 || got[0] != "plant" || got[1] != "line" {
		t.Fatalf("splitMount = %#v", got)
	}
}

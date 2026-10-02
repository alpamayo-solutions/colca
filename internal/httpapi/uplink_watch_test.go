package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRootUplinkWatchStartsWithCurrentState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { watchUplink(w, r, nil) }))
	defer server.Close()
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var state struct {
		Uplink struct {
			State string `json:"state"`
		} `json:"uplink"`
	}
	if err := json.NewDecoder(response.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if state.Uplink.State != "none" {
		t.Fatalf("state = %s", state.Uplink.State)
	}
}

package node

import (
	"encoding/json"
	"net"
	"testing"
)

func TestInterfaceTypeRecognisesVPNs(t *testing.T) {
	mac := net.HardwareAddr{0x02, 0, 0, 0, 0, 1}
	for _, tc := range []struct {
		iface net.Interface
		want  string
	}{
		{net.Interface{Name: "tailscale0", Flags: net.FlagUp | net.FlagPointToPoint}, "vpn"},
		{net.Interface{Name: "wg0"}, "vpn"},
		{net.Interface{Name: "tun0"}, "vpn"},
		{net.Interface{Name: "tap0", HardwareAddr: mac}, "vpn"},
		{net.Interface{Name: "corpvpn1", HardwareAddr: mac}, "vpn"},
		{net.Interface{Name: "ztc3qxyz", HardwareAddr: mac}, "vpn"},
		{net.Interface{Name: "wlan0", HardwareAddr: mac}, "wifi"},
		{net.Interface{Name: "eth0", HardwareAddr: mac}, "ethernet"},
		{net.Interface{Name: "docker0", HardwareAddr: mac}, "virtual"},
	} {
		if got := interfaceType(tc.iface); got != tc.want {
			t.Errorf("interfaceType(%s) = %q, want %q", tc.iface.Name, got, tc.want)
		}
	}
}

// The clock declaration reads the recording rule a deployment derives from
// colca's clock metrics, in milliseconds, with the thresholds at which command
// expiry starts to be judged differently on two nodes.
func TestNodeHealthDeclaresTheClock(t *testing.T) {
	for _, declaration := range nodeHealthMetrics() {
		if declaration["key"] != "clock" {
			continue
		}
		thresholds := declaration["thresholds"].(map[string]any)
		if thresholds["warning"] != 500 || thresholds["critical"] != 5000 || declaration["unit"] != "ms" {
			t.Fatalf("clock declaration = %v, want unit ms, warning 500, critical 5000", declaration)
		}
		const query = `max({{__name__=~"(edge|hub):clock_error_seconds",node_id=~"{node_id_pattern}"}}) * 1000`
		if declaration["query"] != query {
			t.Fatalf("clock query = %v, want %s", declaration["query"], query)
		}
		return
	}
	t.Fatal("the node declares no clock health metric")
}

// A node record written by an older release holds the declarations of that
// release. The upgrade adds what is missing and leaves the held ones alone,
// edits included.
func TestMissingHealthMetricsNamesOnlyWhatTheHeldListLacks(t *testing.T) {
	current := nodeHealthMetrics()
	var held []any
	for _, declaration := range current {
		if declaration["key"] == "clock" {
			continue
		}
		raw, err := json.Marshal(declaration)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any // as the store returns it
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		decoded["name"] = "edited by an operator"
		held = append(held, decoded)
	}
	missing := missingHealthMetrics(held, current)
	if len(missing) != 1 || missing[0]["key"] != "clock" {
		t.Fatalf("missing = %v, want exactly the clock declaration", missing)
	}
	if again := missingHealthMetrics(append(held, missing[0]), current); len(again) != 0 {
		t.Fatalf("a complete list still misses %v", again)
	}
}

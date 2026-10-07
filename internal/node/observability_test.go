package node

import (
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

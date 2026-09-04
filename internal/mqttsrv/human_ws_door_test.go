package mqttsrv

import (
	"net"
	"testing"
	"time"
)

// A ":0" WebSocket door serves on the port it reports, and holds it from the
// moment New returns: the listener binds at Init (our mochi fork) instead of
// pre-resolving the port by binding and releasing it — the release window is
// where another ":0" door in the same process (the API door, in the topology
// tests) was handed the same port, after which the WebSocket door failed to
// serve and the address it advertised answered HTTPS to a wss dial.
func TestTheHumanWebsocketDoorServesThePortItReports(t *testing.T) {
	w := newHumanWorld(t)
	addr := w.srv.HumanWSAddr()
	if addr == "" || addr == "127.0.0.1:0" {
		t.Fatalf("websocket door address = %q, want the kernel-assigned port", addr)
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("the reported websocket door %s does not accept: %v", addr, err)
	}
	_ = conn.Close()
	if ln, err := net.Listen("tcp", addr); err == nil {
		_ = ln.Close()
		t.Fatalf("the reported websocket door %s is not held by the broker — another listener could take it", addr)
	}
}

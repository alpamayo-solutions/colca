package mqttsrv

import (
	"net"
	"testing"
	"time"
)

// A ":0" WebSocket door serves on the port it reports and holds it from New on:
// our mochi fork binds at Init, so no other ":0" door in the process can take the
// port in between.
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

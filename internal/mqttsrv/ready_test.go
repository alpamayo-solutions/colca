package mqttsrv

import (
	"context"
	"net"
	"testing"
	"time"
)

// The websocket door binds inside mochi's Serve, not Init, so the address New
// reports is not yet accepting when it returns. Ready is what closes that gap;
// these two cases prove it reports the difference rather than always passing.
func TestReadyReportsWhetherTheHumanWebsocketDoorAccepts(t *testing.T) {
	w := newHumanWorld(t)

	addr := w.srv.HumanWSAddr()
	if addr == "" {
		t.Fatal("no websocket door configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.srv.Ready(ctx); err != nil {
		t.Fatalf("ready after serve: %v", err)
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("websocket door refused after Ready returned: %v", err)
	}
	_ = conn.Close()
}

func TestReadyFailsWhileTheDoorIsNotServing(t *testing.T) {
	// A server that was built but never served: Ready must not report success,
	// or it would be a no-op that every caller believes.
	w := newUnservedHumanWorld(t)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := w.srv.Ready(ctx); err == nil {
		t.Fatal("Ready reported success for a server that never served")
	}
}

// The asymmetry that made this worth a method rather than a sleep: the TCP
// door binds in AddListener and accepts immediately, the websocket door does
// not bind until Serve. If mochi ever binds websockets in Init this fails, and
// Ready can then be simplified to reporting nil.
func TestOnlyTheWebsocketDoorIsUnboundBeforeServe(t *testing.T) {
	w := newUnservedHumanWorld(t)

	tcpConn, err := net.DialTimeout("tcp", w.srv.HumanTCPAddr(), time.Second)
	if err != nil {
		t.Fatalf("tcp door should accept before Serve: %v", err)
	}
	_ = tcpConn.Close()

	wsConn, err := net.DialTimeout("tcp", w.srv.HumanWSAddr(), 200*time.Millisecond)
	if err == nil {
		_ = wsConn.Close()
		t.Fatal("websocket door accepted before Serve; Ready may no longer be needed")
	}
}

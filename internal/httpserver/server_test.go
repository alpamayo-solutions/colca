package httpserver

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewAppliesTheCrossDoorLimits(t *testing.T) {
	server := New(http.NotFoundHandler())

	if server.ReadHeaderTimeout != ReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", server.ReadHeaderTimeout, ReadHeaderTimeout)
	}
	if server.IdleTimeout != IdleTimeout {
		t.Fatalf("IdleTimeout = %s, want %s", server.IdleTimeout, IdleTimeout)
	}
	if server.MaxHeaderBytes != MaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", server.MaxHeaderBytes, MaxHeaderBytes)
	}
}

func TestASlowHeaderNeverReachesTheHandler(t *testing.T) {
	var handled atomic.Bool
	server := New(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		handled.Store(true)
	}))
	server.ReadHeaderTimeout = 50 * time.Millisecond
	addr := serveForTest(t, server)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: slow"); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	status, _ := bufio.NewReader(conn).ReadString('\n')
	if strings.Contains(status, " 200 ") || handled.Load() {
		t.Fatalf("slow partial header reached handler: status=%q handled=%v", status, handled.Load())
	}
}

func TestAnOversizedHeaderIsRejectedBeforeTheHandler(t *testing.T) {
	var handled atomic.Bool
	server := New(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		handled.Store(true)
	}))
	server.MaxHeaderBytes = 256
	addr := serveForTest(t, server)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: large\r\nX-Fill: %s\r\n\r\n", strings.Repeat("a", 8192)); err != nil {
		t.Fatal(err)
	}

	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusRequestHeaderFieldsTooLarge)
	}
	if handled.Load() {
		t.Fatal("oversized header reached handler")
	}
}

func TestAnIdleKeepAliveConnectionIsClosed(t *testing.T) {
	server := New(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "2")
		_, _ = io.WriteString(w, "ok")
	}))
	server.IdleTimeout = 50 * time.Millisecond
	addr := serveForTest(t, server)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	if _, err := fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: idle\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()

	time.Sleep(3 * server.IdleTimeout)
	_, _ = fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: idle\r\n\r\n")
	response, err = http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err == nil {
		_ = response.Body.Close()
		t.Fatal("idle keep-alive connection remained open")
	}
}

func serveForTest(t *testing.T, server *http.Server) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
	})
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String()
}

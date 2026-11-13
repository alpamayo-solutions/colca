package repl

import (
	"log/slog"
	"strings"
	"testing"
)

// Tests read the captured log while the uplink, downlink and server goroutines
// they started still write to it (the set-aside test reads it before RunUplink
// stops). The buffer must take a read during a write: an unsynchronized
// bytes.Buffer is a data race under -race.
func TestCapturedLogsCanBeReadWhileGoroutinesLog(t *testing.T) {
	logs := captureLogs(t)
	stop, done, started := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		slog.Info("a line from another goroutine")
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				slog.Info("a line from another goroutine")
			}
		}
	}()
	<-started
	for range 200 {
		_ = logs.String()
	}
	close(stop)
	<-done
	if !strings.Contains(logs.String(), "a line from another goroutine") {
		t.Fatal("the line was not captured")
	}
}

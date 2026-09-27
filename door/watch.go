package door

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// Signal coalesces notifications without losing a change between checking a
// queue and sleeping. Capture Changes BEFORE reading the durable queue.
type Signal struct {
	mu      sync.Mutex
	changed chan struct{}
}

func (s *Signal) Changes() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}
func (s *Signal) Notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
}

// WaitChange wakes on a hint or a scheduled deadline; a negative delay waits only for hints. Cancellation interrupts
// both the wait and the fixed batching window; new arrivals cannot extend it.
func WaitChange(ctx context.Context, changed <-chan struct{}, recovery time.Duration, notBefore time.Time) bool {
	var due <-chan time.Time
	if recovery >= 0 {
		timer := time.NewTimer(recovery)
		defer timer.Stop()
		due = timer.C
	}
	select {
	case <-ctx.Done():
		return false
	case <-changed:
	case <-due:
	}
	if delay := time.Until(notBefore); delay > 0 {
		batch := time.NewTimer(delay)
		defer batch.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-batch.C:
		}
	}
	return ctx.Err() == nil
}

// watchSilence is how long a watch may stay silent before the client gives up on
// the connection. The node writes a heartbeat at least every 5 s.
const watchSilence = 15 * time.Second

// Hint is one line of GET /watch: the streams that grew and their next offsets.
// A heartbeat names no stream and is not passed on.
type Hint struct {
	Streams []string         `json:"streams"`
	Next    map[string]int64 `json:"next,omitempty"`
}

// Watch holds GET /watch open and calls notify for every hint, until ctx ends or
// the connection fails, and returns why. The first hint names every stream, so a
// consumer that drains its streams on each hint misses nothing across a
// reconnect. interval is the least time between two hints (0: the node's
// default). Reconnecting, with a pause, is the caller's: see WatchForever.
func (c *Client) Watch(ctx context.Context, streams []string, interval time.Duration, notify func(Hint)) error {
	q := url.Values{"stream": streams}
	if interval > 0 {
		q.Set("interval_ms", strconv.FormatInt(interval.Milliseconds(), 10))
	}
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(watchCtx, http.MethodGet, c.BaseURL+"/watch?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	// The connection stays open: no total deadline, a silence watchdog instead.
	hc := *c.http()
	hc.Timeout = 0
	if c.Token != "" {
		req.Header.Set("X-Colca-Token", c.Token)
	}
	if c.Service != "" {
		req.Header.Set("X-Colca-Service", c.Service)
	}
	watchdog := time.AfterFunc(watchSilence, cancel)
	defer watchdog.Stop()
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("watching %v: %w", streams, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		reason, _ := io.ReadAll(resp.Body)
		return httpResponseError(resp, fmt.Errorf("watching %v: HTTP %d: %s", streams, resp.StatusCode, truncate(reason, 300)))
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024), 64<<10)
	for scanner.Scan() {
		watchdog.Reset(watchSilence)
		var hint Hint
		if err := json.Unmarshal(scanner.Bytes(), &hint); err != nil {
			return fmt.Errorf("watching %v: %w", streams, err)
		}
		if len(hint.Streams) > 0 {
			notify(hint)
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("watching %v: %w", streams, err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("watching %v: connection closed", streams)
}

// WatchForever runs Watch until ctx ends, reconnecting after pause when the
// connection drops; failed, if set, hears why.
func (c *Client) WatchForever(ctx context.Context, streams []string, interval, pause time.Duration, notify func(Hint), failed func(error)) {
	if pause <= 0 {
		pause = time.Second
	}
	delay := pause
	for {
		started := time.Now()
		err := c.Watch(ctx, streams, interval, notify)
		if ctx.Err() != nil {
			return
		}
		if failed != nil {
			failed(err)
		}
		if time.Since(started) >= 30*time.Second {
			delay = pause
		}
		timer := time.NewTimer(RetryDelay(err, delay))
		delay = min(30*time.Second, delay*2)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

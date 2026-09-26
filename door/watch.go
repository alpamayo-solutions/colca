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
	"time"
)

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
		return fmt.Errorf("watching %v: HTTP %d: %s", streams, resp.StatusCode, truncate(reason, 300))
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
	for {
		err := c.Watch(ctx, streams, interval, notify)
		if ctx.Err() != nil {
			return
		}
		if failed != nil {
			failed(err)
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

package httpapi

import (
	"encoding/json"
	"github.com/alpamayo-solutions/colca/internal/repl"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
)

const (
	// watchHeartbeat is the longest a watch stays silent, so a client's read
	// deadline notices a dead connection.
	watchHeartbeat = 5 * time.Second
	// watchWriteDeadline bounds one write, so a stalled reader cannot hold its
	// concurrency slot forever.
	watchWriteDeadline = 5 * time.Second
	// watchDefaultInterval is the least time between two hints on one connection
	// unless the client asks otherwise; hints in between are merged.
	watchDefaultInterval = 100 * time.Millisecond
	watchMaxInterval     = 10 * time.Second
)

// watchHint is one NDJSON line of GET /watch: the streams that grew since the
// previous line, and each one's next offset. A heartbeat names no stream.
type watchHint struct {
	Streams []string          `json:"streams"`
	Next    map[string]uint64 `json:"next,omitempty"`
}

// serveWatch holds one connection open and writes a line whenever a selected
// stream grows, instead of the client polling /fetch. The first line names
// every selected stream, so a client that reconnects drains once and misses
// nothing. Lines are at least interval apart; streams that grow in between are
// merged into the next line, each named once. Hints carry no records and move
// no cursor: the client still reads with /fetch.
func serveWatch(w http.ResponseWriter, r *http.Request, s *store.Store, writeJSON func(http.ResponseWriter, int, any)) {
	q := r.URL.Query()
	streams, bad := watchStreams(q["stream"])
	if bad != "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": bad})
		return
	}
	contracts := q["contract"]
	if len(contracts) > 64 {
		writeJSON(w, 400, map[string]any{"error": "too many contracts"})
		return
	}
	for _, contract := range contracts {
		if !strings.HasPrefix(contract, "_") || len(contract) > 128 || strings.ContainsAny(contract, "/+# ") {
			writeJSON(w, 400, map[string]any{"error": "invalid contract"})
			return
		}
	}
	interval := watchDefaultInterval
	if raw := q.Get("interval_ms"); raw != "" {
		ms, err := strconv.Atoi(raw)
		if err != nil || ms < 0 || time.Duration(ms)*time.Millisecond > watchMaxInterval {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "interval_ms must be between 0 and 10000"})
			return
		}
		interval = time.Duration(ms) * time.Millisecond
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	control := http.NewResponseController(w)
	encoder := json.NewEncoder(w)
	emit := func(hint watchHint) bool {
		_ = control.SetWriteDeadline(time.Now().Add(watchWriteDeadline))
		if encoder.Encode(hint) != nil {
			return false
		}
		return control.Flush() == nil
	}

	var sent map[string]store.StreamPosition // next offsets as of the last hint; nil before the first
	var lastEmit time.Time
	heartbeat := time.NewTimer(watchHeartbeat)
	defer heartbeat.Stop()
	for {
		var waits map[string]<-chan struct{}
		var next map[string]uint64
		var positions map[string]store.StreamPosition
		if len(contracts) > 0 {
			// One snapshot: the hint's next offset must include every append
			// the scoped positions include, or the hint would understate it.
			var wake <-chan struct{}
			wake, next, positions = s.ScopedStreamChanges(streams, contracts)
			waits = make(map[string]<-chan struct{}, len(streams))
			for _, stream := range streams {
				waits[stream] = wake
			}
		} else {
			waits, next = s.StreamChanges(streams)
			positions = make(map[string]store.StreamPosition, len(next))
			for stream, offset := range next {
				positions[stream] = store.StreamPosition{Next: offset}
			}
		}
		hint := watchHint{Streams: []string{}, Next: map[string]uint64{}}
		for _, stream := range streams {
			if sent == nil || positions[stream] != sent[stream] {
				hint.Streams = append(hint.Streams, stream)
				hint.Next[stream] = next[stream]
			}
		}
		if len(hint.Streams) > 0 {
			if wait := time.Until(lastEmit.Add(interval)); sent != nil && wait > 0 {
				pause := time.NewTimer(wait)
				select {
				case <-r.Context().Done():
					pause.Stop()
					return
				case <-pause.C:
				}
				continue
			}
			if !emit(hint) {
				return
			}
			sent, lastEmit = positions, time.Now()
			heartbeat.Reset(watchHeartbeat)
			continue
		}

		cases := make([]reflect.SelectCase, 0, len(waits)+2)
		cases = append(cases,
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.Context().Done())},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(heartbeat.C)},
		)
		for _, stream := range streams {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(waits[stream])})
		}
		switch chosen, _, _ := reflect.Select(cases); chosen {
		case 0:
			return
		case 1:
			if !emit(watchHint{Streams: []string{}}) {
				return
			}
			heartbeat.Reset(watchHeartbeat)
		}
	}
}

// watchStreams checks the selected streams: at least one, each a stream the
// node keeps, repeats dropped. The reason is empty when they are valid.
func watchStreams(requested []string) ([]string, string) {
	if len(requested) == 0 {
		return nil, "select at least one stream with ?stream="
	}
	known := map[string]bool{}
	for _, stream := range store.Streams() {
		known[stream] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, stream := range requested {
		if !known[stream] {
			return nil, "unknown stream " + strconv.Quote(stream)
		}
		if !seen[stream] {
			seen[stream] = true
			out = append(out, stream)
		}
	}
	return out, ""
}

// Queue observers receive coalesced hints, not periodic storage reads. Heartbeats
// prove transport liveness without asking clients to refresh an unchanged queue.
func watchBacklog(w http.ResponseWriter, r *http.Request, s *store.Store) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	control := http.NewResponseController(w)
	emit := func(changed bool) bool {
		_ = control.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if json.NewEncoder(w).Encode(map[string]bool{"backlog_changed": changed}) != nil {
			return false
		}
		return control.Flush() == nil
	}
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()
	wake := s.BacklogChanges() // capture before initial hydration hint
	if !emit(true) {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if !emit(false) {
				return
			}
		case <-wake:
			// Fixed window: additional commits do not postpone this deadline.
			timer := time.NewTimer(time.Second)
			select {
			case <-r.Context().Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			wake = s.BacklogChanges()
			if !emit(true) {
				return
			}
		}
	}
}

// watchUplink streams lifecycle transitions; heartbeats carry no status read.
func watchUplink(w http.ResponseWriter, r *http.Request, client *repl.Client) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	control := http.NewResponseController(w)
	emit := func(value any) bool {
		_ = control.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return json.NewEncoder(w).Encode(value) == nil && control.Flush() == nil
	}
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()
	var wake <-chan struct{}
	for {
		if client == nil {
			if !emit(map[string]any{"uplink": map[string]string{"state": "none"}}) {
				return
			}
		} else {
			wake = client.StatusChanges()
			if !emit(map[string]any{"uplink": client.Status()}) {
				return
			}
		}
		for {
			select {
			case <-r.Context().Done():
				return
			case <-wake:
				goto changed
			case <-heartbeat.C:
				if !emit(map[string]any{"heartbeat": true}) {
					return
				}
			}
		}
	changed:
	}
}

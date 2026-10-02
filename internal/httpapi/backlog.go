package httpapi

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/alpamayo-solutions/colca/internal/cursorwatch"
	"github.com/alpamayo-solutions/colca/internal/engine"
)

// Row caps of one /backlog answer: a local service selects its own queues, the
// admin may list the whole inventory.
const (
	maxLocalBacklogRows = 256
	maxAdminBacklogRows = 4096
)

// backlog is bounded cursor telemetry, not a record read or an ack. Each row is
// a cursor's position, its stream's head, the distance between them, when it
// last moved and whether it is stale (cursorwatch.Stale at cursors.stale_after).
// The local door requires 1–32 cursor prefixes; the admin may omit them to list
// every cursor. Return an error instead of truncating away an overloaded
// consumer.
func backlog(w http.ResponseWriter, r *http.Request, e *engine.Engine, staleAfter time.Duration, admin bool) {
	prefixes := r.URL.Query()["prefix"]
	limit := maxLocalBacklogRows
	if admin {
		limit = maxAdminBacklogRows
		if len(prefixes) == 0 {
			prefixes = []string{""}
		}
	} else if len(prefixes) == 0 {
		http.Error(w, "select 1–32 cursor prefixes", http.StatusBadRequest)
		return
	}
	if len(prefixes) > 32 {
		http.Error(w, "select 1–32 cursor prefixes", http.StatusBadRequest)
		return
	}
	for _, prefix := range prefixes {
		if prefix == "" && !admin {
			http.Error(w, "cursor prefixes must not be empty", http.StatusBadRequest)
			return
		}
	}
	s := e.Store()
	cursors, err := s.CursorPositions(prefixes, limit)
	if err != nil {
		http.Error(w, "cursor inventory unavailable or too large; narrow prefixes", http.StatusServiceUnavailable)
		return
	}
	now := time.Now()
	rows := []map[string]any{}
	for _, cursor := range cursors {
		wanted := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(cursor.Name, prefix) {
				wanted = true
			}
		}
		if !wanted {
			continue
		}
		head := s.NextOffset(cursor.Stream)
		lag := uint64(0)
		if head > cursor.Position {
			lag = head - cursor.Position
		}
		rows = append(rows, map[string]any{
			"cursor": cursor.Name, "stream": cursor.Stream, "position": cursor.Position, "head": head, "lag_records": lag,
			"last_ack_ms":      cursor.LastAdvanceMS,
			"stale":            cursorwatch.Stale(cursor, head, now, staleAfter),
			"read_since_start": e.CursorFilters().ReadSinceStart(cursor.Name, cursor.Stream),
		})
		if len(rows) > limit {
			http.Error(w, "too many consumers; narrow prefixes", http.StatusBadRequest)
			return
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i]["cursor"].(string)+rows[i]["stream"].(string) < rows[j]["cursor"].(string)+rows[j]["stream"].(string)
	})
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"queues": rows})
}

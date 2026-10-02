package httpapi

import (
	"encoding/json"
	"github.com/alpamayo-solutions/colca/internal/store"
	"net/http"
	"sort"
	"strings"
)

// backlog is bounded local operational telemetry, not a record read or an ack.
// Return an error instead of truncating away an overloaded consumer.
func backlog(w http.ResponseWriter, r *http.Request, s *store.Store) {
	prefixes := r.URL.Query()["prefix"]
	if len(prefixes) == 0 || len(prefixes) > 32 {
		http.Error(w, "select 1–32 cursor prefixes", http.StatusBadRequest)
		return
	}
	for _, prefix := range prefixes {
		if prefix == "" {
			http.Error(w, "cursor prefixes must not be empty", http.StatusBadRequest)
			return
		}
	}
	rows := []map[string]any{}
	cursors, err := s.CursorPositions(prefixes, 256)
	if err != nil {
		http.Error(w, "cursor inventory unavailable", http.StatusServiceUnavailable)
		return
	}
	for _, cursor := range cursors {
		wanted := false
		for _, prefix := range prefixes {
			if prefix != "" && strings.HasPrefix(cursor.Name, prefix) {
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
		rows = append(rows, map[string]any{"cursor": cursor.Name, "stream": cursor.Stream, "position": cursor.Position, "head": head, "lag_records": lag})
		if len(rows) > 256 {
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

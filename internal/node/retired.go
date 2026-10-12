package node

import (
	"log/slog"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// purgeRetiredState deletes the retained state of retired contracts from the
// local store before anything reads it: the retained MQTT set, the KV doors and
// the replication of definitions would otherwise keep serving it. It writes no
// stream record, so nothing replicates, and a second start finds nothing.
// Remove it together with uns.IsRetired's entries.
func purgeRetiredState(st *store.Store, log *slog.Logger) (uint64, error) {
	removed, err := st.EvictKV(uns.RetiredTopic)
	if err != nil {
		return 0, err
	}
	if removed > 0 {
		log.Info("removed retained state of retired contracts", "entries", removed)
	}
	return removed, nil
}

package metrics

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/alpamayo-solutions/colca/internal/config"
)

// A scrape carries the store's Pebble statistics, read from the store it serves.
func TestScrapeCarriesThePebbleStatistics(t *testing.T) {
	s := mustStore(t)
	seedRecords(t, s, "metrics", 100)
	m := New(s, config.Retention{}, nil)
	p := s.PebbleMetrics()
	if got := gaugeValue(t, m, "colca_pebble_memtable_bytes", map[string]string{"state": "current"}); got != float64(p.MemTable.Size) || got == 0 {
		t.Fatalf("current memtable bytes = %v, want the store's %d", got, p.MemTable.Size)
	}
	if got := gaugeValue(t, m, "colca_pebble_table_iterators", nil); got != 0 {
		t.Fatalf("a scrape left %v table iterators open", got)
	}
	families, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	series := map[string]int{}
	for _, f := range families {
		series[f.GetName()] = len(f.GetMetric())
	}
	for name, want := range map[string]int{
		"colca_pebble_block_cache_bytes": 1, "colca_pebble_block_cache_hits_total": 1, "colca_pebble_block_cache_misses_total": 1,
		"colca_pebble_memtable_bytes": 2, "colca_pebble_level_bytes": len(p.Levels), "colca_pebble_level_tables": len(p.Levels),
		"colca_pebble_bytes_written_total": 3, "colca_pebble_compaction_debt_bytes": 1, "colca_pebble_compactions_in_progress": 1,
		"colca_pebble_table_iterators": 1, "colca_pebble_tombstones": 1, "colca_pebble_deletion_reclaimable_bytes": 2,
		"colca_pebble_table_stats_complete": 1,
	} {
		if series[name] != want {
			t.Errorf("%s has %d series, want %d", name, series[name], want)
		}
	}
}

// Pebble's ZombieSize wraps below zero while a large batch waits for its flush
// (it logged obsolete=18446744073701163008); the gauge reads 0 then.
func TestObsoleteMemtableBytesDoNotWrap(t *testing.T) {
	for _, tc := range []struct {
		zombie, want uint64
	}{
		{0, 0},
		{8 << 20, 8 << 20},
		{18446744073701163008, 0}, // reserved 8 MiB below Size
	} {
		var m pebble.Metrics
		m.MemTable.ZombieSize = tc.zombie
		if got := obsoleteMemtableBytes(&m); got != tc.want {
			t.Errorf("ZombieSize %d: obsolete %d, want %d", tc.zombie, got, tc.want)
		}
	}
}

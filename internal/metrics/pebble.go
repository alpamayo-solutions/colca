package metrics

import (
	"strconv"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// pebbleCollector exports the storage engine's own statistics: the block cache,
// the memtables, the LSM shape, compaction and deletion tombstones. They are
// what an operator needs to tell a memory or disk problem of the store from one
// of colcad. Every value comes from Pebble's in-memory counters; a scrape reads
// no table.
type pebbleCollector struct{ st *store.Store }

var (
	pebbleCacheBytes    = prometheus.NewDesc("colca_pebble_block_cache_bytes", "Bytes of blocks held in the block cache.", nil, nil)
	pebbleCacheHits     = prometheus.NewDesc("colca_pebble_block_cache_hits_total", "Block cache hits since the store opened.", nil, nil)
	pebbleCacheMisses   = prometheus.NewDesc("colca_pebble_block_cache_misses_total", "Block cache misses since the store opened.", nil, nil)
	pebbleMemtable      = prometheus.NewDesc("colca_pebble_memtable_bytes", "Bytes of memtables: current ones (the mutable one and those queued for flush) and obsolete ones still pinned by an open read or kept for recycling.", []string{"state"}, nil)
	pebbleLevelBytes    = prometheus.NewDesc("colca_pebble_level_bytes", "Bytes of tables per LSM level.", []string{"level"}, nil)
	pebbleLevelTables   = prometheus.NewDesc("colca_pebble_level_tables", "Tables per LSM level.", []string{"level"}, nil)
	pebbleBytesWritten  = prometheus.NewDesc("colca_pebble_bytes_written_total", "Bytes the store wrote since it opened, by kind: the WAL, memtable flushes and compactions.", []string{"kind"}, nil)
	pebbleCompactDebt   = prometheus.NewDesc("colca_pebble_compaction_debt_bytes", "Estimated bytes compaction still has to rewrite to bring the LSM into shape.", nil, nil)
	pebbleCompactions   = prometheus.NewDesc("colca_pebble_compactions_in_progress", "Compactions running now.", nil, nil)
	pebbleTableIters    = prometheus.NewDesc("colca_pebble_table_iterators", "Open table iterators. One read of the store may open several.", nil, nil)
	pebbleTombstones    = prometheus.NewDesc("colca_pebble_tombstones", "Approximate count of deletion tombstones in the tables, point and range deletions together.", nil, nil)
	pebbleDeletionBytes = prometheus.NewDesc("colca_pebble_deletion_reclaimable_bytes", "Estimated table bytes that compaction can reclaim because point or range deletions cover them. Incomplete until colca_pebble_table_stats_complete is 1.", []string{"kind"}, nil)
	pebbleStatsComplete = prometheus.NewDesc("colca_pebble_table_stats_complete", "1 once Pebble has collected the statistics of every table present at open, 0 before.", nil, nil)
)

func (c *pebbleCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		pebbleCacheBytes, pebbleCacheHits, pebbleCacheMisses, pebbleMemtable, pebbleLevelBytes, pebbleLevelTables,
		pebbleBytesWritten, pebbleCompactDebt, pebbleCompactions, pebbleTableIters, pebbleTombstones,
		pebbleDeletionBytes, pebbleStatsComplete,
	} {
		ch <- d
	}
}

func (c *pebbleCollector) Collect(ch chan<- prometheus.Metric) {
	m := c.st.PebbleMetrics()
	gauge := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
	}
	counter := func(d *prometheus.Desc, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v, labels...)
	}
	gauge(pebbleCacheBytes, float64(m.BlockCache.Size))
	counter(pebbleCacheHits, float64(m.BlockCache.Hits))
	counter(pebbleCacheMisses, float64(m.BlockCache.Misses))
	gauge(pebbleMemtable, float64(m.MemTable.Size), "current")
	gauge(pebbleMemtable, float64(obsoleteMemtableBytes(m)), "obsolete")
	var flushed, compacted uint64
	for i, level := range m.Levels {
		label := strconv.Itoa(i)
		gauge(pebbleLevelBytes, float64(level.TablesSize), label)
		gauge(pebbleLevelTables, float64(level.TablesCount), label)
		flushed += level.TableBytesFlushed
		compacted += level.TableBytesCompacted
	}
	counter(pebbleBytesWritten, float64(m.WAL.BytesWritten), "wal")
	counter(pebbleBytesWritten, float64(flushed), "flush")
	counter(pebbleBytesWritten, float64(compacted), "compaction")
	gauge(pebbleCompactDebt, float64(m.Compact.EstimatedDebt))
	gauge(pebbleCompactions, float64(m.Compact.NumInProgress))
	gauge(pebbleTableIters, float64(m.TableIters))
	gauge(pebbleTombstones, float64(m.Keys.TombstoneCount))
	gauge(pebbleDeletionBytes, float64(m.Table.Garbage.PointDeletionsBytesEstimate), "point")
	gauge(pebbleDeletionBytes, float64(m.Table.Garbage.RangeDeletionsBytesEstimate), "range")
	complete := 0.0
	if m.Table.InitialStatsCollectionComplete {
		complete = 1
	}
	gauge(pebbleStatsComplete, complete)
}

// obsoleteMemtableBytes is Pebble's MemTable.ZombieSize without its underflow.
// Pebble computes it as the bytes reserved for memtables minus MemTable.Size in
// unsigned arithmetic, and Size also counts a large batch queued as a flushable
// of its own, which reserves nothing as a memtable. While such a batch waits for
// its flush the difference is negative and ZombieSize wraps to nearly 2^64.
func obsoleteMemtableBytes(m *pebble.Metrics) uint64 {
	if diff := int64(m.MemTable.ZombieSize); diff > 0 { //nolint:gosec // wraps exactly when Pebble's subtraction underflowed
		return uint64(diff)
	}
	return 0
}

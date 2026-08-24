// Package blobgc reclaims blobs nothing references any more (resources
// design §8).
//
// A blob is live iff some live _Resource record names its digest. Everything
// else is deletable — but only after a grace period, because three
// legitimate windows put a blob on disk before the record that references it
// exists: upload-before-upsert at the author, blob-before-entity arrival at
// an ancestor, and pull-before-execute at a provisioning target (§9.1).
// Deleting a blob in any of those windows would destroy a file mid-authoring,
// so an unreferenced blob is only deletable once it is older than the
// configured grace.
package blobgc

import (
	"log/slog"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// Sweeper marks live blobs from the KV and sweeps the blob store on a
// ticker. One instance per node; Run is its only goroutine entry point.
type Sweeper struct {
	blobs *blobstore.Store
	eng   *engine.Engine
	cfg   config.BlobGC
	m     *metrics.Metrics // nil-safe surface: BlobSwept
	ulid  string
	log   *slog.Logger

	// now is a seam: tests age a blob past the grace without sleeping.
	now func() time.Time

	// records is a seam: tests inject a failing read to prove the sweeper
	// never computes liveness from it. Defaults to Engine.ScanContractAll,
	// the one _Resource read that surfaces a storage failure instead of
	// silently answering "nothing found" — uns.EntityStore.KVScanAll cannot
	// be used here for exactly that reason (resources design §8).
	records func() ([]uns.KVRecord, error)
}

// NewSweeper builds a sweeper. ulid is the node's own ULID, used only for
// logging (the sweeper touches no topic that carries an identity of its
// own). m may be nil (every Metrics method is nil-safe).
func NewSweeper(blobs *blobstore.Store, eng *engine.Engine, cfg config.BlobGC, m *metrics.Metrics, ulid string) *Sweeper {
	return &Sweeper{
		blobs: blobs, eng: eng, cfg: cfg, m: m, ulid: ulid,
		log: slog.Default().With("node", ulid, "comp", "blobgc"),
		now: time.Now,
		records: func() ([]uns.KVRecord, error) {
			return eng.ScanContractAll(uns.ResourceContract)
		},
	}
}

// Run sweeps every EffectiveInterval until stop is closed. An EffectiveInterval
// of 0 is the operator's explicit "sweeper disabled" (config.BlobGC's doc
// comment: the same absent-vs-0 precedent as the retention pruner) — Run
// returns immediately and never touches the blob store. A sweep in progress
// always completes before Run returns; the caller's WaitGroup discipline
// (node.Stop waits before closing the store) is what makes that sufficient.
func (s *Sweeper) Run(stop <-chan struct{}) {
	interval := s.cfg.EffectiveInterval()
	if interval <= 0 {
		s.log.Info("blob sweeper disabled (interval 0)")
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			s.runOnce()
		}
	}
}

// runOnce marks the digests every live _Resource references, then deletes
// every stored blob that is neither live nor still inside its grace window.
//
// A failed mark phase must never be read as "nothing is referenced": that
// would delete every unreferenced-looking blob past the grace, including
// ones a working scan would have shown as live. So a records() error skips
// the whole pass, exactly like a blobs.List() failure already does — this
// sweeper deletes nothing on a cycle where it cannot establish liveness.
//
// Known cost, stated rather than hidden: the mark phase scans the whole KV
// each sweep. At this scale that is cheaper than maintaining an index; if
// resource counts grow, this is the first thing to change.
func (s *Sweeper) runOnce() {
	records, err := s.records()
	if err != nil {
		s.log.Error("resource scan failed — sweep skipped this cycle", "err", err)
		return
	}
	live := uns.LiveBlobDigests(records)

	blobs, err := s.blobs.List()
	if err != nil {
		s.log.Error("blob list failed — sweep skipped this cycle", "err", err)
		return
	}

	grace := s.cfg.EffectiveGrace()
	now := s.now()
	for _, b := range blobs {
		if _, ok := live[b.SHA256]; ok {
			continue
		}
		if now.Sub(b.Modified) < grace {
			// Inside the grace window (§8): may be a blob a legitimate
			// upload, replication or pull put here before its record.
			continue
		}
		// blobstore.Delete is already idempotent, so a concurrent delete
		// (another sweep, or the same sha reclaimed elsewhere) races
		// harmlessly.
		if err := s.blobs.Delete(b.SHA256); err != nil {
			s.log.Error("blob delete failed", "sha256", b.SHA256, "err", err)
			continue
		}
		s.m.BlobSwept()
		s.log.Info("blob swept: unreferenced past its grace period",
			"sha256", b.SHA256, "grace", grace)
	}
}

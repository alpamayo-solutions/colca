// Package blobgc reclaims blobs nothing references any more. A blob is live if a
// live _Resource names its digest. Others are deleted only after a grace period,
// because a blob legitimately arrives before its record: uploaded before the
// upsert, replicated before the entity, or pulled before the command runs.
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

	// records returns the _Resource records, Engine.ScanContractAll by default,
	// which reports storage failures instead of answering "nothing found". Tests
	// inject a failing read.
	records func() ([]uns.KVRecord, error)
}

// NewSweeper builds a sweeper. ulid is the node's ULID, used only for logging. m
// may be nil.
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

// Run sweeps every EffectiveInterval until stop is closed; an interval of 0
// disables the sweeper and Run returns at once. A running sweep always completes
// before Run returns.
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

// runOnce marks the digests every live _Resource references, then deletes every
// blob that is neither live nor inside its grace. If the mark phase fails the
// whole pass is skipped, as for a failed List: a failed scan must never read as
// "nothing is referenced". The mark phase scans all of KV each time, which is
// fine at this scale and the first thing to change if resources grow.
func (s *Sweeper) runOnce() {
	// Debris from a process that died mid-Put is reclaimed first and on its own: an
	// unfinished blob has no digest, so nothing can reference it and no mark phase
	// is needed. A failed scan below skips the sweep but not this.
	if removed, err := s.blobs.ReclaimAbandonedTemp(s.now()); err != nil {
		s.log.Error("reclaiming abandoned uploads failed", "err", err)
	} else if removed > 0 {
		s.log.Info("abandoned uploads reclaimed: unfinished blobs left by a process that died mid-transfer",
			"files", removed)
	}

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
			// Inside the grace window: possibly a blob that arrived before its record.
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

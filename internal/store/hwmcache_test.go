package store

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// The marks a parent answers from memory are the durable ones: a reopened store
// reads the same values from disk, and a reader racing the commits never sees a
// mark go backwards (which would make the parent apply a batch twice).
func TestHWMsServedFromMemoryMatchTheDurableMarks(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	const pushes = 200
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		var last uint64
		for {
			select {
			case <-stop:
				return
			default:
			}
			got := s.HWMGet("n-a", "metrics")
			if got < last {
				t.Errorf("HWM went back from %d to %d", last, got)
				return
			}
			last = got
		}
	}()
	for off := uint64(1); off <= pushes; off++ {
		if _, hwm, err := s.ApplyReplicated("n-a", "metrics", []ReplRecord{replRec("n-a", off)}); err != nil || hwm != off {
			t.Fatalf("push %d: hwm %d err %v", off, hwm, err)
		}
		// A redelivered batch applies nothing, also when answered from memory.
		if applied, _, err := s.ApplyReplicated("n-a", "metrics", []ReplRecord{replRec("n-a", off)}); err != nil || len(applied) != 0 {
			t.Fatalf("redelivered push %d applied %d err %v", off, len(applied), err)
		}
	}
	close(stop)
	wg.Wait()
	if got := s.NextOffset("metrics"); got != pushes+1 {
		t.Fatalf("metrics next = %d, want %d", got, pushes+1)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := s.HWMGet("n-a", "metrics"); got != pushes {
		t.Fatalf("HWM after reopen = %d, want %d", got, pushes)
	}
	if applied, _, err := s.ApplyReplicated("n-a", "metrics", []ReplRecord{replRec("n-a", pushes)}); err != nil || len(applied) != 0 {
		t.Fatalf("a batch redelivered after a restart applied %d err %v", len(applied), err)
	}
}

// Pebble's memory follows the node's ceiling: hub-sized only when the node has
// the memory, Pebble's own defaults when the ceiling is small or unknown, and
// never more than 256 MiB each however large the ceiling.
func TestPebbleMemoryFollowsTheNodesCeiling(t *testing.T) {
	for _, tc := range []struct {
		ceiling         int64
		memTable, cache int64
	}{
		{0, 4 << 20, 8 << 20},
		{128 << 20, 4 << 20, 8 << 20},
		{512 << 20, 16 << 20, 16 << 20},
		{2 << 30, 64 << 20, 64 << 20},
		{4 << 30, 128 << 20, 128 << 20},
		{8 << 30, 256 << 20, 256 << 20},
		{64 << 30, 256 << 20, 256 << 20},
	} {
		m, c := SizesFor(tc.ceiling)
		if m != tc.memTable || c != tc.cache {
			t.Errorf("ceiling %d MiB: memtable %d MiB, cache %d MiB; want %d, %d",
				tc.ceiling>>20, m>>20, c>>20, tc.memTable>>20, tc.cache>>20)
		}
		if tc.ceiling >= 512<<20 && cacheSize(m, c) > tc.ceiling/8 {
			t.Errorf("ceiling %d MiB: Pebble may hold %d MiB, over an eighth", tc.ceiling>>20, cacheSize(m, c)>>20)
		}
	}
}

// Pebble reserves its memtables in the block cache, so the cache must be sized
// for them too: with every memtable at full size, a block read twice is a cache
// hit the second time. A cache of the block budget alone, as before, keeps
// nothing at a 2 GiB ceiling. The CPU count matters because Pebble shards the
// cache by GOMAXPROCS and reserves memtables in every shard.
func TestBlocksStayCachedWhileTheMemtablesAreFull(t *testing.T) {
	warmReadHits := func(t *testing.T, cache *pebble.Cache, memTable int64) bool {
		t.Helper()
		release := cache.Reserve(int(memtableReservations * memTable))
		defer release()
		db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem(), Cache: cache, MemTableSize: uint64(memTable)})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		value := bytes.Repeat([]byte("v"), 64<<10) // a block of a few records
		if err := db.Set([]byte("k"), value, pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		read := func() {
			got, closer, err := db.Get([]byte("k"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, value) {
				t.Fatal("read a different value")
			}
			_ = closer.Close()
		}
		read()
		before := db.Metrics().BlockCache
		read()
		after := db.Metrics().BlockCache
		return after.Misses == before.Misses
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	for _, ceiling := range []int64{512 << 20, 2 << 30, 8 << 30} {
		for _, cpus := range []int{1, 4, 11} {
			t.Run(fmt.Sprintf("%dMiB/%dcpus", ceiling>>20, cpus), func(t *testing.T) {
				runtime.GOMAXPROCS(cpus) // Pebble picks the shard count from it
				m, c := SizesFor(ceiling)
				cache := pebble.NewCache(cacheSize(m, c))
				defer cache.Unref()
				if !warmReadHits(t, cache, m) {
					t.Fatalf("a warm read missed the cache of %d MiB with %d MiB of memtables reserved", cacheSize(m, c)>>20, memtableReservations*m>>20)
				}
			})
		}
	}
	t.Run("the block budget alone", func(t *testing.T) {
		runtime.GOMAXPROCS(11)
		m, c := SizesFor(2 << 30)
		cache := pebble.NewCache(c)
		defer cache.Unref()
		if warmReadHits(t, cache, m) {
			t.Fatal("a cache without room for the memtables kept a block; the test does not reproduce the reservation")
		}
	})
}

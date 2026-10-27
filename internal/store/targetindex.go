package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The target index files every command under the identity it is for:
//
//	ti\x00{stream}\x00{target}\x00{offset}  ->  (empty)
//
// A machine's owed commands are what redelivery reads, on every live command
// and on every subscribe. Without the index that read crossed every record of
// the commands stream from the machine's delivery floor: other machines'
// commands, acks, log pages. With it, the read visits this machine's commands
// only. uns.CommandTarget says which records are commands and for whom.
//
// Like the signal index the entry is written with its record and goes with it,
// in the same batch: Prune deletes the entries of the prefix it removes.
// Compaction, eviction and per-signal retention never remove commands. It
// shares the signal index's coverage (sf/{stream}, see sigindex.go): both are
// written by the same appends, and a read below the coverage scans.

// targetKey identifies one command of one target. A target is a ULID, so it
// never contains the separator.
func targetKey(stream, target string, off uint64) []byte {
	return append([]byte("ti\x00"+stream+"\x00"+target+"\x00"), be64(off)...)
}

// recordTarget is the target of a record by its topic, "" for a record that is
// not a command.
func recordTarget(topic string) string {
	target, _ := uns.CommandTarget(topic)
	return target
}

// ReadTarget returns up to limit records in [from, upTo) that are commands for
// target and that filter keeps, in offset order. maxScan bounds the records it
// visits (0: no bound): index entries above the index's coverage, records of
// the stream below it, where the read scans before it switches to the index.
//
// next is where the next read starts: the offset after the last record
// visited, or upTo (capped at the head) when no command for target is left
// before it, so a reader moves over every other record of the stream at once.
func (s *Store) ReadTarget(ctx context.Context, stream, target string, from, upTo uint64, limit, maxScan int, filter func(StoredRecord) bool) (out []StoredRecord, next uint64, err error) {
	s.state.RLock()
	covered, head, lwm := s.sigFrom[stream], s.next[stream], s.lwm[stream]
	snap := s.db.NewSnapshot()
	s.state.RUnlock()
	defer func() { _ = snap.Close() }()
	upTo = min(upTo, head)
	from = max(from, lwm)
	if from >= upTo {
		return nil, max(from, upTo), nil
	}
	scanned := 0
	if from < covered {
		var stopped bool
		page := pageBytes{}
		forTarget := func(topic, _ string) bool { return recordTarget(topic) == target }
		out, next, scanned, stopped, err = readRange(ctx, snap, stream, from, min(covered, upTo), limit, maxScan, &page, forTarget, filter, nil)
		if err != nil || stopped || covered >= upTo {
			return out, next, err
		}
		from = covered
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: targetKey(stream, target, from),
		UpperBound: targetKey(stream, target, upTo),
	})
	if err != nil {
		return nil, from, err
	}
	defer func() { _ = iter.Close() }()
	next = from
	for valid := iter.First(); valid; valid = iter.Next() {
		if len(out) >= limit || (maxScan > 0 && scanned >= maxScan) {
			return out, next, nil
		}
		scanned++
		if scanned%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, next, err
			}
		}
		off, _ := offsetOf(iter.Key())
		next = off + 1
		val, closer, err := snap.Get(streamKey(stream, off))
		if errors.Is(err, pebble.ErrNotFound) {
			continue // an entry its record outlived
		}
		if err != nil {
			return nil, next, err
		}
		var e recEnc
		err = json.Unmarshal(val, &e)
		closer.Close()
		if err != nil {
			return nil, next, fmt.Errorf("decode record %d of %q: %w", off, stream, err)
		}
		if record := storedRecord(off, e); filter == nil || filter(record) {
			out = append(out, record)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, next, err
	}
	if len(out) >= limit {
		return out, next, nil
	}
	return out, upTo, nil
}

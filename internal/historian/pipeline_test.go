package historian

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/door"
)

// stream is a metrics stream for the pipeline tests: signals s0..s(k-1), each
// record a value or (every fifth) a retraction; every seventh record rewrites
// the previous record's key with another value, and every eleventh arrives
// late, with a timestamp before its signal's latest.
type stream struct {
	mu      sync.Mutex
	recs    []door.Record
	cursor  int64 // next offset /fetch reads without From
	acked   []int64
	fetches atomic.Int64
	// failFetch fails the n-th fetch once (1-based), 0 never.
	failFetch int64
}

func newStream(n, signals int) *stream { return newStreamNamed(n, signals, "s") }

// newStreamNamed is newStream with signal ids prefix0..prefix(k-1).
func newStreamNamed(n, signals int, prefix string) *stream {
	s := &stream{cursor: 1}
	ts := int64(1_000)
	for off := int64(1); off <= int64(n); off++ {
		sig := fmt.Sprintf("%s%d", prefix, rand.N(signals))
		value := fmt.Sprint(off)
		switch {
		case off%5 == 0:
			value = "null"
		case off%11 == 0:
			s.recs = append(s.recs, record(off, fmt.Sprintf(`{"signal_id":%q,"value":%d,"timestamp":%d}`,
				sig, off, ts-int64(rand.N(200)))))
			continue
		case off%7 == 0 && off > 1:
			// The same key as the record before, with another value.
			prev := s.recs[len(s.recs)-1]
			s.recs = append(s.recs, record(off, fmt.Sprintf(`{"signal_id":%q,"value":%d,"timestamp":%d}`,
				signalOf(prev), off, tsOf(prev))))
			continue
		}
		ts += 10
		s.recs = append(s.recs, record(off, fmt.Sprintf(`{"signal_id":%q,"value":%s,"timestamp":%d}`, sig, value, ts)))
	}
	return s
}

func signalOf(r door.Record) string {
	row, _ := RowFrom(r.Topic, r.Payload, r.TS)
	return row.SignalID
}

func tsOf(r door.Record) int64 {
	row, _ := RowFrom(r.Topic, r.Payload, r.TS)
	return row.Timestamp.UnixMilli()
}

func (s *stream) head() int64 { return int64(len(s.recs)) }

func (s *stream) serve(from int64, limit int) (door.Page, error) {
	if n := s.fetches.Add(1); n == s.failFetch {
		return door.Page{}, errors.New("colcad restarted")
	}
	var recs []door.Record
	for off := from; off <= s.head() && len(recs) < limit; off++ {
		recs = append(recs, s.recs[off-1])
	}
	return door.Page{Records: recs, From: from, Next: from + int64(len(recs))}, nil
}

func (s *stream) Fetch(_ context.Context, _, _ string, limit int) (door.Page, error) {
	s.mu.Lock()
	from := s.cursor
	s.mu.Unlock()
	return s.serve(from, limit)
}

func (s *stream) FetchWithOptions(_ context.Context, o door.FetchOptions) (door.Page, error) {
	return s.serve(int64(o.From), o.Max) //nolint:gosec // test offsets
}

func (s *stream) Ack(_ context.Context, _, _ string, offset int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acked = append(s.acked, offset)
	s.cursor = offset + 1
	return true, nil
}

func (s *stream) lastAck() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.acked) == 0 {
		return 0
	}
	return s.acked[len(s.acked)-1]
}

type key struct {
	signal string
	ts     int64
}

// table applies rows the way the sink's statements do: a value replaces its
// key; a retraction writes a NULL row only when the latest row at or before it
// holds a value.
type table map[key]*float64

func (t table) apply(row Row) {
	k := key{row.SignalID, row.Timestamp.UnixMilli()}
	if !row.Missing() {
		v := *row.Number
		t[k] = &v
		return
	}
	var latest key
	found := false
	for other := range t {
		if other.signal == k.signal && other.ts <= k.ts && (!found || other.ts > latest.ts) {
			latest, found = other, true
		}
	}
	if found && t[latest] != nil {
		t[k] = nil
	}
}

func (t table) String() string {
	keys := make([]key, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].signal != keys[j].signal {
			return keys[i].signal < keys[j].signal
		}
		return keys[i].ts < keys[j].ts
	})
	out := ""
	for _, k := range keys {
		if t[k] == nil {
			out += fmt.Sprintf("%s@%d=null ", k.signal, k.ts)
		} else {
			out += fmt.Sprintf("%s@%d=%v ", k.signal, k.ts, *t[k])
		}
	}
	return out
}

// sequential is what one-page-at-a-time historisation leaves in the table.
func sequential(s *stream) table {
	t := table{}
	for _, r := range s.recs {
		row, err := RowFrom(r.Topic, r.Payload, r.TS)
		if err != nil {
			panic(err)
		}
		t.apply(row)
	}
	return t
}

// pipeStore is a PipelineStore over a table. It checks, at every marker move,
// that every row at or below the marker is in the table, and records how far
// each partition got ahead of the marker.
type pipeStore struct {
	stream     *stream
	partitions int

	mu        sync.Mutex
	table     table
	marker    int64
	consumers map[string]int64 // the partitions' markers
	written   map[int64]bool   // offsets whose rows committed
	markers   []int64
	slow      int           // this partition's writes take slowFor
	slowFor   time.Duration //
	ahead     int64         // most offsets ever committed past the marker
	failShare int           // the failShare-th share write fails once (1-based)
	lostShare int           // the lostShare-th share write commits but reports failure
	shares    int
	failMark  int // the failMark-th marker write fails once
	marks     int
	inFlight  atomic.Int64 // shares being written at once
	overlap   atomic.Int64 // most shares ever written at once
	bad       error
}

func newPipeStore(s *stream, partitions int) *pipeStore {
	return &pipeStore{stream: s, partitions: partitions, table: table{}, written: map[int64]bool{}, consumers: map[string]int64{}}
}

func (p *pipeStore) Applied(_ context.Context, consumer string) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if consumer == Consumer {
		return p.marker, nil
	}
	return p.consumers[consumer], nil
}

func (p *pipeStore) Apply(context.Context, []Row, string, int64) ([]Rejection, error) {
	return nil, errors.New("the pipeline writes shares, not pages")
}

func (p *pipeStore) Partitions() int { return p.partitions }

func (p *pipeStore) ApplyShare(ctx context.Context, rows []Row, consumer string, through int64) ([]Rejection, error) {
	n := p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	for {
		old := p.overlap.Load()
		if n <= old || p.overlap.CompareAndSwap(old, n) {
			break
		}
	}
	if len(rows) > 0 && partitionOf(rows[0].SignalID, p.partitions) == p.slow && p.slowFor > 0 {
		select {
		case <-time.After(p.slowFor):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shares++
	if p.shares == p.failShare {
		return nil, errors.New("connection reset")
	}
	part := -1
	for _, row := range rows {
		if q := partitionOf(row.SignalID, p.partitions); part >= 0 && q != part && p.bad == nil {
			p.bad = fmt.Errorf("one write held partitions %d and %d", part, q)
		} else {
			part = q
		}
		if p.written[row.Offset] && p.bad == nil {
			p.bad = fmt.Errorf("offset %d was written twice", row.Offset)
		}
		p.table.apply(row)
		p.written[row.Offset] = true
		if ahead := row.Offset - p.marker; ahead > p.ahead {
			p.ahead = ahead
		}
	}
	if through < p.consumers[consumer] && p.bad == nil {
		p.bad = fmt.Errorf("%s went back from %d to %d", consumer, p.consumers[consumer], through)
	}
	p.consumers[consumer] = through
	if p.shares == p.lostShare {
		return nil, errors.New("connection reset while committing")
	}
	return nil, nil
}

func (p *pipeStore) Mark(_ context.Context, _ string, offset int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.marks++
	if p.marks == p.failMark {
		return errors.New("marker write failed")
	}
	if offset < p.marker && p.bad == nil {
		p.bad = fmt.Errorf("the marker went back from %d to %d", p.marker, offset)
	}
	// Every row at or below the marker must be written: the marker is the
	// promise a restart relies on.
	for off := p.marker + 1; off <= offset; off++ {
		if !p.written[off] && p.bad == nil {
			p.bad = fmt.Errorf("the marker moved to %d before offset %d was written", offset, off)
		}
	}
	p.marker = offset
	p.markers = append(p.markers, offset)
	return nil
}

func pipelineBridge(s *stream, store *pipeStore, pages, pageSize int) *Bridge {
	var signal door.Signal
	return &Bridge{Door: s, Store: store, Max: pageSize, Pipeline: pages, Changes: signal.Changes}
}

// runToHead runs the bridge until the stream's head is acked.
func runToHead(t *testing.T, b *Bridge, s *stream) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for s.lastAck() != s.head() {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("acked %d of %d", s.lastAck(), s.head())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
}

func checkStore(t *testing.T, s *stream, store *pipeStore) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.bad != nil {
		t.Fatal(store.bad)
	}
	if got, want := store.table.String(), sequential(s).String(); got != want {
		t.Fatalf("the table differs from a one-page-at-a-time run\n got %s\nwant %s", got, want)
	}
	if store.marker != s.head() {
		t.Fatalf("marker %d, want %d", store.marker, s.head())
	}
}

// A backlog written several pages at once leaves the table a one-page-at-a-
// time run leaves, with values, retractions and rewritten keys, and the
// marker never passes an unwritten row.
func TestAPipelinedBacklogLandsAsOnePageAtATimeWould(t *testing.T) {
	for _, partitions := range []int{1, 4} {
		t.Run(fmt.Sprintf("partitions=%d", partitions), func(t *testing.T) {
			s := newStream(2000, 23)
			store := newPipeStore(s, partitions)
			runToHead(t, pipelineBridge(s, store, 4, 50), s)
			checkStore(t, s, store)
		})
	}
}

// A slow partition does not hold the others: they write pages ahead of it,
// up to the pipeline's bound, while the marker waits for the slow one.
func TestAFastPartitionWritesAheadOfASlowOneAndTheMarkerWaits(t *testing.T) {
	s := newStream(600, 31)
	store := newPipeStore(s, 4)
	store.slow, store.slowFor = 0, 5*time.Millisecond
	runToHead(t, pipelineBridge(s, store, 4, 50), s)
	checkStore(t, s, store)
	if store.overlap.Load() < 2 {
		t.Fatalf("at most %d shares were written at once", store.overlap.Load())
	}
	// Four pages of 50 are the most that may be past the marker.
	if store.ahead <= 50 || store.ahead > 4*50 {
		t.Fatalf("writes ran at most %d offsets past the marker, want past one page and within four", store.ahead)
	}
}

// A failed write is retried in place: nothing is skipped, the marker does not
// pass it meanwhile, and the table ends as a sequential run.
func TestAFailedShareIsRetriedAndTheMarkerWaitsForIt(t *testing.T) {
	s := newStream(400, 11)
	store := newPipeStore(s, 4)
	store.failShare = 3
	b := pipelineBridge(s, store, 4, 25)
	var unhealthy atomic.Int64
	b.Health = func(ok bool, _ string) {
		if !ok {
			unhealthy.Add(1)
		}
	}
	runToHead(t, b, s)
	checkStore(t, s, store)
	if unhealthy.Load() == 0 {
		t.Fatal("a failed write was not reported to health")
	}
}

// A failed marker write and a failed fetch are retried; nothing is lost.
func TestAFailedMarkerOrFetchIsRetried(t *testing.T) {
	s := newStream(300, 7)
	s.failFetch = 3
	store := newPipeStore(s, 2)
	store.failMark = 2
	runToHead(t, pipelineBridge(s, store, 3, 20), s)
	checkStore(t, s, store)
}

// A crash between commit and ack — rows of later pages written, the marker
// and the cursor behind them — replays from the marker on the next start.
// Each partition's own marker skips what it wrote, so no row is applied
// twice (the store fails the test if one is) and the table ends exactly as a
// sequential run leaves it, late data and retractions included.
func TestACrashBetweenCommitAndAckReplaysToTheSameTable(t *testing.T) {
	for attempt := range 20 {
		s := newStream(1500, 13)
		store := newPipeStore(s, 4)
		store.slow, store.slowFor = attempt%4, time.Millisecond
		b := pipelineBridge(s, store, 4, 40)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- b.Run(ctx) }()
		// Crash somewhere in the drain.
		time.Sleep(time.Duration(2+attempt) * time.Millisecond)
		cancel()
		<-done
		store.mu.Lock()
		marker, bad := store.marker, store.bad
		store.mu.Unlock()
		if bad != nil {
			t.Fatal(bad)
		}
		if acked := s.lastAck(); acked > marker {
			t.Fatalf("acked %d past the marker %d", acked, marker)
		}
		// The next start reads from the cursor (at or below the marker).
		store.slowFor = 0
		runToHead(t, pipelineBridge(s, store, 4, 40), s)
		checkStore(t, s, store)
	}
}

// The pipeline holds at most Pipeline pages: memory is bounded, and so is
// what a restart replays.
func TestThePipelineHoldsAtMostItsPages(t *testing.T) {
	s := newStream(1000, 9)
	store := newPipeStore(s, 3)
	store.slow, store.slowFor = 1, 2*time.Millisecond
	runToHead(t, pipelineBridge(s, store, 2, 30), s)
	checkStore(t, s, store)
	if store.ahead > 2*30 {
		t.Fatalf("writes ran %d offsets past the marker, want within two pages of 30", store.ahead)
	}
}

// Strict (coordinated) history and a door that cannot read ahead keep the
// one-page-at-a-time path.
func TestThePipelineIsOnlyUsedWhereItCanBe(t *testing.T) {
	s := newStream(10, 2)
	store := newPipeStore(s, 2)
	if _, ok := (&Bridge{Door: s, Store: store, Pipeline: 4}).pipelined(); !ok {
		t.Fatal("not pipelined with a read-ahead door and a pipeline store")
	}
	if _, ok := (&Bridge{Door: s, Store: store, Pipeline: 4, Strict: true}).pipelined(); ok {
		t.Fatal("strict history was pipelined")
	}
	if _, ok := (&Bridge{Door: s, Store: store, Pipeline: 1}).pipelined(); ok {
		t.Fatal("Pipeline 1 was pipelined")
	}
	if _, ok := (&Bridge{Door: &fakeDoor{}, Store: store, Pipeline: 4}).pipelined(); ok {
		t.Fatal("a door without read-ahead was pipelined")
	}
	if _, ok := (&Bridge{Door: s, Store: &fakeStore{}, Pipeline: 4}).pipelined(); ok {
		t.Fatal("a store without partitions was pipelined")
	}
}

// A write that committed but whose answer was lost is not written again:
// re-applying it would put its retractions after the rows behind them. The
// store fails the test if any offset is written twice.
func TestACommitWhoseAnswerWasLostIsNotWrittenAgain(t *testing.T) {
	s := newStream(400, 11)
	store := newPipeStore(s, 4)
	store.lostShare = 3
	runToHead(t, pipelineBridge(s, store, 4, 25), s)
	checkStore(t, s, store)
}

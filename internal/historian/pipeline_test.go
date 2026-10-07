package historian

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
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
	id      string // the store incarnation, as /fetch names it
	recs    []door.Record
	cursor  int64 // next offset /fetch reads without From
	acked   []int64
	fetches atomic.Int64
	// failFetch fails the n-th fetch once (1-based), 0 never.
	failFetch int64
	// Acks of an offset in [failAckFrom, failAckTo] fail; failAcks more acks
	// fail after that, whatever the offset.
	failAckFrom, failAckTo int64
	failAcks               int
	// staleAcks counts acks that named another store than the current one;
	// ackCalls every ack, blankAcks those that named no store.
	staleAcks, ackCalls, blankAcks int
}

func newStream(n, signals int) *stream { return newStreamNamed(n, signals, "s") }

// newStreamNamed is newStream with signal ids prefix0..prefix(k-1).
func newStreamNamed(n, signals int, prefix string) *stream {
	s := &stream{cursor: 1, id: newStoreID()}
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

func newStoreID() string { return fmt.Sprintf("store-%016x", rand.Uint64()) }

func signalOf(r door.Record) string {
	row, _ := RowFrom(r.Topic, r.Payload, r.TS)
	return row.SignalID
}

func tsOf(r door.Record) int64 {
	row, _ := RowFrom(r.Topic, r.Payload, r.TS)
	return row.Timestamp.UnixMilli()
}

func (s *stream) head() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.recs))
}

// replace gives the node a new stream, as when colcad's data volume is
// recreated: new records from offset 1 and a cursor that starts there.
func (s *stream) replace(recs []door.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs, s.cursor, s.acked, s.id = recs, 1, nil, newStoreID()
}

// grow appends records to the stream, numbered on from its head.
func (s *stream) grow(more []door.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range more {
		r.Offset = int64(len(s.recs)) + 1
		s.recs = append(s.recs, r)
	}
}

func (s *stream) serve(from int64, limit int) (door.Page, error) {
	if n := s.fetches.Add(1); n == s.failFetch {
		return door.Page{}, errors.New("colcad restarted")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var recs []door.Record
	for off := from; off <= int64(len(s.recs)) && len(recs) < limit; off++ {
		recs = append(recs, s.recs[off-1])
	}
	return door.Page{Records: recs, From: from, Next: from + int64(len(recs)), Store: s.id}, nil
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

// AckStore is colcad's /ack: an ack for another store is refused as "store
// changed", one past the head as such; then the fault injection.
func (s *stream) AckStore(_ context.Context, _, _ string, offset int64, store string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackCalls++
	if store == "" {
		s.blankAcks++
	}
	if store != "" && store != s.id {
		s.staleAcks++
		return false, fmt.Errorf("acking at %d: %w", offset, door.ErrStoreChanged)
	}
	if offset > int64(len(s.recs)) {
		return false, fmt.Errorf("offset %d is past the head %d", offset, len(s.recs))
	}
	if offset >= s.failAckFrom && offset <= s.failAckTo {
		return false, errors.New("colcad restarting")
	}
	if s.failAcks > 0 {
		s.failAcks--
		return false, errors.New("colcad restarting")
	}
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
	store     string           // the store the page marker is a position in
	resets    int
	// fresh is set by newStream: the next marker may go back, once.
	fresh     bool
	written   map[int64]bool // offsets whose rows committed
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

// Apply is the one-page path (PIPELINE_PAGES=1): rows and the page marker
// together.
func (p *pipeStore) Apply(_ context.Context, rows []Row, _ string, offset int64, store string) ([]Rejection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, row := range rows {
		if p.written[row.Offset] && p.bad == nil {
			p.bad = fmt.Errorf("offset %d was written twice", row.Offset)
		}
		p.table.apply(row)
		p.written[row.Offset] = true
	}
	p.marker, p.store = offset, store
	return nil, nil
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

func (p *pipeStore) PartitionMarkers(_ context.Context, consumer string) (map[string]int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.partitionsLocked(consumer), nil
}

func (p *pipeStore) partitionsLocked(consumer string) map[string]int64 {
	out := map[string]int64{}
	for name, m := range p.consumers {
		if strings.HasPrefix(name, consumer+"/") {
			out[name] = m
		}
	}
	return out
}

func (p *pipeStore) ResetPartitionMarkers(_ context.Context, consumer string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for name := range p.partitionsLocked(consumer) {
		p.consumers[name] = 0
	}
	p.resets++
	return nil
}

func (p *pipeStore) AppliedStore(context.Context, string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.store, nil
}

func (p *pipeStore) Mark(_ context.Context, _ string, offset int64, store string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.marks++
	if p.marks == p.failMark {
		return errors.New("marker write failed")
	}
	if offset < p.marker && p.fresh {
		p.marker, p.fresh = 0, false
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
	p.marker, p.store = offset, store
	p.markers = append(p.markers, offset)
	return nil
}

// newStream points the store's checks at a recreated stream: offsets start
// again at 1. The table and the markers in the database stay as they are.
func (p *pipeStore) newStream(s *stream) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stream, p.written, p.fresh = s, map[int64]bool{}, true
}

func (p *pipeStore) partitionMarkers() map[string]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.partitionsLocked(Consumer)
}

// oracle is what one page at a time leaves after each stream in turn.
func oracle(streams ...[]door.Record) table {
	t := table{}
	for _, recs := range streams {
		for _, r := range recs {
			row, err := RowFrom(r.Topic, r.Payload, r.TS)
			if err != nil {
				panic(err)
			}
			t.apply(row)
		}
	}
	return t
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

// colcad's volume is recreated while the historian is stopped. The new stream
// first has samples of one signal only, so most writers get no rows; their
// partition markers must not keep the old stream's offsets, or a restart
// skips their rows of the new one. The first write after the reset fails and
// must be written again, not taken for committed because of an old marker.
func TestANewStreamResetsEveryPartitionMarker(t *testing.T) {
	old := newStreamNamed(1000, 7, "a")
	store := newPipeStore(old, 4)
	runToHead(t, pipelineBridge(old, store, 4, 50), old)
	for name, m := range store.partitionMarkers() {
		if m < 900 {
			t.Fatalf("%s at %d after the old stream; the test needs every partition well into it", name, m)
		}
	}
	oldRecs := old.recs

	fresh := newStreamNamed(200, 1, "b")
	s := &stream{cursor: 1}
	s.replace(fresh.recs)
	store.newStream(s)
	store.failShare = store.shares + 1
	runToHead(t, pipelineBridge(s, store, 4, 50), s)
	for name, m := range store.partitionMarkers() {
		if m > 200 {
			t.Fatalf("%s still at %d of the old stream after the reset", name, m)
		}
	}

	// More signals, so every writer gets rows below the old markers; then a
	// restart.
	more := newStreamNamed(1000, 9, "c")
	s.grow(more.recs)
	runToHead(t, pipelineBridge(s, store, 4, 50), s)

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.bad != nil {
		t.Fatal(store.bad)
	}
	for off := int64(1); off <= s.head(); off++ {
		if !store.written[off] {
			t.Fatalf("offset %d of the new stream was never written", off)
		}
	}
	if got, want := store.table.String(), oracle(oldRecs, s.recs).String(); got != want {
		t.Fatalf("the table differs from one page at a time\n got %s\nwant %s", got, want)
	}
}

// colcad's volume is recreated while the historian runs at the head. /fetch
// from the historian's next offset finds nothing in the new stream; the
// historian goes back to the cursor and historises the new stream from its
// first record.
func TestANewStreamWhileRunningIsReadFromTheCursor(t *testing.T) {
	old := newStreamNamed(600, 5, "a")
	store := newPipeStore(old, 4)
	var signal door.Signal
	b := &Bridge{Door: old, Store: store, Max: 50, Pipeline: 4, Changes: signal.Changes}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	waitAcked := func(s *stream, what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for s.lastAck() != s.head() {
			if time.Now().After(deadline) {
				cancel()
				t.Fatalf("%s: acked %d of %d", what, s.lastAck(), s.head())
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitAcked(old, "old stream")
	oldRecs := old.recs

	fresh := newStreamNamed(450, 8, "b")
	store.newStream(old)
	old.replace(fresh.recs)
	signal.Notify()
	waitAcked(old, "new stream")
	cancel()
	<-done

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.bad != nil {
		t.Fatal(store.bad)
	}
	if store.resets == 0 {
		t.Fatal("the partition markers were not reset for the new stream")
	}
	if got, want := store.table.String(), oracle(oldRecs, fresh.recs).String(); got != want {
		t.Fatalf("the table differs from one page at a time\n got %s\nwant %s", got, want)
	}
}

// A node too old to send store ids, whose acks go through, has its volume
// recreated while the historian runs: the page from offset 1 under the marker
// is a new stream (no ack is owed, so it is not the historian's own lagging
// cursor), and every record of it is written.
func TestAnOldNodeRecreatedWhileRunningLosesNothing(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			old := newStreamNamed(600, 5, "a")
			old.id = ""
			store := newPipeStore(old, 4)
			var signal door.Signal
			b := &Bridge{Door: old, Store: store, Max: 50, Pipeline: pages, Changes: signal.Changes}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- b.Run(ctx) }()
			waitAcked := func(what string) {
				t.Helper()
				deadline := time.Now().Add(10 * time.Second)
				for old.lastAck() != old.head() {
					if time.Now().After(deadline) {
						cancel()
						t.Fatalf("%s: acked %d of %d", what, old.lastAck(), old.head())
					}
					time.Sleep(time.Millisecond)
				}
			}
			waitAcked("old stream")
			oldRecs := old.recs

			fresh := newStreamNamed(450, 8, "b")
			store.newStream(old)
			old.replace(fresh.recs)
			old.mu.Lock()
			old.id = ""
			old.mu.Unlock()
			signal.Notify()
			waitAcked("new stream")
			cancel()
			<-done

			store.mu.Lock()
			defer store.mu.Unlock()
			if store.bad != nil {
				t.Fatal(store.bad)
			}
			for off := int64(1); off <= old.head(); off++ {
				if !store.written[off] {
					t.Fatalf("offset %d of the new stream was never written", off)
				}
			}
			if got, want := store.table.String(), oracle(oldRecs, fresh.recs).String(); got != want {
				t.Fatalf("the table differs from one page at a time\n got %s\nwant %s", got, want)
			}
		})
	}
}

// After WRITERS changed (or an unclean stop of a run with another count), the
// smallest marker of a complete set of the old partitions is a point every
// row at or below has been written; an incomplete set says nothing.
func TestStartMarkersUseACompleteSetOfAnotherWriterCount(t *testing.T) {
	applied, own := startMarkers(100, map[string]int64{
		partitionConsumer(2, 0): 300, partitionConsumer(2, 1): 250, // complete: floor 250
		partitionConsumer(3, 0): 900, // incomplete: ignored
		partitionConsumer(4, 1): 180, partitionConsumer(4, 3): 220,
		"historian:metrics/x.y": 999,
	}, 4)
	if applied != 250 {
		t.Fatalf("start at %d, want 250", applied)
	}
	if fmt.Sprint(own) != "[0 180 0 220]" {
		t.Fatalf("own markers %v", own)
	}
}

// The one-page path (PIPELINE_PAGES=1) also zeroes the partition markers when
// it finds a new stream, so a later pipelined run does not trust old ones; and
// it starts after a complete set of a pipelined run's partition markers.
func TestTheOnePagePathResetsAndUsesThePartitionMarkers(t *testing.T) {
	fresh := newStreamNamed(30, 3, "b")
	store := newPipeStore(fresh, 2)
	store.marker = 1000
	store.consumers = map[string]int64{partitionConsumer(4, 0): 1000, partitionConsumer(4, 2): 990}
	b := &Bridge{Door: fresh, Store: store, Max: 50}
	if _, err := b.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.resets != 1 || store.consumers[partitionConsumer(4, 0)] != 0 {
		t.Fatalf("resets %d, markers %v", store.resets, store.consumers)
	}
	if !store.written[1] || store.marker != 30 {
		t.Fatalf("the new stream was not written from its start (marker %d)", store.marker)
	}

	s := newStreamNamed(100, 3, "c")
	store = newPipeStore(s, 2)
	store.marker = 20
	store.consumers = map[string]int64{partitionConsumer(2, 0): 60, partitionConsumer(2, 1): 45}
	s.cursor = 21
	b = &Bridge{Door: s, Store: store, Max: 50}
	if _, err := b.Once(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.written[45] || !store.written[46] {
		t.Fatalf("the one-page path did not start after the partitions' floor 45: %v", store.written)
	}
}

// A complete set of another writer count's markers past the page marker is
// a start point, not a sign of a new stream: nothing is reset, the rows that
// set wrote are not written again, and the rest are.
func TestAStartAfterAnotherWriterCountSkipsWhatItWrote(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			s := newStreamNamed(400, 6, "a")
			store := newPipeStore(s, 4)
			// An earlier run with two writers wrote everything up to 300
			// (one partition to 310) but moved the page marker only to 20.
			for off := int64(1); off <= 300; off++ {
				row, _ := RowFrom(s.recs[off-1].Topic, s.recs[off-1].Payload, s.recs[off-1].TS)
				row.Offset = off
				store.table.apply(row)
				store.written[off] = true
			}
			store.marker = 20
			store.consumers = map[string]int64{partitionConsumer(2, 0): 300, partitionConsumer(2, 1): 310}
			s.cursor = 21
			runToHead(t, pipelineBridge(s, store, pages, 50), s)
			checkStore(t, s, store)
			if store.resets != 0 {
				t.Fatal("the markers were reset; the stream is the same")
			}
		})
	}
}

// Acks fail for a while (colcad restarting mid-drain) while the marks go in,
// so the cursor falls several pages behind the marker; then a fetch fails and
// the historian reads from the cursor again. A lagging cursor is not a new
// stream: nothing is reset, nothing is written twice, the marker never goes
// back, and the cursor catches up.
func TestALaggingCursorIsNotANewStream(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			s := newStreamNamed(1000, 7, "a")
			s.failAckFrom, s.failAckTo = 200, 800
			s.failFetch = 14 // offset ~650 with pages of 50
			store := newPipeStore(s, 4)
			runToHead(t, pipelineBridge(s, store, pages, 50), s)
			checkStore(t, s, store)
			if store.resets != 0 {
				t.Fatalf("the markers were reset %d times for a lagging cursor", store.resets)
			}
		})
	}
}

// An ack that fails at the head is retried with backoff although no new data
// arrives to carry the next one.
func TestAFailedAckAtTheHeadIsRetried(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) { failedAckAtTheHead(t, pages) })
	}
}

func failedAckAtTheHead(t *testing.T, pages int) {
	s := newStreamNamed(120, 3, "a")
	store := newPipeStore(s, 2)
	var signal door.Signal
	b := &Bridge{Door: s, Store: store, Max: 200, Pipeline: pages, Changes: signal.Changes}
	s.failAcks = 2
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for s.lastAck() != s.head() {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("acked %d of %d, never retried", s.lastAck(), s.head())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	checkStore(t, s, store)
}

// A node too old to send a store id: an ack that fails at the head is not
// retried, since a blank store would pass the node's store check whatever
// store the node has by then. The next applied page carries the ack.
func TestAnOwedAckWithoutAStoreIDIsNotRetried(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			s := newStreamNamed(120, 3, "a")
			s.id = ""                              // an old colcad: /fetch names no store
			s.failAckFrom, s.failAckTo = 101, 1000 // the acks at the head fail
			store := newPipeStore(s, 2)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			b := pipelineBridge(s, store, pages, 50)
			b.Head = func() int64 { return s.head() + 1 } // as the node's hints announce it
			go func() { done <- b.Run(ctx) }()
			deadline := time.Now().Add(10 * time.Second)
			for {
				store.mu.Lock()
				marker := store.marker
				store.mu.Unlock()
				if marker == s.head() {
					break
				}
				if time.Now().After(deadline) {
					cancel()
					t.Fatalf("marked %d of %d", marker, s.head())
				}
				time.Sleep(time.Millisecond)
			}
			s.mu.Lock()
			calls := s.ackCalls
			s.mu.Unlock()
			time.Sleep(2500 * time.Millisecond) // past the first retries (1 s, 2 s)
			cancel()
			<-done
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.ackCalls != calls {
				t.Fatalf("%d acks without a store were retried", s.ackCalls-calls)
			}
		})
	}
}

// A node too old to send a store id, whose acks fail from the first page on:
// its cursor stays at 1 under the marker this bridge wrote. That is the
// bridge's own cursor lagging, not a recreated store, so the first page is not
// re-applied over and over; and while the ack is owed, re-reading from the
// cursor backs off instead of running as fast as the node answers.
func TestAnOldNodeWhoseAcksFailIsNotReadInALoop(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			s := newStreamNamed(120, 3, "a")
			s.id = ""
			s.failAcks = 1 << 30
			store := newPipeStore(s, 2)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			// No Head: before the node's first hint, or a node that announces none.
			go func() { done <- pipelineBridge(s, store, pages, 50).Run(ctx) }()
			time.Sleep(3 * time.Second)
			cancel()
			<-done
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.resets != 0 {
				t.Fatalf("the markers were reset %d times", store.resets)
			}
			// Three pages to the head, then a retry after 1 s and 2 s: a
			// handful of fetches, not thousands.
			if n := s.fetches.Load(); n > 12 {
				t.Fatalf("%d fetches in 3 s while the ack was owed", n)
			}
			if store.bad != nil {
				t.Fatal(store.bad)
			}
		})
	}
}

// An upgrade: the marker is from before store ids (it names no store) and the
// cursor lags it. The ack that moves the cursor up fails and is owed. Until a
// page is marked with its store a retry would name no store, which the node
// takes for any store, so it is not retried; after a restart with new data the
// ack goes through naming the store.
func TestAnOwedAckBeforeTheFirstMarkNamesAStore(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			s := newStreamNamed(600, 7, "a")
			store := newPipeStore(s, 4)
			// The old historian wrote all 600 and marked them, store unknown;
			// its acks failed from 101 on, so the cursor is at 101.
			for _, r := range s.recs {
				row, err := RowFrom(r.Topic, r.Payload, r.TS)
				if err != nil {
					t.Fatal(err)
				}
				store.table.apply(row)
				store.written[r.Offset] = true
			}
			store.marker = 600
			s.cursor = 101
			s.failAckFrom, s.failAckTo = 1, 1<<30
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			b := pipelineBridge(s, store, pages, 50)
			b.Head = func() int64 { return s.head() + 1 }
			go func() { done <- b.Run(ctx) }()
			time.Sleep(2500 * time.Millisecond) // past the first retries (1 s, 2 s)
			cancel()
			<-done

			s.mu.Lock()
			s.failAckFrom, s.failAckTo = 0, 0
			s.mu.Unlock()
			s.grow(newStreamNamed(100, 7, "a").recs)
			runToHead(t, pipelineBridge(s, store, pages, 50), s)
			s.mu.Lock()
			blank := s.blankAcks
			s.mu.Unlock()
			if blank != 0 {
				t.Fatalf("%d acks named no store", blank)
			}
			checkStore(t, s, store)
		})
	}
}

// A new stream that already grew past the marker before the historian looked:
// the marker is an offset in it too, but its pages come from another store
// than the one the marker records, so the stream is new.
func TestANewStreamThatGrewPastTheMarkerIsNew(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			old := newStreamNamed(300, 5, "a")
			store := newPipeStore(old, 4)
			runToHead(t, pipelineBridge(old, store, pages, 50), old)
			oldRecs := old.recs

			fresh := newStreamNamed(900, 6, "b")
			s := &stream{cursor: 1}
			s.replace(fresh.recs)
			store.newStream(s)
			runToHead(t, pipelineBridge(s, store, pages, 50), s)
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.bad != nil {
				t.Fatal(store.bad)
			}
			if store.resets != 1 {
				t.Fatalf("reset %d times, want once", store.resets)
			}
			for off := int64(1); off <= s.head(); off++ {
				if !store.written[off] {
					t.Fatalf("offset %d of the new stream was never written", off)
				}
			}
			if got, want := store.table.String(), oracle(oldRecs, fresh.recs).String(); got != want {
				t.Fatalf("the table differs from one page at a time\n got %s\nwant %s", got, want)
			}
		})
	}
}

// colcad's store is recreated while acks are owed: every ack of the old
// stream failed, so the marker is at 600 and the cursor at 1. The owed ack is
// retried and must not land on the new store (it would move the new cursor to
// 601 and the new stream's 450 records would count as read). The node refuses
// it as "store changed", and the historian reads the new store from its
// cursor and writes all of it. With 900 new records the old marker is inside
// the new stream, so only the store id tells them apart.
func TestARecreatedStoreWithAnOwedAckLosesNothing(t *testing.T) {
	for _, tc := range []struct{ pages, fresh int }{{1, 450}, {4, 450}, {1, 900}, {4, 900}} {
		pages := tc.pages
		t.Run(fmt.Sprintf("pages=%d,new=%d", tc.pages, tc.fresh), func(t *testing.T) {
			s := newStreamNamed(600, 5, "a")
			s.failAckFrom, s.failAckTo = 1, 600
			store := newPipeStore(s, 4)
			var signal door.Signal
			b := &Bridge{Door: s, Store: store, Max: 50, Pipeline: pages, Changes: signal.Changes}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- b.Run(ctx) }()
			deadline := time.Now().Add(10 * time.Second)
			for {
				store.mu.Lock()
				marker := store.marker
				store.mu.Unlock()
				if marker == 600 {
					break
				}
				if time.Now().After(deadline) {
					cancel()
					t.Fatalf("marker at %d, never reached the old head", marker)
				}
				time.Sleep(time.Millisecond)
			}
			oldRecs := s.recs

			fresh := newStreamNamed(tc.fresh, 6, "b")
			store.newStream(s)
			s.mu.Lock()
			s.failAckFrom, s.failAckTo = 0, -1
			s.mu.Unlock()
			s.replace(fresh.recs)
			// Let the owed ack be retried against the new store first, where
			// the retry timer gets there before the next read.
			for wait := time.Now().Add(3 * time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
				s.mu.Lock()
				stale := s.staleAcks
				s.mu.Unlock()
				if stale > 0 {
					break
				}
			}
			signal.Notify()
			deadline = time.Now().Add(20 * time.Second)
			for s.lastAck() != s.head() {
				if time.Now().After(deadline) {
					cancel()
					t.Fatalf("the new store: acked %d of %d (cursor %d)", s.lastAck(), s.head(), s.cursor)
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			<-done

			store.mu.Lock()
			defer store.mu.Unlock()
			if store.bad != nil {
				t.Fatal(store.bad)
			}
			for off := int64(1); off <= int64(tc.fresh); off++ {
				if !store.written[off] {
					t.Fatalf("offset %d of the new store was never written", off)
				}
			}
			if store.resets != 1 {
				t.Fatalf("reset %d times, want once", store.resets)
			}
			if got, want := store.table.String(), oracle(oldRecs, fresh.recs).String(); got != want {
				t.Fatalf("the table differs from one page at a time\n got %s\nwant %s", got, want)
			}
		})
	}
}

// A marker written before stores were recorded: the first page from the
// cursor decides by the old rules, once (the same stream here: nothing is
// reset), and the marker records the store from then on.
func TestAMarkerWithoutAStoreIsUpgraded(t *testing.T) {
	for _, pages := range []int{1, 4} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			s := newStreamNamed(400, 5, "a")
			store := newPipeStore(s, 2)
			for off := int64(1); off <= 100; off++ {
				row, _ := RowFrom(s.recs[off-1].Topic, s.recs[off-1].Payload, s.recs[off-1].TS)
				row.Offset = off
				store.table.apply(row)
				store.written[off] = true
			}
			store.marker = 100
			s.cursor = 101
			runToHead(t, pipelineBridge(s, store, pages, 50), s)
			checkStore(t, s, store)
			if store.resets != 0 {
				t.Fatal("a marker without a store was reset on the same stream")
			}
			if store.store != s.id {
				t.Fatalf("the marker records store %q, want %q", store.store, s.id)
			}
		})
	}
}

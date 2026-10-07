package historian

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alpamayo-solutions/colca/door"
)

// PipelineStore is a Store that writes one partition of the signals without
// the marker and moves the marker on its own, which is what lets the bridge
// write several pages at once (runPipelined).
type PipelineStore interface {
	Store
	// Partitions is how many writers write at once. A signal always belongs to
	// the same one (partitionOf), so its rows are written in stream order.
	Partitions() int
	// ApplyShare writes rows of one partition, and moves that partition's own
	// marker (consumer) to through, in one transaction. Rejections are rows the
	// schema permanently refused; an error means nothing was written.
	ApplyShare(ctx context.Context, rows []Row, consumer string, through int64) ([]Rejection, error)
	// Mark moves the consumer's marker to offset.
	Mark(ctx context.Context, consumer string, offset int64) error
	// PartitionMarkers returns every partition marker of consumer, by name,
	// whatever writer count wrote it.
	PartitionMarkers(ctx context.Context, consumer string) (map[string]int64, error)
	// ResetPartitionMarkers sets every partition marker of consumer to 0, in
	// one transaction.
	ResetPartitionMarkers(ctx context.Context, consumer string) error
}

// PartitionMarkers reads every historian:metrics/<n>.<i> marker.
func (s *Sink) PartitionMarkers(ctx context.Context, consumer string) (map[string]int64, error) {
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT coalesce(json_object_agg(consumer, "offset"), '{}')::text
		FROM colca_applied_offset WHERE consumer LIKE $1`, consumer+"/%").Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return map[string]int64{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("historian: reading the partition markers: %w", err)
	}
	out := map[string]int64{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("historian: reading the partition markers: %w", err)
	}
	return out, nil
}

// ResetPartitionMarkers zeroes every historian:metrics/<n>.<i> marker.
func (s *Sink) ResetPartitionMarkers(ctx context.Context, consumer string) error {
	if _, err := s.Pool.Exec(ctx, `UPDATE colca_applied_offset SET "offset" = 0, updated_at = now() WHERE consumer LIKE $1`,
		consumer+"/%"); err != nil {
		return fmt.Errorf("historian: resetting the partition markers: %w", err)
	}
	return nil
}

// startMarkers turns the page marker and the partition markers into where a
// start resumes: the marker rows at or below are skipped, and partition i of
// n's own marker. Markers of another writer count (WRITERS changed, or a run
// stopped uncleanly before it) do not map onto today's partitions, but the
// smallest of a complete set is a point every row at or below has been
// written, so the start may skip to it.
func startMarkers(page int64, markers map[string]int64, n int) (int64, []int64) {
	own := make([]int64, n)
	sets := map[int][]int64{}
	for name, offset := range markers {
		count, index, ok := parsePartitionConsumer(name)
		if !ok {
			continue
		}
		if count == n {
			own[index] = offset
			continue
		}
		sets[count] = append(sets[count], offset)
	}
	applied := page
	for count, offsets := range sets {
		if len(offsets) != count {
			continue // a partition that never wrote has no marker: no floor
		}
		floor := offsets[0]
		for _, o := range offsets[1:] {
			floor = min(floor, o)
		}
		applied = max(applied, floor)
	}
	return applied, own
}

// parsePartitionConsumer reads historian:metrics/<n>.<i>.
func parsePartitionConsumer(name string) (n, i int, ok bool) {
	rest, found := strings.CutPrefix(name, Consumer+"/")
	if !found {
		return 0, 0, false
	}
	a, b, found := strings.Cut(rest, ".")
	if !found {
		return 0, 0, false
	}
	n, err1 := strconv.Atoi(a)
	i, err2 := strconv.Atoi(b)
	if err1 != nil || err2 != nil || n < 1 || i < 0 || i >= n {
		return 0, 0, false
	}
	return n, i, true
}

// Partitions reports the sink's writer count.
func (s *Sink) Partitions() int { return max(s.Writers, 1) }

// ApplyShare writes one partition's rows and its marker in one transaction,
// row by row with savepoints when a row is poison (applyOne).
func (s *Sink) ApplyShare(ctx context.Context, rows []Row, consumer string, through int64) ([]Rejection, error) {
	return s.applyOne(ctx, rows, consumer, through)
}

// partitionConsumer names partition i of n's marker. The count is part of the
// name: with another WRITERS the signals fall into other partitions, and an
// old partition's marker says nothing about the new one's rows.
func partitionConsumer(n, i int) string { return fmt.Sprintf("%s/%d.%d", Consumer, n, i) }

// Mark moves the marker in a transaction of its own.
func (s *Sink) Mark(ctx context.Context, consumer string, offset int64) error {
	if _, err := s.Pool.Exec(ctx, upsertOffset, consumer, offset); err != nil {
		return fmt.Errorf("historian: moving the marker to %d: %w", offset, err)
	}
	return nil
}

// pipelinePage is one fetched page on its way to the marker.
type pipelinePage struct {
	last    int64 // the page's last offset: the marker once it and every page before it are written
	pending int   // shares not yet written; guarded by pipeline.mu
}

// share is one partition's rows of one page.
type share struct {
	page *pipelinePage
	rows []Row
}

// Why a partition has a marker of its own: a crash after a partition wrote
// page N+1 but before the page marker reached it replays page N+1. Replaying a
// value is harmless, but a retraction is written only when the row before it
// holds a value, and with late data (an older timestamp arriving after the
// retraction) the replayed retraction can see a row the first run had not
// written yet. Each share moves its partition's marker in its own
// transaction, so a replay skips exactly the rows that partition already wrote
// and nothing is applied twice.

// decoded is a fetched page on its way to the writers. Pages are decoded in
// parallel and dispatched in fetch order.
type decoded struct {
	page    door.Page
	applied int64   // the marker rowsOf skips at or below
	last    int64   // the page's last offset
	written []int64 // non-nil: the partitions' markers from this page on
	rows    []Row
	done    chan struct{}
}

// decoders is how many pages are decoded at once. Decoding (rowsOf) costs
// ~2 µs a record, a third of a 5000-record page's ~27 ms between fetch and
// dispatch; on the fetcher it capped the historian near 100 k rows/s.
const decoders = 4

// pipeline is the state of one runPipelined.
type pipeline struct {
	b     *Bridge
	store PipelineStore
	ahead aheadFetcher

	// slots bounds the pages between fetch and marker: a fetch takes one, the
	// marker passing the page returns it. This is the whole of the pipeline's
	// memory, and how far a crash can make the next start replay.
	slots chan struct{}
	// writers[i] holds partition i's shares in page order.
	writers []chan share
	// pages are the fetched pages in stream order, oldest first.
	pages chan *pipelinePage
	// written wakes the marker after a page's last share was written.
	written chan struct{}
	// decode feeds the decoders; order hands the same pages to the
	// dispatcher in fetch order.
	decode, order chan *decoded

	// marked is the page marker as last written; idle hears when the last page
	// in flight was marked.
	marked atomic.Int64
	idle   chan struct{}

	mu sync.Mutex
	// failing holds each part's current error (the fetcher, a writer, the
	// marker); Health hears healthy only when none is failing.
	failing map[string]string
}

// health records one part's outcome and tells Health the pipeline's state.
func (p *pipeline) health(part string, err error) {
	if p.b.Health == nil {
		return
	}
	p.mu.Lock()
	if err != nil {
		p.failing[part] = err.Error()
	} else {
		delete(p.failing, part)
	}
	ok, detail := len(p.failing) == 0, ""
	for name, msg := range p.failing {
		detail = name + ": " + msg
		break
	}
	p.mu.Unlock()
	p.b.Health(ok, detail)
}

// pipelined reports whether Run can write several pages at once: the door reads
// ahead of the cursor, the store writes partitions on their own, and the run is
// neither strict nor coordinated (those apply one page at a time on purpose).
func (b *Bridge) pipelined() (*pipeline, bool) {
	if b.Pipeline < 2 || b.Strict || b.Coordinate != nil {
		return nil, false
	}
	ahead, ok := b.Door.(aheadFetcher)
	if !ok {
		return nil, false
	}
	store, ok := b.Store.(PipelineStore)
	if !ok {
		return nil, false
	}
	p := &pipeline{
		b: b, store: store, ahead: ahead,
		slots:   make(chan struct{}, b.Pipeline),
		writers: make([]chan share, store.Partitions()),
		pages:   make(chan *pipelinePage, b.Pipeline),
		written: make(chan struct{}, 1),
		idle:    make(chan struct{}, 1),
		decode:  make(chan *decoded, b.Pipeline),
		order:   make(chan *decoded, b.Pipeline),
		failing: map[string]string{},
	}
	for i := range p.writers {
		p.writers[i] = make(chan share, b.Pipeline)
	}
	return p, true
}

// runPipelined is Run with up to b.Pipeline pages between fetch and marker.
//
// One goroutine fetches pages in stream order and hands each partition's rows
// to that partition's writer. Each writer writes its shares in page order, so a
// signal's rows land in stream order (what a retraction depends on), but a
// writer does not wait for the other partitions of a page before it starts on
// the next one, and it writes the shares that queued up while it was busy in
// one transaction. The marker follows in its own transaction and only to the
// end of the newest page whose shares, and every earlier page's, are written;
// the ack follows the marker. A crash anywhere replays from the marker: every
// statement is idempotent, and the replay re-applies each signal's rows in
// order, so it ends where the first run would have.
//
// A write that fails is retried in place with backoff; the pages behind it
// wait in the bounded queues and the fetcher waits for a slot.
func (p *pipeline) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for i := range p.writers {
		wg.Go(func() { p.write(ctx, i) })
	}
	wg.Go(func() { p.mark(ctx) })
	for range decoders {
		wg.Go(func() { p.decodePages(ctx) })
	}
	wg.Go(func() { p.dispatchPages(ctx) })
	err := p.fetch(ctx)
	cancel()
	wg.Wait()
	return err
}

// fetch reads pages and hands them to the writers. It follows Run's drain
// rules: the wakeup is captured before the first page of a drain, and a drain
// that reached the announced head (or an empty page) waits for the next hint.
func (p *pipeline) fetch(ctx context.Context) error {
	b := p.b
	started := time.Now()
	retry := time.Second
	var changed <-chan struct{}
	var head int64
	var next int64 // 0: from the cursor
	applied := int64(-1)
	var written []int64 // each partition's own marker, until the first page carries it
	failed := func(err error) bool {
		p.health("fetch", err)
		b.logger().Error("historian pass failed, retrying", "err", err)
		ok := sleep(ctx, door.RetryDelay(err, retry))
		retry = min(30*time.Second, retry*2)
		return ok
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if changed == nil {
			changed = b.Changes()
			head = 0
			if b.Head != nil {
				head = b.Head()
			}
		}
		if b.Signals != nil {
			if err := b.Signals.Wait(ctx); err != nil {
				return err
			}
		}
		if applied < 0 {
			marker, err := p.store.Applied(ctx, Consumer)
			var markers map[string]int64
			if err == nil {
				markers, err = p.store.PartitionMarkers(ctx, Consumer)
			}
			if err != nil {
				if !failed(err) {
					return ctx.Err()
				}
				continue
			}
			applied, written = startMarkers(marker, markers, len(p.writers))
			p.marked.Store(marker)
		}
		select {
		case p.slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		var page door.Page
		var err error
		if next == 0 {
			page, err = b.Door.Fetch(ctx, "metrics", Cursor, b.max())
		} else {
			page, err = p.ahead.FetchWithOptions(ctx, door.FetchOptions{Stream: "metrics", Cursor: Cursor, Max: b.max(), From: uint64(next)}) //nolint:gosec // offsets are positive
			if err == nil && page.From != 0 && page.From != next {
				err = fmt.Errorf("historian: asked for metrics from %d, the node served from %d", next, page.From)
			}
		}
		if err != nil {
			<-p.slots
			if !failed(err) {
				return ctx.Err()
			}
			// colcad may have restarted on a new store meanwhile: read from
			// the cursor again (see below).
			if next != 0 && !p.backToCursor(ctx, &next, &applied) {
				return ctx.Err()
			}
			continue
		}
		retry = time.Second
		p.health("fetch", nil)
		b.NowMS, b.FetchedAt = page.NowMS, time.Now()
		if len(page.Records) == 0 {
			<-p.slots
			// At the head: once the pages in flight are marked, the next drain
			// reads from the cursor again. That is where a recreated stream
			// (colcad's volume replaced) shows: /fetch from an offset never
			// reads behind the cursor and past a new stream's head returns
			// nothing, while the new cursor starts at its first record.
			if next != 0 && !p.backToCursor(ctx, &next, &applied) {
				return ctx.Err()
			}
			p.health("fetch", nil)
			if !door.WaitChange(ctx, changed, -1, started.Add(b.BatchInterval)) {
				return ctx.Err()
			}
			started, changed = time.Now(), nil
			continue
		}
		last := page.Records[len(page.Records)-1].Offset
		item := &decoded{page: page, applied: applied, last: last, written: written, done: make(chan struct{})}
		written = nil
		if b.newStream(page, p.marked.Load()) {
			// The rest of the stream is new too, and so are the partitions'
			// markers. They are zeroed in the database before
			// any row of the new stream is written: a writer that gets no rows
			// for a while would otherwise keep an old marker, and a restart
			// would skip its rows below it.
			for {
				err := p.store.ResetPartitionMarkers(ctx, Consumer)
				if err == nil {
					break
				}
				if !failed(err) {
					return ctx.Err()
				}
			}
			applied, item.applied = 0, 0
			p.marked.Store(0)
			item.written = make([]int64, len(p.writers))
		}
		next = page.Next
		select {
		case p.order <- item:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case p.decode <- item:
		case <-ctx.Done():
			return ctx.Err()
		}
		if head == 0 || next < head {
			continue
		}
		if !door.WaitChange(ctx, changed, -1, started.Add(b.BatchInterval)) {
			return ctx.Err()
		}
		started, changed = time.Now(), nil
	}
}

// backToCursor waits until every page in flight is marked, then makes the
// next fetch read from the cursor with the marker as last written.
func (p *pipeline) backToCursor(ctx context.Context, next, applied *int64) bool {
	for len(p.slots) > 0 {
		select {
		case <-p.idle:
		case <-ctx.Done():
			return false
		}
	}
	*next = 0
	*applied = max(*applied, p.marked.Load())
	return true
}

// decodePages turns fetched pages into rows, several pages at once.
func (p *pipeline) decodePages(ctx context.Context) {
	for {
		select {
		case item := <-p.decode:
			item.rows, _ = p.b.rowsOf(item.page, item.applied) // never fails when not strict
			item.page = door.Page{}
			close(item.done)
		case <-ctx.Done():
			return
		}
	}
}

// dispatchPages hands decoded pages to the writers in fetch order.
func (p *pipeline) dispatchPages(ctx context.Context) {
	written := make([]int64, len(p.writers))
	for {
		var item *decoded
		select {
		case item = <-p.order:
		case <-ctx.Done():
			return
		}
		select {
		case <-item.done:
		case <-ctx.Done():
			return
		}
		if item.written != nil {
			copy(written, item.written)
		}
		p.dispatch(ctx, item.last, item.rows, written)
	}
}

// dispatch queues a page for the marker, then its shares for the writers.
// A row at or below its partition's marker was written before a restart.
func (p *pipeline) dispatch(ctx context.Context, last int64, rows []Row, written []int64) {
	n := len(p.writers)
	shares := make([][]Row, n)
	for _, row := range rows {
		i := 0
		if n > 1 {
			i = partitionOf(row.SignalID, n)
		}
		if row.Offset <= written[i] {
			continue
		}
		shares[i] = append(shares[i], row)
	}
	page := &pipelinePage{last: last}
	for _, s := range shares {
		if len(s) > 0 {
			page.pending++
		}
	}
	select {
	case p.pages <- page:
	case <-ctx.Done():
		return
	}
	if page.pending == 0 {
		p.wake()
		return
	}
	for i, s := range shares {
		if len(s) == 0 {
			continue
		}
		select {
		case p.writers[i] <- share{page: page, rows: s}:
		case <-ctx.Done():
			return
		}
	}
}

func (p *pipeline) wake() {
	select {
	case p.written <- struct{}{}:
	default:
	}
}

// write is partition i's writer.
func (p *pipeline) write(ctx context.Context, i int) {
	b := p.b
	part := fmt.Sprintf("writer %d", i)
	consumer := partitionConsumer(len(p.writers), i)
	queue := p.writers[i]
	for {
		var batch []share
		select {
		case s := <-queue:
			batch = append(batch, s)
		case <-ctx.Done():
			return
		}
		// What queued while the last write ran goes in the same transaction,
		// still in page order, up to a page's worth of rows.
		rows := len(batch[0].rows)
	more:
		for rows < b.max() {
			select {
			case s := <-queue:
				batch = append(batch, s)
				rows += len(s.rows)
			default:
				break more
			}
		}
		all := batch[0].rows
		if len(batch) > 1 {
			all = make([]Row, 0, rows)
			for _, s := range batch {
				all = append(all, s.rows...)
			}
		}
		through := batch[len(batch)-1].page.last
		retry := time.Second
		for {
			rejections, err := p.store.ApplyShare(ctx, all, consumer, through)
			if err == nil {
				for _, rej := range rejections {
					b.reject(rej)
				}
				p.health(part, nil)
				break
			}
			if ctx.Err() != nil {
				return
			}
			// A commit whose answer was lost is in the database with the
			// partition's marker. Writing it again would re-apply its
			// retractions after the rows behind them, so look first.
			if marker, merr := p.store.Applied(ctx, consumer); merr == nil && marker >= through {
				b.logger().Warn("a write reported failure but had committed; not writing it again",
					"rows", len(all), "through", through, "err", err)
				p.health(part, nil)
				break
			}
			p.health(part, err)
			b.logger().Error("historian write failed, retrying", "rows", len(all), "err", err)
			if !sleep(ctx, door.RetryDelay(err, retry)) {
				return
			}
			retry = min(30*time.Second, retry*2)
		}
		p.mu.Lock()
		done := false
		for _, s := range batch {
			s.page.pending--
			done = done || s.page.pending == 0
		}
		p.mu.Unlock()
		if done {
			p.wake()
		}
	}
}

// mark moves the marker and the ack to the newest page that, with every page
// before it, is written.
func (p *pipeline) mark(ctx context.Context) {
	b := p.b
	var waiting []*pipelinePage
	for {
		select {
		case <-p.written:
		case <-ctx.Done():
			return
		}
		// Pages are queued before their shares, so every page a share
		// finished is already here or still in the channel.
	collect:
		for {
			select {
			case page := <-p.pages:
				waiting = append(waiting, page)
			default:
				break collect
			}
		}
		p.mu.Lock()
		n := 0
		for n < len(waiting) && waiting[n].pending == 0 {
			n++
		}
		p.mu.Unlock()
		if n == 0 {
			continue
		}
		last := waiting[n-1].last
		retry := time.Second
		for {
			err := p.store.Mark(ctx, Consumer, last)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			p.health("marker", err)
			b.logger().Error("historian marker write failed, retrying", "offset", last, "err", err)
			if !sleep(ctx, door.RetryDelay(err, retry)) {
				return
			}
			retry = min(30*time.Second, retry*2)
		}
		if _, err := b.Door.Ack(ctx, "metrics", Cursor, last); err != nil {
			// The rows and the marker are durable; a restart re-reads from the
			// cursor and the marker skips what is written.
			b.logger().Warn("applied but could not ack", "offset", last, "err", err)
		} else {
			b.Acknowledged = last
		}
		p.health("marker", nil)
		p.marked.Store(last)
		waiting = waiting[n:]
		for range n {
			<-p.slots
		}
		if len(p.slots) == 0 {
			select {
			case p.idle <- struct{}{}:
			default:
			}
		}
		// A page that finished while the marker was written has no wakeup of
		// its own left.
		p.wake()
	}
}

package historian

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// insertMetric writes one row into historian_metric, which Grafana and the
// API's history endpoints read. Metrics are idempotent by (signal_id,
// timestamp), so a recomputed point overwrites the stored value.
//
// Every value column is set from EXCLUDED, not only the incoming one: a row has
// exactly one value column, and a type change at the same key must not leave the
// old column behind. The WHERE clause skips identical redeliveries, so they
// write no new row version.
const insertMetric = `
INSERT INTO historian_metric
    (timestamp, value_json, value_number, value_text, value_bool, colca_node_id, signal_id)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (signal_id, timestamp) DO UPDATE SET
    value_json = EXCLUDED.value_json,
    value_number = EXCLUDED.value_number,
    value_text = EXCLUDED.value_text,
    value_bool = EXCLUDED.value_bool,
    colca_node_id = EXCLUDED.colca_node_id
WHERE (historian_metric.value_json, historian_metric.value_number, historian_metric.value_text,
       historian_metric.value_bool, historian_metric.colca_node_id)
      IS DISTINCT FROM
      (EXCLUDED.value_json, EXCLUDED.value_number, EXCLUDED.value_text,
       EXCLUDED.value_bool, EXCLUDED.colca_node_id)`

// insertMetrics is insertMetric for many rows in one statement: the page's
// values arrive as one array per column. Postgres plans and executes it once,
// where one statement per row cost a parse, a plan and an executor start each
// (the historian's ceiling was one Postgres core at ~5,700 rows/s, fleet scale
// benchmark 2026-10). A statement may not touch the same row twice, so a group
// carries each (signal_id, timestamp) once: the last one, as a row-by-row apply
// would leave it.
const insertMetrics = `
INSERT INTO historian_metric
    (timestamp, value_json, value_number, value_text, value_bool, colca_node_id, signal_id)
SELECT u.ts, u.vj::jsonb, u.vn, u.vt, u.vb, u.node, u.sig
FROM unnest($1::timestamptz[], $2::text[], $3::double precision[], $4::text[], $5::boolean[], $6::text[], $7::text[])
    AS u(ts, vj, vn, vt, vb, node, sig)
ON CONFLICT (signal_id, timestamp) DO UPDATE SET
    value_json = EXCLUDED.value_json,
    value_number = EXCLUDED.value_number,
    value_text = EXCLUDED.value_text,
    value_bool = EXCLUDED.value_bool,
    colca_node_id = EXCLUDED.colca_node_id
WHERE (historian_metric.value_json, historian_metric.value_number, historian_metric.value_text,
       historian_metric.value_bool, historian_metric.colca_node_id)
      IS DISTINCT FROM
      (EXCLUDED.value_json, EXCLUDED.value_number, EXCLUDED.value_text,
       EXCLUDED.value_bool, EXCLUDED.colca_node_id)`

// insertRetraction writes a row with every value column NULL: the value went
// missing at this timestamp. Report by exception applies to missing too: the row
// is written only when the latest row at or before it still holds a value, so a
// repeated null writes nothing, and a signal with no history gets no row. At the
// same key it replaces a stored value, like insertMetric.
const insertRetraction = `
INSERT INTO historian_metric
    (timestamp, value_json, value_number, value_text, value_bool, colca_node_id, signal_id)
SELECT $1::timestamptz, NULL::jsonb, NULL::double precision, NULL::text, NULL::boolean, $2::text, $3::text
WHERE EXISTS (
    SELECT 1
    FROM (SELECT value_json, value_number, value_text, value_bool
          FROM historian_metric
          WHERE signal_id = $3::text AND timestamp <= $1::timestamptz
          ORDER BY timestamp DESC
          LIMIT 1) AS latest
    WHERE latest.value_json IS NOT NULL OR latest.value_number IS NOT NULL
       OR latest.value_text IS NOT NULL OR latest.value_bool IS NOT NULL)
ON CONFLICT (signal_id, timestamp) DO UPDATE SET
    value_json = NULL,
    value_number = NULL,
    value_text = NULL,
    value_bool = NULL,
    colca_node_id = EXCLUDED.colca_node_id`

// upsertOffset moves the marker in the same transaction as the rows it
// describes, with the store the records were read from ($3, NULL when none):
// colca's /fetch names it, and a marker is a position only in that store.
const upsertOffset = `
INSERT INTO colca_applied_offset (consumer, "offset", store, updated_at)
VALUES ($1, $2, NULLIF($3::text, ''), now())
ON CONFLICT (consumer) DO UPDATE SET "offset" = EXCLUDED."offset", store = EXCLUDED.store, updated_at = now()`

// markLateWrites records which hours of which signals a page wrote into after
// those hours were due to be summarised. PREKIT keeps per-signal statistics of
// the history and computes an hour once, from lateAfter past its start; a
// sample that arrives later (a child catching up, a replay, an import, a
// corrected value) would leave that hour's statistics stale. The mark names the
// hour again, in the transaction that writes the sample: a sample is never
// committed without its mark, and a mark is never visible before its sample.
//
// It is append-only: one row per hour the page wrote late into, carrying the
// signals as a JSON array. No unique key, no conflict handling, no lock beyond
// the insert's own: a page never waits for the reader or for another page.
// The reader (PREKIT's statistics worker) reads committed marks, recomputes
// their hours from historian_metric and deletes the marks it handled; marks of
// the same hour from several pages are merged there, not here.
//
// Only signals the reader lists in historian_late_write_signal are marked: a
// node without statistics writes nothing here. The database's clock decides
// what is late, read when this statement runs (clock_timestamp): the reader
// closes an hour by the same clock and first waits for every transaction that
// was in flight when the hour became due, so a page that decided "not late"
// has committed before the hour is read. Hours are UTC hours. $1 and $2 are
// parallel arrays, each (signal, hour) once (lateWrites).
//
// The reader is notified when a mark was added.
const markLateWrites = `
WITH marked AS (
    INSERT INTO historian_late_write (hour, signal_ids)
    SELECT u.hour, jsonb_agg(u.sig ORDER BY u.sig)
    FROM unnest($1::text[], $2::timestamptz[]) AS u(sig, hour)
    WHERE u.hour + interval '65 minutes' <= clock_timestamp()
      AND EXISTS (SELECT 1 FROM historian_late_write_signal AS wanted WHERE wanted.signal_id = u.sig)
    GROUP BY u.hour
    RETURNING 1
)
SELECT pg_notify('historian_late_write', '') FROM (SELECT 1 FROM marked LIMIT 1) AS any_marked`

// lateAfter is how long after an hour's start a sample of that hour is late;
// markLateWrites carries the same 65 minutes. lateSlack is how far this
// process's clock may run behind the database's before a late sample would
// not be offered to the statement at all.
const (
	lateAfter = 65 * time.Minute
	lateSlack = 10 * time.Minute
)

const createOffsetTable = `
CREATE TABLE IF NOT EXISTS colca_applied_offset (
    consumer   text PRIMARY KEY,
    "offset"   bigint NOT NULL DEFAULT 0,
    store      text,
    updated_at timestamptz NOT NULL DEFAULT now()
)`

// addOffsetStore gives a table created by an older historian the store column.
const addOffsetStore = `ALTER TABLE colca_applied_offset ADD COLUMN IF NOT EXISTS store text`

// dbPool is the part of *pgxpool.Pool the sink uses. pgx.Tx is already an
// interface, so poison_test.go can fake Begin without a database.
type dbPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Sink writes rows and the marker into Timescale.
type Sink struct {
	Pool dbPool
	// Strict preserves the whole page on any failure during coordinated runs.
	Strict bool
	// Writers, above 1, writes a page over that many connections at once, each
	// with the rows of its own share of the signals; see applyPartitioned.
	Writers int
}

// lateWrites returns markLateWrites' arrays for rows: each (signal, hour) once,
// left out when the hour cannot be late yet by this process's clock plus
// lateSlack. The statement decides exactly, by the database's clock.
func lateWrites(rows []Row, now time.Time) (signals []string, hours []time.Time) {
	type key struct {
		signal string
		hour   time.Time
	}
	var seen map[key]bool
	for _, row := range rows {
		hour := row.Timestamp.UTC().Truncate(time.Hour)
		if hour.Add(lateAfter).After(now.Add(lateSlack)) {
			continue
		}
		k := key{row.SignalID, hour}
		if seen[k] {
			continue
		}
		if seen == nil {
			seen = make(map[key]bool)
		}
		seen[k] = true
		signals = append(signals, row.SignalID)
		hours = append(hours, hour)
	}
	return signals, hours
}

// Rejection is one row the schema permanently refused, set aside so the rest of
// the page and every later page keep flowing.
type Rejection struct {
	Row      Row
	Reason   string // bounded label — see poisonReasons below
	SQLState string
	Err      error
}

// poisonStates maps the Postgres error codes a bad publisher can cause to the
// reason labels of colca_historian_rows_rejected_total. PoisonReasons derives
// from this list.
var poisonStates = []struct {
	code   string
	reason string
}{
	// A value longer than its column, such as a non-ULID signal_id.
	{"22001", "value_too_long"},
	// Input that does not parse as the column's type.
	{"22P02", "invalid_text_representation"},
	// A NOT NULL column left empty.
	{"23502", "not_null_violation"},
	// A number outside the column's range.
	{"22003", "numeric_out_of_range"},
}

var poisonReasonByCode = func() map[string]string {
	m := make(map[string]string, len(poisonStates))
	for _, s := range poisonStates {
		m[s.code] = s.reason
	}
	return m
}()

// PoisonReasons returns the reason labels, so colca-historian can pre-create a
// zero-valued series for each and the first incident is alertable.
func PoisonReasons() []string {
	out := make([]string, len(poisonStates))
	for i, s := range poisonStates {
		out[i] = s.reason
	}
	return out
}

// poisonReason reports the reason label for an error no retry can fix: bad
// data in the row. Everything else (a dropped connection, a deadlock, a full
// disk) is transient and must be retried, or a measurement would be lost.
func poisonReason(err error) (reason, sqlstate string, ok bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", "", false
	}
	reason, ok = poisonReasonByCode[pgErr.Code]
	return reason, pgErr.Code, ok
}

// EnsureSchema creates the offset table and applies the retention policy.
// historian_metric itself belongs to the API's migrations.
func (s *Sink) EnsureSchema(ctx context.Context, retentionDays int) error {
	if _, err := s.Pool.Exec(ctx, createOffsetTable); err != nil {
		return fmt.Errorf("historian: creating the offset table: %w", err)
	}
	if _, err := s.Pool.Exec(ctx, addOffsetStore); err != nil {
		return fmt.Errorf("historian: adding the store to the offset table: %w", err)
	}
	var isHypertable bool
	if err := s.Pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM timescaledb_information.hypertables
			WHERE hypertable_schema = current_schema()
			  AND hypertable_name = 'historian_metric'
		)`).Scan(&isHypertable); err != nil {
		return fmt.Errorf("historian: checking the metric hypertable: %w", err)
	}
	if !isHypertable {
		if retentionDays == 0 {
			return nil
		}
		return fmt.Errorf("historian: cannot enforce %d-day metric retention: historian_metric is not a TimescaleDB hypertable", retentionDays)
	}
	if retentionDays == 0 {
		if _, err := s.Pool.Exec(ctx,
			`SELECT remove_retention_policy('historian_metric', if_exists => true)`); err != nil {
			return fmt.Errorf("historian: removing metric retention policy: %w", err)
		}
		return nil
	}
	if _, err := s.Pool.Exec(ctx,
		`SELECT remove_retention_policy('historian_metric', if_exists => true)`); err != nil {
		return fmt.Errorf("historian: replacing metric retention policy: %w", err)
	}
	if _, err := s.Pool.Exec(ctx,
		`SELECT add_retention_policy('historian_metric', make_interval(days => $1))`,
		retentionDays); err != nil {
		return fmt.Errorf("historian: applying %d-day metric retention policy: %w", retentionDays, err)
	}
	return nil
}

// Applied reads how far this consumer got. Zero when it has never run.
func (s *Sink) Applied(ctx context.Context, consumer string) (int64, error) {
	var offset int64
	err := s.Pool.QueryRow(ctx,
		`SELECT "offset" FROM colca_applied_offset WHERE consumer = $1`, consumer).Scan(&offset)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("historian: reading the marker for %s: %w", consumer, err)
	}
	return offset, nil
}

// AppliedStore reads which store this consumer's marker is a position in, ""
// when none was recorded (a marker written before stores were).
func (s *Sink) AppliedStore(ctx context.Context, consumer string) (string, error) {
	var store *string
	err := s.Pool.QueryRow(ctx,
		`SELECT store FROM colca_applied_offset WHERE consumer = $1`, consumer).Scan(&store)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && store == nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("historian: reading the marker's store for %s: %w", consumer, err)
	}
	return *store, nil
}

// Apply writes the rows and moves the marker in one transaction. An empty batch
// still moves the marker, or a page of tombstones would be fetched forever.
//
// The batch path runs first. If it fails because one row is poison, the page is
// applied row by row so the good rows land and the bad one is set aside;
// otherwise the page would block the sink forever. Any other error is returned
// and the whole page is retried.
func (s *Sink) Apply(ctx context.Context, rows []Row, consumer string, offset int64, store string) ([]Rejection, error) {
	if s.Writers > 1 && !s.Strict && len(rows) >= 2*s.Writers {
		return s.applyPartitioned(ctx, rows, consumer, offset, store)
	}
	return s.applyOne(ctx, rows, consumer, offset, store)
}

// applyOne writes rows, and the marker unless consumer is "", in one
// transaction, falling back to row by row when a row is poison.
func (s *Sink) applyOne(ctx context.Context, rows []Row, consumer string, offset int64, store string) ([]Rejection, error) {
	err := s.applyBatch(ctx, rows, consumer, offset, store)
	if err == nil {
		return nil, nil
	}
	if _, _, poison := poisonReason(err); !poison || s.Strict {
		return nil, err
	}
	return s.applyRowByRow(ctx, rows, consumer, offset, store)
}

// applyPartitioned writes a page over s.Writers connections at once. Rows are
// split by signal, so each signal's rows stay in one transaction and in page
// order, which is what a retraction depends on. The marker moves in a
// transaction of its own once every share committed: a crash in between
// replays the page, and every statement of it is idempotent (an identical
// value is not rewritten, a retraction is not stored twice). Any share failing
// fails the page, which is then retried whole.
//
// A single transaction keeps one Postgres backend busy; the historian's
// ceiling was that one core (fleet scale benchmark, 2026-10).
func (s *Sink) applyPartitioned(ctx context.Context, rows []Row, consumer string, offset int64, store string) ([]Rejection, error) {
	shares := make([][]Row, s.Writers)
	for _, row := range rows {
		i := partitionOf(row.SignalID, s.Writers)
		shares[i] = append(shares[i], row)
	}
	type result struct {
		rejections []Rejection
		err        error
	}
	results := make([]result, len(shares))
	var wg sync.WaitGroup
	for i, share := range shares {
		if len(share) == 0 {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rej, err := s.applyOne(ctx, share, "", offset, "")
			results[i] = result{rej, err}
		}()
	}
	wg.Wait()
	var rejections []Rejection
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		rejections = append(rejections, r.rejections...)
	}
	if _, err := s.Pool.Exec(ctx, upsertOffset, consumer, offset, store); err != nil {
		return nil, fmt.Errorf("historian: moving the marker to %d: %w", offset, err)
	}
	return rejections, nil
}

// partitionOf is a signal's share: FNV-1a of its id.
func partitionOf(signalID string, n int) int {
	h := uint32(2166136261)
	for i := 0; i < len(signalID); i++ {
		h ^= uint32(signalID[i])
		h *= 16777619
	}
	return int(h % uint32(n)) //nolint:gosec // n is a small positive writer count
}

// applyBatch sends the page and the marker in one batch and one transaction.
// Runs of values go as one statement each (insertMetrics); a retraction keeps a
// statement of its own and its place in the order, because whether it writes a
// row depends on the rows before it. The page's late writes are marked in the
// same transaction (markLateWrites).
func (s *Sink) applyBatch(ctx context.Context, rows []Row, consumer string, offset int64, store string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("historian: beginning a batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch := &pgx.Batch{}
	for start := 0; start < len(rows); {
		if rows[start].Missing() {
			sql, args := statementFor(rows[start])
			batch.Queue(sql, args...)
			start++
			continue
		}
		end := start
		for end < len(rows) && !rows[end].Missing() {
			end++
		}
		batch.Queue(insertMetrics, valueColumns(rows[start:end])...)
		start = end
	}
	if signals, hours := lateWrites(rows, time.Now()); len(signals) > 0 {
		batch.Queue(markLateWrites, signals, hours)
	}
	if consumer != "" {
		batch.Queue(upsertOffset, consumer, offset, store)
	}

	results := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("historian: applying a batch of %d at offset %d: %w",
				len(rows), offset, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("historian: closing a batch: %w", err)
	}
	return tx.Commit(ctx)
}

// valueColumns turns a run of value rows into insertMetrics' column arrays,
// keeping the last row of each (signal_id, timestamp). The key is the
// timestamp as Postgres stores it, in microseconds: two samples a few
// nanoseconds apart are one row there.
func valueColumns(rows []Row) []any {
	type key struct {
		signal string
		ts     time.Time
	}
	stored := func(row Row) time.Time { return row.Timestamp.Truncate(time.Microsecond).UTC() }
	last := make(map[key]int, len(rows))
	for i, row := range rows {
		last[key{row.SignalID, stored(row)}] = i
	}
	n := len(last)
	ts := make([]time.Time, 0, n)
	vj := make([]*string, 0, n)
	vn := make([]*float64, 0, n)
	vt := make([]*string, 0, n)
	vb := make([]*bool, 0, n)
	node := make([]*string, 0, n)
	sig := make([]string, 0, n)
	for i, row := range rows {
		if last[key{row.SignalID, stored(row)}] != i {
			continue
		}
		ts = append(ts, stored(row))
		var j *string
		if len(row.JSON) > 0 {
			v := string(row.JSON)
			j = &v
		}
		vj = append(vj, j)
		vn = append(vn, row.Number)
		vt = append(vt, row.Text)
		vb = append(vb, row.Bool)
		var nd *string
		if row.NodeID != "" {
			v := row.NodeID
			nd = &v
		}
		node = append(node, nd)
		sig = append(sig, row.SignalID)
	}
	return []any{ts, vj, vn, vt, vb, node, sig}
}

// applyRowByRow applies each row under its own savepoint in one transaction,
// skipping poisoned rows and committing the marker with the rows that landed. A
// transient error aborts the whole pass, and the page is retried.
func (s *Sink) applyRowByRow(ctx context.Context, rows []Row, consumer string, offset int64, store string) ([]Rejection, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("historian: beginning a row-by-row retry at offset %d: %w", offset, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var rejections []Rejection
	landed := make([]Row, 0, len(rows))
	for i, row := range rows {
		savepoint := fmt.Sprintf("hist_row_%d", i)
		if _, err := tx.Exec(ctx, "SAVEPOINT "+savepoint); err != nil {
			return nil, fmt.Errorf("historian: opening a savepoint for row %d at offset %d: %w", i, offset, err)
		}

		sql, args := statementFor(row)
		_, execErr := tx.Exec(ctx, sql, args...)
		if execErr == nil {
			if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
				return nil, fmt.Errorf("historian: releasing the savepoint for row %d at offset %d: %w", i, offset, err)
			}
			landed = append(landed, row)
			continue
		}

		reason, sqlstate, poison := poisonReason(execErr)
		if !poison {
			// Same rule as applyBatch: a transient failure retries the whole
			// page, it never skips a row.
			return nil, fmt.Errorf("historian: row %d at offset %d: %w", i, offset, execErr)
		}
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
			return nil, fmt.Errorf("historian: rolling back the poisoned row %d at offset %d: %w", i, offset, err)
		}
		rejections = append(rejections, Rejection{Row: row, Reason: reason, SQLState: sqlstate, Err: execErr})
	}

	if signals, hours := lateWrites(landed, time.Now()); len(signals) > 0 {
		if _, err := tx.Exec(ctx, markLateWrites, signals, hours); err != nil {
			return nil, fmt.Errorf("historian: marking the late writes of a row-by-row apply at offset %d: %w", offset, err)
		}
	}
	if consumer != "" {
		if _, err := tx.Exec(ctx, upsertOffset, consumer, offset, store); err != nil {
			return nil, fmt.Errorf("historian: moving the marker after a row-by-row apply at offset %d: %w", offset, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("historian: committing a row-by-row apply at offset %d: %w", offset, err)
	}
	return rejections, nil
}

// statementFor picks the insert for a row: a value, or a retraction.
func statementFor(row Row) (string, []any) {
	if row.Missing() {
		return insertRetraction, []any{row.Timestamp, nullable(row.NodeID), row.SignalID}
	}
	return insertMetric, []any{row.Timestamp, nullableJSON(row.JSON), row.Number, row.Text, row.Bool,
		nullable(row.NodeID), row.SignalID}
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return string(b)
}

// Open connects with a bounded pool. The historian is one follower, not a
// request server: a handful of connections is the whole need, and a large pool
// on an edge box competes with the API for the same Postgres.
func Open(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("historian: bad database URL: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnIdleTime = 5 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("historian: connecting: %w", err)
	}
	return pool, nil
}

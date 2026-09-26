package historian

import (
	"context"
	"errors"
	"fmt"
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
// describes.
const upsertOffset = `
INSERT INTO colca_applied_offset (consumer, "offset", updated_at)
VALUES ($1, $2, now())
ON CONFLICT (consumer) DO UPDATE SET "offset" = EXCLUDED."offset", updated_at = now()`

const createOffsetTable = `
CREATE TABLE IF NOT EXISTS colca_applied_offset (
    consumer   text PRIMARY KEY,
    "offset"   bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
)`

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

// Apply writes the rows and moves the marker in one transaction. An empty batch
// still moves the marker, or a page of tombstones would be fetched forever.
//
// The batch path runs first. If it fails because one row is poison, the page is
// applied row by row so the good rows land and the bad one is set aside;
// otherwise the page would block the sink forever. Any other error is returned
// and the whole page is retried.
func (s *Sink) Apply(ctx context.Context, rows []Row, consumer string, offset int64) ([]Rejection, error) {
	err := s.applyBatch(ctx, rows, consumer, offset)
	if err == nil {
		return nil, nil
	}
	if _, _, poison := poisonReason(err); !poison || s.Strict {
		return nil, err
	}
	return s.applyRowByRow(ctx, rows, consumer, offset)
}

// applyBatch sends every row and the marker in one batch and one transaction.
func (s *Sink) applyBatch(ctx context.Context, rows []Row, consumer string, offset int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("historian: beginning a batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch := &pgx.Batch{}
	for _, row := range rows {
		sql, args := statementFor(row)
		batch.Queue(sql, args...)
	}
	batch.Queue(upsertOffset, consumer, offset)

	results := tx.SendBatch(ctx, batch)
	for i := 0; i <= len(rows); i++ {
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

// applyRowByRow applies each row under its own savepoint in one transaction,
// skipping poisoned rows and committing the marker with the rows that landed. A
// transient error aborts the whole pass, and the page is retried.
func (s *Sink) applyRowByRow(ctx context.Context, rows []Row, consumer string, offset int64) ([]Rejection, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("historian: beginning a row-by-row retry at offset %d: %w", offset, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var rejections []Rejection
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

	if _, err := tx.Exec(ctx, upsertOffset, consumer, offset); err != nil {
		return nil, fmt.Errorf("historian: moving the marker after a row-by-row apply at offset %d: %w", offset, err)
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

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

// The sink's contract is fixed by a table that already exists and that Grafana
// and the API's history endpoints already read. `historian_metric` carries the
// value columns and a unique index on (signal_id, timestamp); the deleted Kafka
// writer used ON CONFLICT DO NOTHING against exactly that index. This uses
// ON CONFLICT DO UPDATE instead: metrics are idempotent by (signal_id,
// timestamp), not by revision (design §6), so the evaluator can recompute a
// window and safely re-publish the same points — the historian must end up
// with the NEW value, not the first one it ever saw. This is still the second
// net behind the marker: a redelivered batch cannot double-write even if the
// marker were somehow lost, it just now overwrites in place rather than
// silently dropping.
//
// Every value column is written unconditionally from EXCLUDED, not just the
// one the new row set. A Row carries at most one of Number/Text/Bool/JSON;
// the others arrive as NULL parameters (see nullable/nullableJSON below). If
// the SET list only touched the incoming row's own column, a value-type
// change at the same (signal_id, timestamp) — e.g. a signal that used to be a
// number now publishing text — would leave the stale value_number in place
// alongside the new value_text, corrupting the "exactly one column is set"
// invariant the API's reader depends on. Setting all four every time keeps
// that invariant no matter which column, if any, changes.
//
// The WHERE clause is the guard against churn: an identical redelivery must
// not write a new row version (no update, no replication, no cost) — only a
// genuine value change may. IS DISTINCT FROM treats NULL <> NULL as "not
// distinct", so a redelivery that still carries three NULL value columns and
// one unchanged value correctly matches as identical.
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

// The marker lives beside the rows it describes, in the same database and the
// same transaction — the guarantee, not a convenience (projector design §4).
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

// dbPool is the slice of *pgxpool.Pool the sink actually calls, narrowed to an
// interface for exactly one reason: pgx.Tx and pgx.BatchResults are already
// interfaces ("to allow tests to mock transactions" — their own doc comment),
// so a fake dbPool.Begin lets the poison/retry algorithm in Apply below be
// proven at level 1 (poison_test.go) with no real database, while
// *pgxpool.Pool (as built by Open) satisfies this exactly as-is — production
// wiring does not change.
type dbPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Sink writes rows and the marker into Timescale.
type Sink struct {
	Pool dbPool
}

// Rejection is one row the schema permanently refused, set aside by
// applyRowByRow so the rest of the page — and every page behind it — keeps
// flowing. Row.Offset/Row.Topic (record.go) carry enough context for the
// bridge to log and count it without this package knowing anything about the
// door or the stream.
type Rejection struct {
	Row      Row
	Reason   string // bounded label — see poisonReasons below
	SQLState string
	Err      error
}

// poisonState pairs one Postgres error code a bad publisher can trigger with
// the bounded reason label colca_historian_rows_rejected_total carries for
// it. This is the ONE list — poisonReasons (what cmd/colca-historian
// pre-creates as zero-valued metric children) is derived from it rather than
// hand-duplicated, so the two can never silently disagree about which reasons
// exist (architecture principle 2, one owner per fact).
var poisonStates = []struct {
	code   string
	reason string
}{
	// The concrete incident this exists for: a non-ULID signal_id (or any
	// text) longer than a column allows. No retry ever changes the length of
	// the same bytes.
	{"22001", "value_too_long"},
	// Malformed input for the target type — e.g. a numeric column fed text
	// that doesn't parse. Same row, same bytes, same failure every retry.
	{"22P02", "invalid_text_representation"},
	// A NOT NULL column left empty by the publisher. Retrying supplies the
	// same missing value.
	{"23502", "not_null_violation"},
	// A numeric literal outside the column's range (e.g. a double that
	// doesn't fit where the schema expects it to). Same value, same overflow
	// every time.
	{"22003", "numeric_out_of_range"},
}

var poisonReasonByCode = func() map[string]string {
	m := make(map[string]string, len(poisonStates))
	for _, s := range poisonStates {
		m[s.code] = s.reason
	}
	return m
}()

// PoisonReasons returns the bounded reason labels
// colca_historian_rows_rejected_total can carry, so cmd/colca-historian's
// /metrics handler can pre-create a zero-valued child for each — the same
// "every label combination scrapes as zero from boot" convention
// internal/metrics uses, so the first real incident is alertable rather than
// appearing as a previously-absent series.
func PoisonReasons() []string {
	out := make([]string, len(poisonStates))
	for i, s := range poisonStates {
		out[i] = s.reason
	}
	return out
}

// poisonReason reports the bounded reason label for an error the schema will
// NEVER accept regardless of how many times the same bytes are retried — a
// row-level data problem a bad publisher caused. ok is false for every other
// error: a dropped connection, a deadlock, a serialization failure, disk
// full, Postgres itself being down. Those are TRANSIENT — retrying is exactly
// right, and treating them as poison would durably lose a measurement that a
// moment's retry would have written. This is the one place that line is
// drawn; nothing else in this package guesses at a SQLSTATE.
func poisonReason(err error) (reason, sqlstate string, ok bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", "", false
	}
	reason, ok = poisonReasonByCode[pgErr.Code]
	return reason, pgErr.Code, ok
}

// EnsureSchema creates what this service owns and nothing else.
//
// It creates ONLY the offset table. `historian_metric` belongs to the api's
// migrations: a service that created another service's table would give that
// table two definitions, and the one that ran first would win.
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

// Apply writes the rows and moves the marker, in ONE transaction.
//
// An empty batch still moves the marker: a page whose records were all
// tombstones or all already-applied would otherwise be re-fetched forever.
//
// The batch path (applyBatch) is tried first — one round trip, and the path
// every page takes the overwhelming majority of the time. It fails wholesale
// only when the connection cannot deliver it (a real outage) or when ONE row
// in it is something the schema will never accept: a non-ULID signal_id
// longer than the column, a NOT NULL left empty, a value of the wrong shape.
// Before this fix that second case wedged the sink forever — the log line
// this fix exists for was the SAME batch (offset 36, then 46, then 56…)
// failing on the same poisoned row on every pass, because the cursor cannot
// advance past a page whose Apply keeps erroring.
//
// poisonReason is what tells the two cases apart. A poison-classified error
// falls back to applyRowByRow, which retries the SAME rows one at a time so
// every row this schema accepts still lands and the poisoned one is set aside
// instead of blocking it and everything on every later page. Any other error
// (the connection dropped, a deadlock, Postgres is down) is returned exactly
// as before: the whole page is retried unchanged on the next pass, and the
// marker does not move — skipping here would durably lose a measurement a
// moment's retry would have written.
func (s *Sink) Apply(ctx context.Context, rows []Row, consumer string, offset int64) ([]Rejection, error) {
	err := s.applyBatch(ctx, rows, consumer, offset)
	if err == nil {
		return nil, nil
	}
	if _, _, poison := poisonReason(err); !poison {
		return nil, err
	}
	return s.applyRowByRow(ctx, rows, consumer, offset)
}

// applyBatch is the original, still-the-common-case path: every row plus the
// marker pipelined in one batch, one transaction, one round trip.
func (s *Sink) applyBatch(ctx context.Context, rows []Row, consumer string, offset int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("historian: beginning a batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(insertMetric,
			row.Timestamp, nullableJSON(row.JSON), row.Number, row.Text, row.Bool,
			nullable(row.NodeID), row.SignalID)
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

// applyRowByRow is applyBatch's fallback once a poison-classified error says
// one row in this page will never apply, however many times it is retried.
//
// Each row gets its own SAVEPOINT inside a single transaction: a poisoned row
// rolls back to a clean point without discarding the rows already inserted
// ahead of it, and the marker upsert at the end still commits alongside every
// row that DID apply — one transaction, same guarantee applyBatch always had
// (rows and marker commit together, or neither does). Moving the marker here
// is the fix's actual observable effect: it is what lets the follow loop's
// cursor advance past a page that contains a poisoned row, instead of
// re-fetching the same page forever with a growing "batch of N" offset.
//
// A TRANSIENT failure hit during this retry (the same class applyBatch could
// hit) aborts the whole thing: the function returns an error, the deferred
// Rollback undoes every row this pass already inserted, and the caller
// retries the untouched page on its next pass — nothing here is durable until
// the final Commit, so aborting mid-way costs one redundant re-apply, not a
// lost row. Only a poison-classified row is ever skipped.
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

		_, execErr := tx.Exec(ctx, insertMetric,
			row.Timestamp, nullableJSON(row.JSON), row.Number, row.Text, row.Bool,
			nullable(row.NodeID), row.SignalID)
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

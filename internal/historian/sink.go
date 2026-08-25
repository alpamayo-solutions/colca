package historian

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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

// Sink writes rows and the marker into Timescale.
type Sink struct {
	Pool *pgxpool.Pool
}

// EnsureSchema creates what this service owns and nothing else.
//
// It creates ONLY the offset table. `historian_metric` belongs to the api's
// migrations: a service that created another service's table would give that
// table two definitions, and the one that ran first would win.
func (s *Sink) EnsureSchema(ctx context.Context) error {
	if _, err := s.Pool.Exec(ctx, createOffsetTable); err != nil {
		return fmt.Errorf("historian: creating the offset table: %w", err)
	}
	return nil
}

// Applied reads how far this consumer got. Zero when it has never run.
func (s *Sink) Applied(ctx context.Context, consumer string) (int64, error) {
	var offset int64
	err := s.Pool.QueryRow(ctx,
		`SELECT "offset" FROM colca_applied_offset WHERE consumer = $1`, consumer).Scan(&offset)
	if err == pgx.ErrNoRows {
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
func (s *Sink) Apply(ctx context.Context, rows []Row, consumer string, offset int64) error {
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

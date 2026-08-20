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
// writer used ON CONFLICT DO NOTHING against exactly that index, and so does
// this. That is the second net behind the marker: a redelivered batch cannot
// double-write even if the marker were somehow lost.
const insertMetric = `
INSERT INTO historian_metric
    (timestamp, value_json, value_number, value_text, value_bool, colca_node_id, signal_id)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (signal_id, timestamp) DO NOTHING`

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

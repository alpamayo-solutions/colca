package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/alpamayo-solutions/colca/internal/historian"
)

// schemaPool answers EnsureSchema's statements: every Exec fails while
// execFailures is above zero, and every boolean a query scans is present.
type schemaPool struct {
	execFailures int
	present      bool
	attempts     int
}

func (p *schemaPool) Begin(context.Context) (pgx.Tx, error) { panic("not used by EnsureSchema") }

func (p *schemaPool) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	if p.execFailures > 0 {
		p.execFailures--
		p.attempts++
		return pgconn.CommandTag{}, errors.New("the database system is starting up")
	}
	return pgconn.CommandTag{}, nil
}

func (p *schemaPool) QueryRow(context.Context, string, ...any) pgx.Row {
	p.attempts++
	return schemaRow{present: p.present}
}

type schemaRow struct{ present bool }

func (r schemaRow) Scan(dest ...any) error {
	for _, d := range dest {
		*(d.(*bool)) = r.present
	}
	return nil
}

func TestStartStopsAtOnceOnASchemaItsOwnerHasNotMigrated(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Waiting does not create the late write relations: one attempt, no retry
	// window, and the error the operator reads names what is missing.
	pool := &schemaPool{}
	started := time.Now()
	err := ensureSchema(context.Background(), &historian.Sink{Pool: pool}, 0, log, time.Minute)
	if !errors.Is(err, historian.ErrLateWriteRelationsMissing) {
		t.Fatalf("ensureSchema = %v, want ErrLateWriteRelationsMissing", err)
	}
	if pool.attempts != 1 || time.Since(started) > time.Second {
		t.Fatalf("checked %d times in %s, want once and at once", pool.attempts, time.Since(started))
	}

	// A database that is still starting IS waited for: the same loop retries
	// that error and starts once the schema answers.
	starting := &schemaPool{execFailures: 1, present: true}
	if err := ensureSchema(context.Background(), &historian.Sink{Pool: starting}, 0, log, time.Minute); err != nil {
		t.Fatalf("ensureSchema after a database that was starting: %v", err)
	}
	if starting.attempts < 2 {
		t.Fatalf("the starting database was asked %d times, want a retry", starting.attempts)
	}
}

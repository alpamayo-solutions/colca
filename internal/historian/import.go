package historian

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/alpamayo-solutions/colca/plugins/uns"
	"github.com/oklog/ulid/v2"
)

// ScanImport validates explicit measurements without opening a broker session.
// The caller owns authorization to the archive; importing never changes live
// retained values or dispatches old observations to operational consumers.
func ScanImport(ctx context.Context, input io.Reader, before time.Time, visit func(Row, int64) error) (int64, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	var count int64
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		count++
		var record struct {
			Topic   string          `json:"topic"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return count, fmt.Errorf("import line %d: %w", count, err)
		}
		parsed, err := uns.Parse(record.Topic)
		if err != nil || parsed.Contract != "_Metric" {
			return count, fmt.Errorf("import line %d: expected a valid _Metric topic", count)
		}
		var payload struct {
			Timestamp *float64 `json:"timestamp"`
		}
		if err := json.Unmarshal(record.Payload, &payload); err != nil || payload.Timestamp == nil ||
			math.IsNaN(*payload.Timestamp) || math.IsInf(*payload.Timestamp, 0) ||
			*payload.Timestamp < 0 || *payload.Timestamp >= float64(before.Unix()) {
			return count, fmt.Errorf("import line %d: explicit timestamp must precede %s", count, before.Format(time.RFC3339))
		}
		row, err := RowFrom(record.Topic, record.Payload, 0)
		if err != nil {
			return count, fmt.Errorf("import line %d: %w", count, err)
		}
		if _, err := ulid.ParseStrict(row.SignalID); err != nil {
			return count, fmt.Errorf("import line %d: signal_id must be a ULID", count)
		}
		if _, err := ulid.ParseStrict(parsed.NodeID); err != nil || row.NodeID != parsed.NodeID {
			return count, fmt.Errorf("import line %d: node identity must match the topic", count)
		}
		row.Offset, row.Topic = count, record.Topic
		if visit != nil {
			if err := visit(row, count); err != nil {
				return count, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return count, err
	}
	if count == 0 {
		return 0, fmt.Errorf("import contains no measurements")
	}
	return count, nil
}

// Import applies a previously validated immutable file through the ordinary
// historian sink. Its content-addressed marker cannot advance the live cursor.
// The sink must be strict: no rejected row may be acknowledged as imported.
func Import(ctx context.Context, input io.Reader, before time.Time, digest string, batchSize int, sink Store) (int64, error) {
	if len(digest) != 64 || batchSize < 1 || batchSize > 5000 {
		return 0, fmt.Errorf("import requires SHA-256 and a batch size in [1,5000]")
	}
	consumer := "historian:import:" + digest
	applied, err := sink.Applied(ctx, consumer)
	if err != nil {
		return 0, err
	}
	rows := make([]Row, 0, batchSize)
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		rejected, err := sink.Apply(ctx, rows, consumer, rows[len(rows)-1].Offset, "")
		if err != nil {
			return err
		}
		if len(rejected) != 0 {
			return fmt.Errorf("import sink rejected %d rows; strict sink required", len(rejected))
		}
		rows = rows[:0]
		return nil
	}
	count, err := ScanImport(ctx, input, before, func(row Row, ordinal int64) error {
		if ordinal <= applied {
			return nil
		}
		rows = append(rows, row)
		if len(rows) == batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return count, err
	}
	if applied > count {
		return count, fmt.Errorf("import checkpoint exceeds the immutable file length")
	}
	return count, flush()
}

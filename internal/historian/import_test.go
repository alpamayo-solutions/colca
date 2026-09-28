package historian

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

const importTopic = "colca/v1/_Metric/01J0000000000000000000000A/plant/machine/temp"
const importSignal = "01J0000000000000000000000B"

func importLine(ts int) string {
	return fmt.Sprintf(`{"topic":%q,"payload":{"signal_id":%q,"timestamp":%d,"value":21.5}}`+"\n", importTopic, importSignal, ts)
}

func TestScanImportRejectsInvalidHistory(t *testing.T) {
	valid := importLine(100)
	for name, input := range map[string]string{
		"empty": "", "invalid json": "{", "current": importLine(200),
		"missing timestamp": strings.ReplaceAll(valid, `"timestamp":100,`, ""),
		"wrong contract":    strings.ReplaceAll(valid, "_Metric", "_Signal"),
		"wrong identity":    strings.ReplaceAll(valid, importSignal, "unregistered"),
		"node mismatch":     strings.ReplaceAll(valid, `"value":21.5`, `"value":21.5,"colca_node_id":"other"`),
		"tombstone":         strings.ReplaceAll(valid, `"value":21.5`, `"deleted":true`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ScanImport(context.Background(), strings.NewReader(input), time.Unix(200, 0), nil); err == nil {
				t.Fatal("invalid history was accepted")
			}
		})
	}
}

type importStore struct {
	marker   int64
	rows     []Row
	failAt   int64
	consumer string
}

func (s *importStore) Applied(_ context.Context, consumer string) (int64, error) {
	s.consumer = consumer
	return s.marker, nil
}

func (s *importStore) Apply(_ context.Context, rows []Row, consumer string, offset int64) ([]Rejection, error) {
	if offset == s.failAt {
		return nil, fmt.Errorf("database unavailable")
	}
	s.marker = offset
	s.consumer = consumer
	s.rows = append(s.rows, rows...)
	return nil, nil
}

func TestImportResumesCommittedBatchesWithoutMovingLiveCursor(t *testing.T) {
	input := importLine(100) + importLine(101) + importLine(102)
	sink := &importStore{failAt: 3}
	digest := strings.Repeat("a", 64)
	ctx := context.Background()
	if _, err := Import(ctx, strings.NewReader(input), time.Unix(200, 0), digest, 2, sink); err == nil {
		t.Fatal("lost failure")
	}
	if sink.marker != 2 || len(sink.rows) != 2 {
		t.Fatalf("uncommitted progress: %+v", sink)
	}
	sink.failAt = 0
	for range 2 {
		if _, err := Import(ctx, strings.NewReader(input), time.Unix(200, 0), digest, 2, sink); err != nil {
			t.Fatal(err)
		}
	}
	if sink.marker != 3 || len(sink.rows) != 3 {
		t.Fatalf("resume duplicated rows: %+v", sink)
	}
	if sink.consumer != "historian:import:"+digest || sink.consumer == Consumer {
		t.Fatal("import shares live cursor")
	}
}

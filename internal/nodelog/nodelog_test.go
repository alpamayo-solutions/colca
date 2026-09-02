package nodelog

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func record(attrs ...slog.Attr) slog.Record {
	r := slog.NewRecord(time.Now(), slog.LevelDebug, "atomic event ingest", 0)
	r.AddAttrs(attrs...)
	return r
}

func TestARecordAboutPublishingALogIsRefused(t *testing.T) {
	// The engine logs every append with the topic it appended. Publishing
	// that record appends again, which logs again: on a node started with
	// LOG_LEVEL=debug the two feed each other forever.
	appended := record(
		slog.String("stream", "logs"),
		slog.String("topic", "colca/v1/_Log/01NODE/colca/INFO"),
	)
	if !SkipsItsOwnPublishing(appended) {
		t.Error("a record about appending a _Log must never itself be published")
	}
}

func TestEveryOtherRecordStillPublishes(t *testing.T) {
	// The denominator for the assertion above: the guard must cut the cycle
	// and nothing else, or the node would go quiet for the wrong reason.
	for _, topic := range []string{
		"colca/v1/_Metric/01NODE/line1/press3/temperature",
		"colca/v1/_ServiceDetails/01NODE/line1/connector",
		"colca/v1/_CmdConfigure/01NODE/apply",
	} {
		if SkipsItsOwnPublishing(record(slog.String("topic", topic))) {
			t.Errorf("a record about %s is ordinary and must be published", topic)
		}
	}
	if SkipsItsOwnPublishing(record(slog.String("stream", "metrics"))) {
		t.Error("a record with no topic at all must be published")
	}
}

func TestTheSinkRefusesUntilTheEngineIsAttached(t *testing.T) {
	// The publisher is installed before the engine exists, so that startup
	// lines are captured. Until Attach, delivery must fail rather than panic
	// -- the publisher treats it as an outage and holds the records.
	sink := &Sink{}
	if _, _, err := sink.LogPosition(context.Background()); err == nil {
		t.Error("LogPosition must report not-ready before Attach, not answer with an empty node")
	}
	if err := sink.PublishLog(context.Background(), "colca/v1/_Log/01NODE/colca/INFO",
		map[string]any{"message": "starting"}); err == nil {
		t.Error("PublishLog must report not-ready before Attach")
	}
}

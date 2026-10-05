package repl

import (
	"testing"

	"github.com/alpamayo-solutions/colca/internal/config"
)

func TestLogForwardingDefaultIsWarning(t *testing.T) {
	f := newLogForwarding(config.ParentLogs{})
	for topic, want := range map[string]bool{
		"colca/v1/_Log/n1/colca/DEBUG":            false,
		"colca/v1/_Log/n1/colca/INFO":             false,
		"colca/v1/_Log/n1/colca/WARNING":          true,
		"colca/v1/_Log/n1/colca/ERROR":            true,
		"colca/v1/_Log/n1/colca/CRITICAL":         true,
		"colca/v1/_Log/n1/site/line/dataops/INFO": false,
	} {
		if got, _ := f.forward(topic); got != want {
			t.Errorf("%s: forward=%v, want %v", topic, got, want)
		}
	}
}

func TestLogForwardingServiceOverride(t *testing.T) {
	f := newLogForwarding(config.ParentLogs{MinLevel: "error", Services: map[string]string{"dataops": "info"}})
	for topic, want := range map[string]bool{
		// The override is keyed by the segment before the level.
		"colca/v1/_Log/n1/site/line/dataops/INFO":    true,
		"colca/v1/_Log/n1/site/line/dataops/DEBUG":   false,
		"colca/v1/_Log/n1/dataops/INFO":              true,
		"colca/v1/_Log/n1/site/line/connector/INFO":  false,
		"colca/v1/_Log/n1/site/line/connector/ERROR": true,
		// A mount segment of the same name is not the service.
		"colca/v1/_Log/n1/dataops/connector/WARNING": false,
	} {
		if got, _ := f.forward(topic); got != want {
			t.Errorf("%s: forward=%v, want %v", topic, got, want)
		}
	}
	if ok, level := f.forward("colca/v1/_Log/n1/connector/INFO"); ok || level != "INFO" {
		t.Errorf("a withheld record reports its level: ok=%v level=%q", ok, level)
	}
}

func TestLogForwardingFailsOpen(t *testing.T) {
	f := newLogForwarding(config.ParentLogs{MinLevel: "CRITICAL"})
	for _, topic := range []string{
		"colca/v1/_Log/n1/colca/NOTICE",       // unknown level segment
		"colca/v1/_Log/n1/colca/info",         // levels are upper case on the wire
		"colca/v1/_StreamGap/n1/logs",         // another contract on the logs stream
		"not/a/uns/topic",                     // unparseable
		"colca/v1/_Metric/n1/line/press/temp", // not a log at all
	} {
		if ok, _ := f.forward(topic); !ok {
			t.Errorf("%s: must be forwarded", topic)
		}
	}
}

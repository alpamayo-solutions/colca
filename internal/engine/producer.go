package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// observeIndexes keeps the engine's in-memory projections current with a
// record this node just stored, whatever door it came through.
func (e *Engine) observeIndexes(contract, topic string, payload []byte) {
	e.elements.Observe(contract, topic, payload)
	e.catalogues.Observe(contract, topic, payload)
}

// boundTag returns the data tag the signal at a _Metric's path is bound to,
// or "" when there is no signal there or the signal is bound to nothing.
func (e *Engine) boundTag(p uns.Parsed) string {
	raw, ok := e.EntityStore().KVGet(uns.SignalTopicForMetric(p))
	if !ok || len(raw) == 0 {
		return ""
	}
	var signal struct {
		DataTag string `json:"data_tag"`
	}
	if json.Unmarshal(raw, &signal) != nil {
		return ""
	}
	return signal.DataTag
}

// catalogueTopicOf is the catalogue topic entry publishes on, or false when
// its element is not resolvable here: such an entry owns no catalogue.
func (e *Engine) catalogueTopicOf(entry *uns.Entry) (string, bool) {
	mount := ""
	if entry.Element != "" {
		path, ok := e.elements.PathOf(entry.Element)
		if !ok {
			return "", false
		}
		mount = path
	}
	return uns.CatalogueTopic(e.cfg.ULID, mount, entry.CatalogueName()), true
}

// checkMetricProducer admits a _Metric for a bound signal only from the
// signal's producer: the identity whose catalogue holds the data tag the
// signal is bound to. That covers connectors, dataops outputs and signals
// autobind created alike, since each is bound to a tag in its producer's
// catalogue. A signal bound to nothing, or a path with no signal, keeps the
// write-zone rule the caller already applied.
//
// Replicated records from children and the admin door do not pass through
// here: a child admitted its own metrics, and the admin token is the operator.
func (e *Engine) checkMetricProducer(entry *uns.Entry, p uns.Parsed) error {
	tag := e.boundTag(p)
	if tag == "" {
		return nil
	}
	holders := e.catalogues.Holders(tag)
	own, placed := e.catalogueTopicOf(entry)
	if placed && len(holders) == 1 && holders[0] == own {
		return nil
	}
	switch len(holders) {
	case 0:
		return fmt.Errorf("signal %s is bound to data tag %s, which no catalogue on this node holds; "+
			"only its producer may publish its _Metric", p.Path, tag)
	case 1:
		return fmt.Errorf("signal %s is bound to data tag %s of %s; %s is not its producer",
			p.Path, tag, catalogueOwner(holders[0]), entry.CatalogueName())
	default:
		return fmt.Errorf("signal %s is bound to data tag %s, which %d catalogues claim (%s); "+
			"no one may publish its _Metric until one of them drops it",
			p.Path, tag, len(holders), strings.Join(holders, ", "))
	}
}

// catalogueOwner is the service name a catalogue topic ends in.
func catalogueOwner(topic string) string {
	return topic[strings.LastIndex(topic, "/")+1:]
}

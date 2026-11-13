package uns

import (
	"encoding/json"
	"fmt"
)

// A _Signal's replication_policy decides whether the samples of its _Metric
// records rise to the parent. The store asks these at append time, under its
// write lock, and keeps the answer with each sample.

// MetricSignalKey names the KV entry whose _Signal decides how a _Metric at
// topic replicates: path, node and topic of that entry. ok is false for a
// record that is not a metric, whose replication no signal decides.
func MetricSignalKey(topic string) (path, node, signalTopic string, ok bool) {
	p, err := Parse(topic)
	if err != nil || !IsMetric(p.Contract) {
		return "", "", "", false
	}
	return p.Path, p.NodeID, SignalTopicForMetric(p), true
}

// DecidesReplication reports whether a record at topic can change how later
// records replicate: a _Signal, whose replication_policy its metrics follow.
func DecidesReplication(topic string) bool {
	p, err := Parse(topic)
	return err == nil && p.Contract == "_Signal"
}

// SignalSourceLocalOnly reads a _Signal payload's replication_policy: true for
// source_local_only, false for replicate_to_parents or none. Any other value
// is an error.
func SignalSourceLocalOnly(signal []byte) (bool, error) {
	var fields struct {
		Policy *string `json:"replication_policy"`
	}
	if err := json.Unmarshal(signal, &fields); err != nil {
		return false, err
	}
	if fields.Policy == nil {
		return false, nil
	}
	switch *fields.Policy {
	case "replicate_to_parents":
		return false, nil
	case "source_local_only":
		return true, nil
	default:
		return false, fmt.Errorf("invalid signal replication_policy %q", *fields.Policy)
	}
}

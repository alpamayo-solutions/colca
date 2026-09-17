package store

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestMetricReplicationDecisionSurvivesChangesAndRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	writePolicy := func(policy string) {
		t.Helper()
		payload := fmt.Sprintf(`{"id":"s1","replication_policy":%q}`, policy)
		_, _, err := s.Append("entities", []Record{{Topic: "colca/v1/_Signal/n1/temp", Payload: []byte(payload), KVPath: "temp", KVNode: "n1"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	writeMetric := func() {
		t.Helper()
		_, _, err := s.Append("metrics", []Record{{Topic: "colca/v1/_Metric/n1/temp", Payload: []byte(`{"signal_id":"s1","value":42}`), KVPath: "temp", KVNode: "n1"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	writeMetric() // Existing/unbound signals retain the default.
	writePolicy("source_local_only")
	writeMetric()
	writePolicy("replicate_to_parents")
	writeMetric()
	writePolicy("source_local_only")
	writeMetric()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	records, next, err := s.Read("metrics", 1, 10, nil)
	if err != nil || len(records) != 4 || next != 5 {
		t.Fatalf("read: %+v %d %v", records, next, err)
	}
	for i, local := range []bool{false, true, false, true} {
		if records[i].SourceLocalOnly != local {
			t.Fatalf("record %d changed eligibility: %+v", i, records[i])
		}
	}
	entries, err := s.KVScan("temp")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("local signal and retained metric must remain: %+v", entries)
	}
	// Changing/deleting the signal must not change already accepted samples.
	_, _, err = s.Append("entities", []Record{{Topic: "colca/v1/_Signal/n1/temp", KVPath: "temp", KVNode: "n1", Delete: true}})
	if err != nil {
		t.Fatal(err)
	}
	records, _, _ = s.Read("metrics", 1, 10, nil)
	if !records[1].SourceLocalOnly || records[2].SourceLocalOnly {
		t.Fatal("deletion changed history")
	}
}

func TestReplicationSkipsAdvanceOnlyTheChildProgress(t *testing.T) {
	s := mustOpen(t)
	records := []ReplRecord{{SkipFrom: 1, ChildOffset: 200}, {ChildOffset: 201, Topic: "colca/v1/_Metric/n1/temp", Payload: []byte(`{"value":42}`)}, {SkipFrom: 202, ChildOffset: 400}}
	applied, hwm, err := s.ApplyReplicated("child", "metrics", records)
	if err != nil || len(applied) != 3 || hwm != 400 || s.NextOffset("metrics") != 2 {
		t.Fatalf("apply: %+v %d %v", applied, hwm, err)
	}
	applied, hwm, err = s.ApplyReplicated("child", "metrics", records)
	if err != nil || len(applied) != 0 || hwm != 400 {
		t.Fatalf("replay: %+v %d %v", applied, hwm, err)
	}
	if _, _, err = s.ApplyReplicated("child", "metrics", []ReplRecord{{SkipFrom: 402, ChildOffset: 401}}); err == nil {
		t.Fatal("accepted reversed skip")
	}
	if _, _, err = s.ApplyReplicated("child", "entities", []ReplRecord{{SkipFrom: 1, ChildOffset: 3}}); err == nil {
		t.Fatal("accepted non-metric skip")
	}
}

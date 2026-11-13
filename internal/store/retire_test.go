package store

import (
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// seedReplicated applies one record a child replicated, projected into KV the way
// the repl server projects owned state.
func seedReplicated(t *testing.T, s *Store, child, stream string, off uint64, node, path, contract, payload string) string {
	t.Helper()
	topic := "colca/v1/" + contract + "/" + node + "/" + path
	if _, _, err := s.ApplyReplicated(child, stream, []ReplRecord{{
		ChildOffset: off, Topic: topic, Payload: []byte(payload), TS: 1,
		KVPath: path, KVNode: node,
	}}); err != nil {
		t.Fatal(err)
	}
	return topic
}

func kvTopics(t *testing.T, s *Store) []string {
	t.Helper()
	entries, err := s.KVScan("")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Topic)
	}
	sort.Strings(out)
	return out
}

// A decommissioned child's replicated state has no author left to retire it, so
// the retiring revoke does: every selected entry goes, each with a tombstone on
// its own stream so ancestors retire their copies, in the batch that deletes the
// identity. The child's marks and incarnation go too, so the same identity
// enrolled again later starts clean.
func TestRegistryRetireTombstonesTheChildsStateAndForgetsItsMarks(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.registryPutForTest("01CHILD"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AdoptChildStore("01CHILD", "child-store"); err != nil {
		t.Fatal(err)
	}
	element := seedReplicated(t, s, "01CHILD", "entities", 5, "01CHILD", "site/edge1/press3", "_SystemElement", `{"id":"el-p3"}`)
	metric := seedReplicated(t, s, "01CHILD", "metrics", 9, "01CHILD", "site/edge1/press3/state", "_Metric", `{"v":1}`)
	kept := seedReplicated(t, s, "01OTHER", "entities", 3, "01OTHER", "site/edge2/x", "_SystemElement", `{"id":"el-x"}`)
	if _, _, err := s.Append("entities", []Record{{
		Topic: "colca/v1/_SystemElement/01PARENT/site/edge1", Payload: []byte(`{"id":"el-edge1"}`), TS: 1,
		KVPath: "site/edge1", KVNode: "01PARENT",
	}}); err != nil {
		t.Fatal(err)
	}
	own := "colca/v1/_SystemElement/01PARENT/site/edge1"

	streamOf := func(e KVEntry) string {
		if e.NodeID != "01CHILD" {
			return ""
		}
		if strings.Contains(e.Topic, "/_Metric/") {
			return "metrics"
		}
		return "entities"
	}
	entitiesHead, metricsHead := s.NextOffset("entities"), s.NextOffset("metrics")
	off, retired, err := s.RegistryRetire("01CHILD", "entities",
		identityTombstone("01CHILD"),
		nil, ChildRetirement{Child: "01CHILD", StreamOf: streamOf, TS: 2})
	if err != nil {
		t.Fatalf("RegistryRetire: %v", err)
	}
	if off != entitiesHead {
		t.Fatalf("offset = %d, want the identity's tombstone at the entities head %d", off, entitiesHead)
	}

	if got, want := kvTopics(t, s), []string{kept, own}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("KV after retire = %v, want only %v", got, want)
	}
	if len(retired) != 2 {
		t.Fatalf("retired = %v, want the element and the metric", retired)
	}
	reg, err := s.RegistryScan()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reg["01CHILD"]; ok {
		t.Fatal("the registry entry survived the retire")
	}

	// Each tombstone rides the stream its contract rises on.
	tombstones := func(stream string, from uint64) map[string]bool {
		recs, _, err := s.Read(stream, from, 100, nil)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, r := range recs {
			if len(r.Payload) == 0 {
				out[r.Topic] = true
			}
		}
		return out
	}
	if got := tombstones("entities", entitiesHead); !got[element] || got[kept] || got[own] {
		t.Fatalf("entities tombstones = %v, want %s and neither %s nor %s", got, element, kept, own)
	}
	if got := tombstones("metrics", metricsHead); !got[metric] || len(got) != 1 {
		t.Fatalf("metrics tombstones = %v, want exactly %s", got, metric)
	}

	for _, stream := range []string{"entities", "metrics"} {
		if got := s.HWMGet("01CHILD", stream); got != 0 {
			t.Fatalf("HWM(01CHILD, %s) = %d after retire, want 0", stream, got)
		}
	}
	if got := s.HWMGet("01OTHER", "entities"); got != 3 {
		t.Fatalf("HWM(01OTHER) = %d, want 3: another child's marks are not this retire's", got)
	}
	// The recorded incarnation is gone: a store seen after the retire is the
	// first one, not a rebuild, so marks written since are not reset.
	seedReplicated(t, s, "01CHILD", "entities", 1, "01CHILD", "site/edge1/new", "_SystemElement", `{"id":"el-new"}`)
	if reset, err := s.AdoptChildStore("01CHILD", "next-store"); err != nil || reset {
		t.Fatalf("adopting a store after the retire: reset=%v err=%v, want a first sighting", reset, err)
	}
	if got := s.HWMGet("01CHILD", "entities"); got != 1 {
		t.Fatalf("HWM after the first sighting = %d, want 1 kept", got)
	}
}

// A retire that selects nothing is still a revoke with its marks cleared.
func TestRegistryRetireWithNothingToRetire(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.registryPutForTest("01CHILD"); err != nil {
		t.Fatal(err)
	}
	seedReplicated(t, s, "01CHILD", "metrics", 4, "01CHILD", "a/b", "_Metric", `{"v":1}`)
	_, retired, err := s.RegistryRetire("01CHILD", "entities",
		identityTombstone("01CHILD"),
		nil, ChildRetirement{Child: "01CHILD", StreamOf: func(KVEntry) string { return "" }, TS: 2})
	if err != nil || len(retired) != 0 {
		t.Fatalf("retired=%v err=%v, want nothing and no error", retired, err)
	}
	if got := s.HWMGet("01CHILD", "metrics"); got != 0 {
		t.Fatalf("HWM = %d, want 0", got)
	}
}

func identityTombstone(ulid string) Record {
	return Record{
		Topic: "colca/v1/_EnrolledIdentity/01PARENT/_colca/identities/" + ulid, TS: 2,
		KVPath: "_colca/identities/" + ulid, KVNode: "01PARENT",
	}
}

func (s *Store) registryPutForTest(ulid string) error {
	_, err := s.RegistryPut(ulid, []byte(`{}`), "entities", Record{
		Topic: "colca/v1/_EnrolledIdentity/01PARENT/_colca/identities/" + ulid, Payload: []byte(`{}`), TS: 1,
		KVPath: "_colca/identities/" + ulid, KVNode: "01PARENT",
	})
	return err
}

// The selection walk runs without the store lock: the node's appends go on
// while it walks. An entry deleted before the batch gets no tombstone.
func TestRegistryRetireWalksWithoutTheStoreLock(t *testing.T) {
	s := mustOpen(t)
	if err := s.registryPutForTest("01CHILD"); err != nil {
		t.Fatal(err)
	}
	seedReplicated(t, s, "01CHILD", "entities", 5, "01CHILD", "site/edge1/a", "_SystemElement", `{"id":"a"}`)
	seedReplicated(t, s, "01CHILD", "entities", 6, "01CHILD", "site/edge1/b", "_SystemElement", `{"id":"b"}`)
	walking, appended := make(chan struct{}), make(chan error)
	var once sync.Once
	streamOf := func(e KVEntry) string {
		if e.NodeID != "01CHILD" {
			return ""
		}
		once.Do(func() {
			close(walking)
			select {
			case err := <-appended:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("an append waited for the retirement's walk")
			}
		})
		return "entities"
	}
	go func() {
		<-walking
		// Another writer while the walk runs: an append, and the deletion of
		// one selected entry.
		_, _, err := s.Append("entities", []Record{{
			Topic: "colca/v1/_SystemElement/01CHILD/site/edge1/b", TS: 3, KVPath: "site/edge1/b", KVNode: "01CHILD", Delete: true,
		}})
		appended <- err
	}()
	_, retired, err := s.RegistryRetire("01CHILD", "entities", identityTombstone("01CHILD"), nil,
		ChildRetirement{Child: "01CHILD", StreamOf: streamOf, TS: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(retired) != 1 || !strings.HasSuffix(retired[0].Topic, "/site/edge1/a") {
		t.Fatalf("retired %+v, want only a: b was deleted before the batch", retired)
	}
}

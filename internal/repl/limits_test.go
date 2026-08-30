package repl

import (
	"strings"
	"testing"

	"github.com/alpamayo-solutions/colca/internal/store"
)

func TestFitReplicationBatchReturnsLargestPrefixWithinBodyLimit(t *testing.T) {
	records := []store.ReplRecord{
		{ChildOffset: 1, Topic: "colca/v1/_Metric/n/a", Payload: []byte(strings.Repeat("a", 1024))},
		{ChildOffset: 2, Topic: "colca/v1/_Metric/n/b", Payload: []byte(strings.Repeat("b", 1024))},
		{ChildOffset: 3, Topic: "colca/v1/_Metric/n/c", Payload: []byte(strings.Repeat("c", 1024))},
	}
	two, err := marshalReplication("metrics", records[:2])
	if err != nil {
		t.Fatal(err)
	}
	fitted, err := fitReplicationBatch("metrics", records, int64(len(two)))
	if err != nil {
		t.Fatal(err)
	}
	if len(fitted) != 2 || fitted[1].ChildOffset != 2 {
		t.Fatalf("fitted batch = %+v, want first two records", fitted)
	}
	if _, err := fitReplicationBatch("metrics", records, 1); err == nil {
		t.Fatal("one record larger than the request limit was accepted")
	}
}

func TestReplicationClientRejectsMoreThanProtocolBatchMaximumBeforeNetwork(t *testing.T) {
	c := &Client{maxReplicateBody: defaultReplicateBodyBytes}
	if _, _, err := c.replicate(t.Context(), "metrics", make([]store.ReplRecord, maxReplicateRecords+1)); err == nil {
		t.Fatal("201-record replication batch was accepted")
	}
}

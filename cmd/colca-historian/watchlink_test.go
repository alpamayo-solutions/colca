package main

import (
	"testing"
	"time"
)

func TestTheWatchLinkCountsFromTheFirstFailureUntilAHintArrives(t *testing.T) {
	var link watchLink
	start := time.Unix(1000, 0)
	if got := link.downFor(start); got != 0 {
		t.Fatalf("a link never down reports %s", got)
	}
	link.down(start)
	link.down(start.Add(30 * time.Second)) // a failed reconnect does not restart the count
	if got := link.downFor(start.Add(90 * time.Second)); got != 90*time.Second {
		t.Fatalf("down for %s, want 90s", got)
	}
	link.up()
	if got := link.downFor(start.Add(100 * time.Second)); got != 0 {
		t.Fatalf("a link that got a hint reports %s down", got)
	}
}

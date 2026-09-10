package uns

import (
	"strings"
	"testing"
)

// withRoot switches the process-wide root for one test and restores it after.
func withRoot(t *testing.T, r string) {
	t.Helper()
	before := Root()
	if err := SetRoot(r); err != nil {
		t.Fatalf("SetRoot(%q): %v", r, err)
	}
	t.Cleanup(func() {
		if err := SetRoot(before); err != nil {
			t.Errorf("restoring root %q: %v", before, err)
		}
	})
}

func TestTheRootDefaultsToColca(t *testing.T) {
	if Root() != "colca" || Prefix() != "colca/v1/" {
		t.Fatalf("root %q, prefix %q: want colca and colca/v1/", Root(), Prefix())
	}
}

func TestValidRoot(t *testing.T) {
	for _, ok := range []string{"colca", "acme", "plant-7", "a.b_c", "X1", strings.Repeat("x", 64)} {
		if err := ValidRoot(ok); err != nil {
			t.Errorf("ValidRoot(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "-lead", "_contract", ".dot", "$SYS", "a/b", "+", "#", "a b", "ä", strings.Repeat("x", 65)} {
		if ValidRoot(bad) == nil {
			t.Errorf("ValidRoot(%q) = nil, want an error", bad)
		}
	}
}

// Changing the root moves the whole namespace: what counts as a topic, what a
// subscription can reach, and the topics this package builds itself.
func TestSetRootMovesTheWholeNamespace(t *testing.T) {
	withRoot(t, "acme")

	if !IsUns("acme/v1/_Metric/n1/line1/temp") {
		t.Error("a topic under the configured root is not part of the namespace")
	}
	if IsUns("colca/v1/_Metric/n1/line1/temp") {
		t.Error("a topic under the default root is still part of the namespace after the root changed")
	}
	if got := TimeSyncTopic("n1"); got != "acme/v1/_TimeSync/n1" {
		t.Errorf("TimeSyncTopic = %q, want acme/v1/_TimeSync/n1", got)
	}
	if got := SignalTopicForMetric(Parsed{NodeID: "n1", Path: "line1/temp"}); got != "acme/v1/_Signal/n1/line1/temp" {
		t.Errorf("SignalTopicForMetric = %q, want acme/v1/_Signal/n1/line1/temp", got)
	}
	if _, inNamespace := fixedPathPrefix("acme/#"); !inNamespace {
		t.Error("a subscription under the configured root is treated as plain broker traffic")
	}
	if _, inNamespace := fixedPathPrefix("colca/#"); inNamespace {
		t.Error("a subscription under the old root is still judged as namespace traffic")
	}
	if !isTimeSyncFilter("acme/v1/_TimeSync/+") || isTimeSyncFilter("colca/v1/_TimeSync/+") {
		t.Error("the time-sync filter does not follow the root")
	}
}

func TestSetRootRefusesAnInvalidRootAndKeepsTheCurrentOne(t *testing.T) {
	withRoot(t, "acme")
	if err := SetRoot("a/b"); err == nil {
		t.Fatal("SetRoot accepted a root with a slash")
	}
	if Root() != "acme" {
		t.Fatalf("root = %q after a refused change, want acme", Root())
	}
}

func TestSetRootFromEnv(t *testing.T) {
	before := Root()
	t.Cleanup(func() { _ = SetRoot(before) })

	t.Setenv(RootEnv, "plant")
	if err := SetRootFromEnv(); err != nil || Root() != "plant" {
		t.Fatalf("SetRootFromEnv with %s=plant: root %q, err %v", RootEnv, Root(), err)
	}
	t.Setenv(RootEnv, "")
	if err := SetRootFromEnv(); err != nil || Root() != DefaultRoot {
		t.Fatalf("SetRootFromEnv with %s unset: root %q, err %v", RootEnv, Root(), err)
	}
	t.Setenv(RootEnv, "#")
	if err := SetRootFromEnv(); err == nil {
		t.Fatalf("SetRootFromEnv accepted %s=#", RootEnv)
	}
}

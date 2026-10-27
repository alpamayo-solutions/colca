package repl

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

// The uplink setting a record aside and DELETE /uplink/set-aside clearing
// another both read the copies and write the finding from what they read. A
// clear that read "none left" before a new copy was stored must not retire the
// finding after the uplink wrote it for the new copy: one publish at a time, so
// the finding left standing names every copy there is.
//
// The clear is held between its read and its write; the uplink sets a record
// aside meanwhile and publishes. Before the fix that publish ran through and the
// held clear then retired the finding over it. Now it waits for the clear.
func TestASetAsideFindingIsNotRetiredByAClearThatReadBeforeANewCopy(t *testing.T) {
	dir := t.TempDir()
	cs := mustStore(t, filepath.Join(dir, "cdata"))
	_, ceng := nodeParts(t, cs, &config.Config{ULID: "n-child"}, nil, nil, nil)
	ceng.SetContracts(findingBundle(t))
	parent := mustIdentity(t, filepath.Join(dir, "p.key"))
	cl := mustClient(t, "127.0.0.1:1", parent.PublicHex(), mustIdentity(t, filepath.Join(dir, "c.key")))
	up := uns.UplinkCursor(cl.ParentPub())

	setAside := func(off uint64) {
		t.Helper()
		moved, err := cs.SetAsideUplink(up, store.SetAsideRecord{
			Parent: cl.ParentPub(), Stream: "metrics", Offset: off,
			Record: store.StoredRecord{Offset: off, Topic: "colca/v1/_CmdParam/n-child/m1/go", Payload: []byte(`{}`)},
			Status: 400, Reason: "refused", AtMS: 1,
		})
		if err != nil || !moved {
			t.Fatalf("SetAsideUplink(%d) = %v, %v", off, moved, err)
		}
	}
	finding := SetAsideFindingTopic("n-child")
	p, _ := uns.Parse(finding)

	setAside(1)
	if err := cl.publishSetAsideFinding(ceng); err != nil {
		t.Fatal(err)
	}

	// The clear reads no copy left, then is held before it writes.
	clearRead, releaseClear := make(chan struct{}), make(chan struct{})
	cl.findingRead = func() {
		cl.findingRead = nil // only the clear is held
		close(clearRead)
		<-releaseClear
	}
	clearDone := make(chan error, 1)
	go func() {
		_, err := cl.ClearSetAside(ceng, "metrics", 1)
		clearDone <- err
	}()
	waitForClosed(t, "the clear to read the copies", clearRead, 5*time.Second)

	// Meanwhile the uplink sets the next record aside and publishes.
	setAside(2)
	waiting := make(chan struct{})
	cl.findingWait = func() { close(waiting) }
	uplinkDone := make(chan error, 1)
	go func() { uplinkDone <- cl.publishSetAsideFinding(ceng) }()
	select {
	case <-waiting: // it waits for the held clear: release the clear
	case err := <-uplinkDone: // it ran past the held clear
		if err != nil {
			t.Fatal(err)
		}
		uplinkDone <- nil
	}
	close(releaseClear)
	if err := <-clearDone; err != nil {
		t.Fatal(err)
	}
	if err := <-uplinkDone; err != nil {
		t.Fatal(err)
	}

	entry, ok, err := cs.KVGet(p.Path, p.NodeID, finding)
	if err != nil || !ok {
		t.Fatalf("the finding was retired although offset 2 is set aside (err %v)", err)
	}
	if !strings.Contains(string(entry.Payload), `"offset":2`) || strings.Contains(string(entry.Payload), `"offset":1`) {
		t.Fatalf("the finding does not name exactly the copy left: %s", entry.Payload)
	}
}

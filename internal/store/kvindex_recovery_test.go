package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

func TestKVIndexRecoveryCheckpointsBothPasses(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b := s.db.NewBatch()
	for i := 0; i < 75; i++ {
		path := fmt.Sprintf("p%03d", i)
		topic := "colca/v1/_Signal/n1/" + path
		// The recovery only needs keys: it must not decode or retain KV payloads.
		if err := b.Set(kvKey(path, "n1", topic), []byte("large or undecodable payload"), nil); err != nil {
			t.Fatal(err)
		}
		if err := b.Set(kvIndexKey("gone"+path, "n1", topic), nil, nil); err != nil {
			t.Fatal(err)
		}
		if err := b.Set([]byte("x\x00_Wrong\x00"+path+"\x00n1\x00"+topic), nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Set([]byte("x\x00broken"), nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}
	b.Close()
	stopped := errors.New("interrupt after durable page")
	var previous *kvIndexRecovery
	pages, sawSecond := 0, false
	for {
		_, _, err := s.reconcileKVIndexBounded(7, 256, func(p kvIndexRecovery, size int) error {
			pages++
			if size > 1024 {
				t.Fatalf("page batch %d exceeds bounded allowance", size)
			}
			if previous != nil && p.Phase == previous.Phase && bytes.Compare(p.After, previous.After) <= 0 {
				t.Fatalf("checkpoint did not advance: %+v -> %+v", previous, p)
			}
			copyP := p
			copyP.After = append([]byte(nil), p.After...)
			previous = &copyP
			if p.Phase == 1 {
				sawSecond = true
			}
			if p.Phase == 2 {
				return nil
			}
			return stopped
		})
		if err == nil {
			break
		}
		if !errors.Is(err, stopped) {
			t.Fatal(err)
		}
		// Close only Pebble, as a failed Open does: never write the clean marker.
		if err := s.db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err := pebble.Open(dir, &pebble.Options{})
		if err != nil {
			t.Fatal(err)
		}
		s.db = db
	}
	if pages < 10 || !sawSecond {
		t.Fatalf("pages=%d second pass=%v", pages, sawSecond)
	}
	if n := countIndexKeys(t, s); n != 75 {
		t.Fatalf("index has %d entries, want 75", n)
	}
	raw, c, err := s.db.Get(kvIndexRecoveryKey)
	if err == nil {
		c.Close()
		t.Fatalf("completed checkpoint remains: %s", raw)
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatal(err)
	}
	for i := 0; i < 75; i++ {
		path := fmt.Sprintf("p%03d", i)
		_, c, err := s.db.Get(kvIndexKey(path, "n1", "colca/v1/_Signal/n1/"+path))
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	// Exercise the real unclean Open after the interrupted recovery finished.
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if n := countIndexKeys(t, s); n != 75 {
		t.Fatal(n)
	}
}

func TestKVIndexRecoveryInvalidatesIncompatibleCheckpoint(t *testing.T) {
	for _, kind := range []string{"heads", "version", "phase", "position", "corrupt"} {
		t.Run(kind, func(t *testing.T) {
			s := mustOpen(t)
			if _, _, err := s.Append("entities", []Record{kvRec("_Signal", "a", `{}`)}); err != nil {
				t.Fatal(err)
			}
			breakIndex(t, s)
			p := kvIndexRecovery{Version: kvIndexRecoveryVersion, Heads: s.kvIndexCleanValue(), Phase: 1, After: []byte("x\x00zzzz")}
			switch kind {
			case "heads":
				p.Heads = []byte("old")
			case "version":
				p.Version++
			case "phase":
				p.Phase = 2
			case "position":
				p.After = []byte("outside")
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "corrupt" {
				raw = []byte("{")
			}
			if err := s.db.Set(kvIndexRecoveryKey, raw, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.reconcileKVIndex(); err != nil {
				t.Fatal(err)
			}
			if n := countIndexKeys(t, s); n != 1 {
				t.Fatalf("index count=%d", n)
			}
			_, c, err := s.db.Get(kvIndexKey("a", "n1", "colca/v1/_Signal/n1/a"))
			if err != nil {
				t.Fatal(err)
			}
			c.Close()
		})
	}
}

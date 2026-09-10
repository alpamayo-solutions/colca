package store

import "encoding/binary"

func be64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

func streamKey(stream string, off uint64) []byte {
	return append([]byte("s\x00"+stream+"\x00"), be64(off)...)
}

// offsetOf reads the offset back out of a stream key (the trailing big-endian
// uint64 streamKey appended).
func offsetOf(key []byte) (uint64, bool) {
	if len(key) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(key)-8:]), true
}
func metaKey(stream string) []byte { return []byte("m\x00" + stream) }

// kvKey identifies one current-state record. Path and node keep hierarchy scans
// efficient; the topic makes the key contract-aware, since several retained
// contracts can share a node and path, such as a _Signal and its latest _Metric.
func kvKey(path, nodeID, topic string) []byte {
	return []byte("k\x00" + path + "\x00" + nodeID + "\x00" + topic)
}
func kvPrefix(prefix string) []byte        { return []byte("k\x00" + prefix) }
func cursorKey(name, stream string) []byte { return []byte("c\x00" + name + "\x00" + stream) }

// ctKey holds a cursor's last-advance time in unix ms, written by CursorAck with
// the cursor. It does not collide with the c\x00 cursor scan, which stops at
// {prefix, 0x01}, and 't' sorts after that.
func ctKey(name, stream string) []byte   { return []byte("ct\x00" + name + "\x00" + stream) }
func hwmKey(child, stream string) []byte { return []byte("h\x00" + child + "\x00" + stream) }
func lwmKey(stream string) []byte        { return []byte("l\x00" + stream) }
func bytesKey(stream string) []byte      { return []byte("b\x00" + stream) }

// rpKey holds a stream's pending state-refresh range: two big-endian uint64s
// [From, To) over KV Offsets, written in the prune batch and cleared once every
// refresh append succeeded.
func rpKey(stream string) []byte { return []byte("rp\x00" + stream) }

func journalKey(stream string, first uint64) []byte {
	return append([]byte("j\x00"+stream+"\x00"), be64(first)...)
}

// journalBounds returns the [lower, upper) iterator bounds covering every
// journal key of a stream: the separator \x00 bumped to \x01 sorts strictly
// after all 8-byte offset suffixes.
func journalBounds(stream string) (lb, ub []byte) {
	return []byte("j\x00" + stream + "\x00"), []byte("j\x00" + stream + "\x01")
}

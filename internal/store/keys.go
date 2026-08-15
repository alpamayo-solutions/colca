package store

import "encoding/binary"

func be64(v uint64) []byte { b := make([]byte, 8); binary.BigEndian.PutUint64(b, v); return b }

func streamKey(stream string, off uint64) []byte {
	return append([]byte("s\x00"+stream+"\x00"), be64(off)...)
}
func streamPrefix(stream string) []byte    { return []byte("s\x00" + stream + "\x00") }
func metaKey(stream string) []byte         { return []byte("m\x00" + stream) }
func kvKey(path, nodeID string) []byte     { return []byte("k\x00" + path + "\x00" + nodeID) }
func kvPrefix(prefix string) []byte        { return []byte("k\x00" + prefix) }
func cursorKey(name, stream string) []byte { return []byte("c\x00" + name + "\x00" + stream) }

// ctKey holds a cursor's last-advance timestamp in unix ms (spec §2:
// ct/{name}/{stream}, written by CursorAck in the same synced write as the
// cursor). The two-byte prefix does NOT collide with the c\x00 cursor scan:
// scanU64Pairs bounds its iteration to {prefix, 0x00}..{prefix, 0x01}, and
// 't' sorts after 0x01.
func ctKey(name, stream string) []byte   { return []byte("ct\x00" + name + "\x00" + stream) }
func hwmKey(child, stream string) []byte { return []byte("h\x00" + child + "\x00" + stream) }
func lwmKey(stream string) []byte        { return []byte("l\x00" + stream) }
func bytesKey(stream string) []byte      { return []byte("b\x00" + stream) }

func journalKey(stream string, first uint64) []byte {
	return append([]byte("j\x00"+stream+"\x00"), be64(first)...)
}

// journalBounds returns the [lower, upper) iterator bounds covering every
// journal key of a stream: the separator \x00 bumped to \x01 sorts strictly
// after all 8-byte offset suffixes.
func journalBounds(stream string) (lb, ub []byte) {
	return []byte("j\x00" + stream + "\x00"), []byte("j\x00" + stream + "\x01")
}

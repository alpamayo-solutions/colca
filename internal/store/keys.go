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
func hwmKey(child, stream string) []byte   { return []byte("h\x00" + child + "\x00" + stream) }

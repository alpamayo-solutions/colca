package store

import "bytes"

// HeadFilter decides on a stored record from its topic and stored signal id
// alone, before the record is decoded. A scan asks it first and decodes only
// the records it keeps, so the records a filtered read crosses cost a look at
// their first and last bytes instead of a JSON decode of their payload.
//
// It must reject only records the reader's full filter rejects as well: a
// record it keeps still goes through that filter. signalID is "" for a record
// stored without one (not a metric, or written before the store kept it), so
// a signal filter decides such a record from its payload, in the full filter.
type HeadFilter func(topic, signalID string) bool

// sidSuffix opens the last field of an encoded recEnc that carries a signal
// id. encoding/json writes the fields in declaration order and SignalID is the
// last one, so a record stored with a signal id ends in `,"sid":"<id>"}`. The
// text cannot occur inside a string value: a quote there is escaped.
var sidSuffix = []byte(`,"sid":"`)

// peekHead returns the topic and the signal id of an encoded recEnc without
// decoding it. ok is false when either is not plain (a backslash escape), and
// the caller decodes the record instead.
func peekHead(val []byte) (topic, signalID string, ok bool) {
	rest, found := bytes.CutPrefix(val, []byte(`{"t":"`))
	if !found {
		if rest, found = bytes.CutPrefix(val, []byte(`{"slo":true,"t":"`)); !found {
			return "", "", false
		}
	}
	end := bytes.IndexByte(rest, '"')
	if end < 0 || bytes.IndexByte(rest[:end], '\\') >= 0 {
		return "", "", false
	}
	topic = string(rest[:end])
	if i := bytes.LastIndex(val, sidSuffix); i >= 0 {
		id, found := bytes.CutSuffix(val[i+len(sidSuffix):], []byte(`"}`))
		if !found || bytes.IndexByte(id, '"') >= 0 || bytes.IndexByte(id, '\\') >= 0 {
			return "", "", false
		}
		signalID = string(id)
	}
	return topic, signalID, true
}

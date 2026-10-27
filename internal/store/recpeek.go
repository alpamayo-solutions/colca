package store

import (
	"bytes"
	"encoding/json"
	"strconv"
)

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

// recHead is what a scan of the stream itself needs of a record: retention,
// compaction, eviction and the log reader decide on these and never on the
// rest of the record.
type recHead struct {
	topic, signalID string
	ts              int64
	// emptyPayload is true for a record without payload: a retraction.
	emptyPayload bool
}

// peekRecord reads a stored record's head without decoding it. encoding/json
// writes recEnc's fields in declaration order, so a record starts with its
// topic, its payload (base64 or null, neither of which holds a quote or an
// escape) and its timestamp. ok is false when anything is not in that plain
// shape; the caller decodes the record instead (headOf).
func peekRecord(val []byte) (h recHead, ok bool) {
	topic, signalID, ok := peekHead(val)
	if !ok {
		return h, false
	}
	h.topic, h.signalID = topic, signalID
	rest := val[bytes.Index(val, []byte(`"t":"`))+len(`"t":"`)+len(topic)+1:]
	rest, found := bytes.CutPrefix(rest, []byte(`,"p":`))
	if !found {
		return h, false
	}
	if after, isNull := bytes.CutPrefix(rest, []byte("null")); isNull {
		h.emptyPayload, rest = true, after
	} else {
		body, found := bytes.CutPrefix(rest, []byte(`"`))
		end := bytes.IndexByte(body, '"')
		if !found || end < 0 {
			return h, false
		}
		h.emptyPayload, rest = end == 0, body[end+1:]
	}
	rest, found = bytes.CutPrefix(rest, []byte(`,"ts":`))
	if !found {
		return h, false
	}
	n := 0
	if n < len(rest) && rest[n] == '-' {
		n++
	}
	for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
		n++
	}
	ts, err := strconv.ParseInt(string(rest[:n]), 10, 64)
	if err != nil || n == len(rest) || (rest[n] != ',' && rest[n] != '}') {
		return h, false
	}
	h.ts = ts
	return h, true
}

// headOf is peekRecord with the decode as its fallback: the head of any
// record that decodes.
func headOf(val []byte) (recHead, error) {
	if h, ok := peekRecord(val); ok {
		return h, nil
	}
	var e recEnc
	if err := json.Unmarshal(val, &e); err != nil {
		return recHead{}, err
	}
	return recHead{topic: e.Topic, signalID: e.SignalID, ts: e.TS, emptyPayload: len(e.Payload) == 0}, nil
}

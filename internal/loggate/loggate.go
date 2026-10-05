// Package loggate bounds the _Log records a node admits from its own services
// and from itself. An error loop or a debug level left on after a deploy must
// not fill the logs stream and the uplink, so the gate does two things per
// window:
//
//   - Repeats collapse. The first record of a key (topic, logger_name,
//     message) is stored; identical records later in its window are withheld
//     and counted. When the window ends with withheld records, one summary
//     record is written at the same topic: the last withheld payload, its
//     message suffixed with the count, and the count in extra.
//   - Each service (a _Log topic without its level) may store MaxPerService
//     records per window. Records beyond that are dropped and counted, and
//     when the window ends one WARNING record at the service's position says
//     how many.
//
// The gate decides and remembers; it never writes or logs. The engine writes
// what Admit, Due and Close return. Logging per record from here would turn
// into _Log records that pass the gate again.
//
// Memory is bounded: at most MaxTracked repeat keys and MaxTracked services
// are held, each repeat key with at most one pending payload of at most
// MaxCollapsiblePayload bytes. A record whose key finds no room is stored
// without collapsing (counted as untracked).
package loggate

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config is the gate's configuration (config.Logs, resolved).
type Config struct {
	// Window is the collapse and rate-cap window. 0 turns the gate off.
	Window time.Duration
	// MaxPerService is the per-service record budget per window. 0 = no cap.
	MaxPerService int
	// MaxTracked bounds the repeat keys and the services held in memory.
	MaxTracked int
}

// Verdict is what the gate decided for one record.
type Verdict int

const (
	// Store: write the record.
	Store Verdict = iota
	// Collapsed: an identical record was stored earlier in the window. Do not
	// write it; it is counted in the window's summary.
	Collapsed
	// RateLimited: the service used up its budget for the window. Do not write
	// it; it is counted in the window's drop notice.
	RateLimited
)

// String is the verdict's metric label and HTTP answer.
func (v Verdict) String() string {
	switch v {
	case Collapsed:
		return "collapsed"
	case RateLimited:
		return "rate_limited"
	default:
		return "stored"
	}
}

// Write is one record the gate asks its owner to write: a collapse summary,
// attributed to the author of the last withheld record, or a drop notice,
// attributed to the node.
type Write[A any] struct {
	Topic   string
	Payload []byte
	Author  A
	// Notice is true for a drop notice, false for a collapse summary.
	Notice bool
}

// Untracked says why a record went past the gate without being remembered.
type Untracked int

const (
	// NotUntracked: the record was tracked.
	NotUntracked Untracked = iota
	// UntrackedRepeat: the repeat table was full; the record is not collapsed.
	UntrackedRepeat
	// UntrackedService: the service table was full; the record is not capped.
	UntrackedService
)

// Gate is safe for concurrent use. A is the attribution type the owner stores
// records with.
type Gate[A any] struct {
	cfg  Config
	node A // the drop notice's author

	mu       sync.Mutex
	closed   bool
	repeats  map[[sha256.Size]byte]*repeat[A]
	services map[string]*service
	due      deadlines
	gen      uint64
	// wake has room for one signal: the earliest deadline moved earlier.
	wake chan struct{}
}

type repeat[A any] struct {
	gen      uint64
	topic    string
	end      time.Time
	count    int
	first    time.Time
	last     time.Time
	payload  []byte // the last withheld record's, held only while count > 0
	author   A
	hasCount bool
}

type service struct {
	gen     uint64
	end     time.Time
	stored  int
	dropped int
}

// New builds a gate. node attributes drop notices.
func New[A any](cfg Config, node A) *Gate[A] {
	return &Gate[A]{
		cfg: cfg, node: node,
		repeats:  map[[sha256.Size]byte]*repeat[A]{},
		services: map[string]*service{},
		wake:     make(chan struct{}, 1),
	}
}

// Enabled reports whether the gate does anything at all.
func (g *Gate[A]) Enabled() bool { return g != nil && g.cfg.Window > 0 }

// Wake is signalled when an earlier deadline appeared; the runner re-arms.
func (g *Gate[A]) Wake() <-chan struct{} { return g.wake }

// Admit decides one _Log record admitted at now. It also returns the writes of
// any window of this record's keys that ended before now, so a summary is
// written before the record that starts the next window. untracked says
// whether a full table let the record past unremembered.
func (g *Gate[A]) Admit(now time.Time, topic string, payload []byte, author A) (Verdict, Untracked, []Write[A]) {
	if !g.Enabled() {
		return Store, NotUntracked, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return Store, NotUntracked, nil
	}
	var writes []Write[A]
	untracked := NotUntracked

	key, collapsible := repeatKey(topic, payload)
	if collapsible {
		if r, ok := g.repeats[key]; ok {
			if !now.Before(r.end) {
				writes = g.endRepeat(key, r, writes)
			} else {
				r.count++
				if !r.hasCount {
					r.first, r.hasCount = now, true
				}
				// Copied: the caller's buffer may be reused once Admit returns.
				r.last, r.payload, r.author = now, bytes.Clone(payload), author
				return Collapsed, NotUntracked, writes
			}
		}
	}

	svcKey := serviceKey(topic)
	if g.cfg.MaxPerService > 0 {
		s, ok := g.services[svcKey]
		if ok && !now.Before(s.end) {
			writes = g.endService(svcKey, s, writes)
			ok = false
		}
		if !ok {
			if len(g.services) >= g.cfg.MaxTracked {
				untracked = UntrackedService
			} else {
				g.gen++
				s = &service{gen: g.gen, end: now.Add(g.cfg.Window)}
				g.services[svcKey] = s
				g.push(entry{at: s.end, gen: s.gen, service: svcKey})
			}
		}
		if s != nil && untracked == NotUntracked {
			if s.stored >= g.cfg.MaxPerService {
				s.dropped++
				return RateLimited, NotUntracked, writes
			}
			s.stored++
		}
	}

	if collapsible {
		if len(g.repeats) >= g.cfg.MaxTracked {
			if untracked == NotUntracked {
				untracked = UntrackedRepeat
			}
		} else {
			g.gen++
			r := &repeat[A]{gen: g.gen, topic: topic, end: now.Add(g.cfg.Window)}
			g.repeats[key] = r
			g.push(entry{at: r.end, gen: r.gen, repeat: key, isRepeat: true})
		}
	}
	return Store, untracked, writes
}

// NextDeadline is when the earliest window ends; ok is false when nothing is
// pending.
func (g *Gate[A]) NextDeadline() (time.Time, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.due) == 0 {
		return time.Time{}, false
	}
	return g.due[0].at, true
}

// Due ends every window that ended at or before now and returns the writes.
func (g *Gate[A]) Due(now time.Time) []Write[A] {
	g.mu.Lock()
	defer g.mu.Unlock()
	var writes []Write[A]
	for len(g.due) > 0 && !g.due[0].at.After(now) {
		e := heap.Pop(&g.due).(entry)
		writes = g.endEntry(e, writes)
	}
	return writes
}

// Close ends every window now and returns the writes. Afterwards the gate
// passes every record: a node that is stopping must not withhold a record it
// can no longer summarize.
func (g *Gate[A]) Close() []Write[A] {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	var writes []Write[A]
	for len(g.due) > 0 {
		e := heap.Pop(&g.due).(entry)
		writes = g.endEntry(e, writes)
	}
	return writes
}

// Tracked reports how many repeat keys and services are held; for tests.
func (g *Gate[A]) Tracked() (repeats, services int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.repeats), len(g.services)
}

func (g *Gate[A]) endEntry(e entry, writes []Write[A]) []Write[A] {
	if e.isRepeat {
		if r, ok := g.repeats[e.repeat]; ok && r.gen == e.gen {
			return g.endRepeat(e.repeat, r, writes)
		}
		return writes
	}
	if s, ok := g.services[e.service]; ok && s.gen == e.gen {
		return g.endService(e.service, s, writes)
	}
	return writes
}

// endRepeat forgets a repeat key and, when it withheld records, returns its
// summary. A stale heap entry for the key is skipped by its generation.
func (g *Gate[A]) endRepeat(key [sha256.Size]byte, r *repeat[A], writes []Write[A]) []Write[A] {
	delete(g.repeats, key)
	if r.count == 0 {
		return writes
	}
	payload, err := summaryPayload(r.payload, r.count, g.cfg.Window, r.first, r.last)
	if err != nil {
		// The payload decoded when the key was made, so this cannot happen;
		// write the last record unchanged rather than lose the line.
		payload = r.payload
	}
	return append(writes, Write[A]{Topic: r.topic, Payload: payload, Author: r.author})
}

func (g *Gate[A]) endService(key string, s *service, writes []Write[A]) []Write[A] {
	delete(g.services, key)
	if s.dropped == 0 {
		return writes
	}
	return append(writes, Write[A]{
		Topic:   key + "/WARNING",
		Payload: dropNotice(key, s.dropped, g.cfg.MaxPerService, g.cfg.Window, s.end),
		Author:  g.node,
		Notice:  true,
	})
}

func (g *Gate[A]) push(e entry) {
	heap.Push(&g.due, e)
	if g.due[0].gen == e.gen {
		select {
		case g.wake <- struct{}{}:
		default:
		}
	}
}

// MaxCollapsiblePayload is the largest record the gate collapses. A repeat key
// holds one pending payload, so this and MaxTracked bound the gate's memory:
// 4096 keys hold at most 256 MiB, and a real error loop holds a few KiB each.
// A larger record is stored as it comes, still within its service's budget.
const MaxCollapsiblePayload = 64 << 10

// repeatKey hashes (topic, logger_name, message). A payload that is not a JSON
// object with a message, or is larger than MaxCollapsiblePayload, cannot be
// collapsed.
func repeatKey(topic string, payload []byte) ([sha256.Size]byte, bool) {
	var fields struct {
		LoggerName *string `json:"logger_name"`
		Message    *string `json:"message"`
	}
	if len(payload) == 0 || len(payload) > MaxCollapsiblePayload ||
		json.Unmarshal(payload, &fields) != nil || fields.Message == nil {
		return [sha256.Size]byte{}, false
	}
	logger := ""
	if fields.LoggerName != nil {
		logger = *fields.LoggerName
	}
	h := sha256.New()
	for _, part := range []string{topic, logger, *fields.Message} {
		h.Write([]byte(strconv.Itoa(len(part))))
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	var key [sha256.Size]byte
	copy(key[:], h.Sum(nil))
	return key, true
}

// serviceKey is the topic without its level segment: node and position.
func serviceKey(topic string) string {
	if i := strings.LastIndexByte(topic, '/'); i > 0 {
		return topic[:i]
	}
	return topic
}

// seconds renders a window for a message: "60", "1.5".
func seconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64)
}

// summaryPayload is the last withheld record with the count in its message
// and in extra.
func summaryPayload(last []byte, count int, window time.Duration, first, lastAt time.Time) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(last))
	dec.UseNumber() // keep line_no and any numbers in extra as sent
	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		return nil, err
	}
	message, _ := body["message"].(string)
	body["message"] = fmt.Sprintf("%s (×%d in %s s)", message, count, seconds(window))
	extra, ok := body["extra"].(map[string]any)
	if !ok {
		extra = map[string]any{}
	}
	extra["repeated"] = count
	extra["repeat_window_s"] = window.Seconds()
	extra["first_repeat_at"] = first.UTC().Format(time.RFC3339Nano)
	extra["last_repeat_at"] = lastAt.UTC().Format(time.RFC3339Nano)
	body["extra"] = extra
	return json.Marshal(body)
}

// dropNotice is the WARNING record that says how many records a service lost.
func dropNotice(key string, dropped, budget int, window time.Duration, end time.Time) []byte {
	name := key
	if i := strings.LastIndexByte(key, '/'); i >= 0 {
		name = key[i+1:]
	}
	body, _ := json.Marshal(map[string]any{
		"timestamp": end.UTC().Format(time.RFC3339Nano),
		"level":     "WARNING",
		"message": fmt.Sprintf("%d log record(s) dropped: %s exceeded %d records in %s s",
			dropped, name, budget, seconds(window)),
		"logger_name": "colca.logs",
		"module":      "loggate",
		"function":    "rate_cap",
		"line_no":     0,
		"extra": map[string]any{
			"dropped":  dropped,
			"window_s": window.Seconds(),
			"service":  name,
		},
	})
	return body
}

// entry is one window end. gen tells a live window from one already ended
// early by a later Admit.
type entry struct {
	at       time.Time
	gen      uint64
	isRepeat bool
	repeat   [sha256.Size]byte
	service  string
}

type deadlines []entry

func (d deadlines) Len() int           { return len(d) }
func (d deadlines) Less(i, j int) bool { return d[i].at.Before(d[j].at) }
func (d deadlines) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d *deadlines) Push(x any)        { *d = append(*d, x.(entry)) }
func (d *deadlines) Pop() any {
	old := *d
	n := len(old)
	e := old[n-1]
	*d = old[:n-1]
	return e
}

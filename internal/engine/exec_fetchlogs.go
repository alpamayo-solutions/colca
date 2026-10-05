// _CmdAdmin fetchLogs: one page of this node's own logs stream for a time
// window, answered in the command's _Ack. A node keeps all of its logs locally
// and forwards only part of them up; this is how an ancestor reads the rest on
// demand, page by page, without streaming the whole log upward.

package engine

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	// FetchLogsDefaultLimit is the page size when the command names none.
	FetchLogsDefaultLimit = 500
	// FetchLogsMaxLimit is the largest page a command may ask for.
	FetchLogsMaxLimit = 1000
	// FetchLogsMaxResultBytes bounds the encoded records of one page. The page
	// rides in an _Ack that is stored in the commands stream and replicated up
	// to every ancestor, so it stays far below limits.max_record_bytes (4 MiB by
	// default); a node configured with a smaller limit halves that instead.
	FetchLogsMaxResultBytes = 256 << 10
	// FetchLogsMaxScan bounds how many records one page examines, matching or
	// not, so a narrow filter over a long window answers promptly with a resume
	// point instead of reading the whole stream.
	FetchLogsMaxScan = 50_000
	// FetchLogsAckMaxAge is how long a fetchLogs ack, and the page in it, stays
	// in the commands stream of the target and of every ancestor. The retention
	// pruner removes older ones once every cursor on the stream passed them,
	// keeping only the newest (see store.PruneSignals).
	FetchLogsAckMaxAge = time.Hour
	// FetchLogsSkew is how far out of append order a record's timestamp may be
	// and still be found. A node's logs stream is mostly in time order, but not
	// strictly: records a child replicates keep the child's timestamps, and a
	// clock step moves the node's own. A page therefore starts its search
	// FetchLogsSkew before from, and the window counts as exhausted only at a
	// record FetchLogsSkew past to.
	FetchLogsSkew = time.Hour
	// FetchLogsMaxConcurrent bounds the fetchLogs scans that run at once on one
	// node; more wait their turn.
	FetchLogsMaxConcurrent = 2
	// FetchLogsPagesPerHour and FetchLogsBurst rate-limit the pages one node
	// answers (a token bucket). Together with FetchLogsAckMaxAge they bound what
	// fetchLogs acks hold in a commands stream: about (1 h + one pruner
	// interval) x 600 x 256 KiB, roughly 160 MiB per answering node, at the
	// node and at each ancestor. A command past the limit is answered 429.
	FetchLogsPagesPerHour = 600
	FetchLogsBurst        = 60
	// fetchLogsEntryDepth is how deeply the ack nests a record's payload:
	// ack object, result object, records array, record object.
	fetchLogsEntryDepth = 4
)

// LogReader is the read surface fetchLogs needs from the node's store.
// Satisfied by *store.Store.
type LogReader interface {
	SeekTS(stream string, ts int64) (uint64, error)
	LWM(stream string) uint64
	NextOffset(stream string) uint64
	EachRecord(stream string, from, upTo uint64, fn func(store.StoredRecord) bool) error
	MaxRecordBytes() uint64
}

// logLevels ranks the level segment of a _Log topic. DEBUG, the default, means
// everything, so a level not listed is returned only then.
var logLevels = map[string]int{
	"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50,
}

// fetchLogsCmd is the fetchLogs payload beside correlation_id and expires_at.
type fetchLogsCmd struct {
	From     *int64  `json:"from"`
	To       *int64  `json:"to"`
	After    *int64  `json:"after"`
	Limit    *int    `json:"limit"`
	MinLevel *string `json:"min_level"`
	Service  *string `json:"service"`
}

// fetchLogsQuery is a validated fetchLogs command.
type fetchLogsQuery struct {
	from, to int64
	after    uint64
	hasAfter bool // after was given: this page resumes an earlier one
	limit    int
	minLevel int
	service  string
}

// FetchLogsRecord is one log record of a fetchLogs page. Truncated replaces a
// payload too large or too deeply nested to carry in the ack; the record is
// still listed so the reader knows it exists.
type FetchLogsRecord struct {
	Offset    uint64          `json:"offset"`
	TS        int64           `json:"ts"`
	Topic     string          `json:"topic"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Truncated bool            `json:"truncated,omitempty"`
}

// FetchLogsResult is the fetchLogs page carried in the ack's result. Next is
// the `after` of the following page and is absent once Complete. LWM is the
// logs stream's lowest retained offset when the page was read; Gap is true when
// records this page should have started with were already removed by
// retention: a resume point below the LWM, or a window that reaches back past
// the oldest retained record.
type FetchLogsResult struct {
	Records  []FetchLogsRecord `json:"records"`
	Next     *uint64           `json:"next,omitempty"`
	Complete bool              `json:"complete"`
	LWM      uint64            `json:"lwm"`
	Gap      bool              `json:"gap"`
}

func parseFetchLogs(payload []byte) (fetchLogsQuery, error) {
	var c fetchLogsCmd
	if err := json.Unmarshal(payload, &c); err != nil {
		return fetchLogsQuery{}, fmt.Errorf("fetchLogs: unreadable payload: %w", err)
	}
	if c.From == nil || c.To == nil {
		return fetchLogsQuery{}, fmt.Errorf("fetchLogs: from and to (unix ms) are required")
	}
	q := fetchLogsQuery{from: *c.From, to: *c.To, limit: FetchLogsDefaultLimit, minLevel: logLevels["DEBUG"]}
	if q.from >= q.to {
		return fetchLogsQuery{}, fmt.Errorf("fetchLogs: from (%d) must be before to (%d)", q.from, q.to)
	}
	if c.After != nil {
		if *c.After < 0 {
			return fetchLogsQuery{}, fmt.Errorf("fetchLogs: after must be a stream offset >= 0, got %d", *c.After)
		}
		q.after, q.hasAfter = uint64(*c.After), true
	}
	if c.Limit != nil {
		if *c.Limit < 1 || *c.Limit > FetchLogsMaxLimit {
			return fetchLogsQuery{}, fmt.Errorf("fetchLogs: limit must be 1..%d, got %d", FetchLogsMaxLimit, *c.Limit)
		}
		q.limit = *c.Limit
	}
	if c.MinLevel != nil {
		rank, ok := logLevels[strings.ToUpper(*c.MinLevel)]
		if !ok {
			return fetchLogsQuery{}, fmt.Errorf("fetchLogs: min_level must be one of CRITICAL, ERROR, WARNING, INFO, DEBUG, got %q", *c.MinLevel)
		}
		q.minLevel = rank
	}
	if c.Service != nil {
		if *c.Service == "" || strings.ContainsAny(*c.Service, "/+#") {
			return fetchLogsQuery{}, fmt.Errorf("fetchLogs: service must be one topic segment, got %q", *c.Service)
		}
		q.service = *c.Service
	}
	return q, nil
}

// matches reports whether a logs-stream record is a _Log record the
// query keeps, apart from its timestamp.
func (q fetchLogsQuery) matches(topic string) bool {
	p, err := uns.Parse(topic)
	if err != nil || p.Contract != "_Log" {
		return false
	}
	segs := strings.Split(p.Path, "/")
	if len(segs) < 2 {
		return false
	}
	service, level := segs[len(segs)-2], segs[len(segs)-1]
	if q.service != "" && service != q.service {
		return false
	}
	return q.minLevel <= logLevels["DEBUG"] || logLevels[level] >= q.minLevel
}

// fetchLogsBudget bounds one page.
type fetchLogsBudget struct {
	bytes int // encoded records
	scan  int // records examined
}

// fetchLogs reads one page of the logs stream: the _Log records with
// from <= ts < to that the filters keep, in offset order.
//
// The guarantee: a record whose timestamp is at most FetchLogsSkew out of
// append order is found. The page starts at the first offset whose timestamp
// is at least from - FetchLogsSkew (binary search), and the window is
// exhausted at the first record with ts >= to + FetchLogsSkew, or at the head
// once to + FetchLogsSkew has passed. A record further out of order (a child's
// backlog replicated more than FetchLogsSkew late, a larger clock step) can be
// missed. The page also stops at the limit, byte and scan budgets, which bound
// the work of a skewed or sparse window; next resumes it.
func fetchLogs(r LogReader, q fetchLogsQuery, now time.Time, budget fetchLogsBudget) (FetchLogsResult, error) {
	const stream = "logs"
	skew := FetchLogsSkew.Milliseconds()
	searchFrom, doneAt := addSat(q.from, -skew), addSat(q.to, skew)
	lwm := r.LWM(stream)
	start, err := r.SeekTS(stream, searchFrom)
	if err != nil {
		return FetchLogsResult{}, err
	}
	res := FetchLogsResult{Records: []FetchLogsRecord{}, LWM: lwm}
	if q.hasAfter {
		res.Gap = lwm > 1 && q.after+1 < lwm
		start = max(start, q.after+1)
	} else {
		// The search found no retained record old enough to lie before the window:
		// what retention removed may have belonged to it.
		res.Gap = lwm > 1 && start <= lwm
	}
	start = max(start, 1) // offsets start at 1
	head := r.NextOffset(stream)

	// last is the last offset this page fully dealt with: the next page resumes
	// after it.
	last := start - 1
	used, scanned := 0, 0
	stopped := false // a budget ended the page before the window did
	err = r.EachRecord(stream, start, head, func(rec store.StoredRecord) bool {
		if scanned >= budget.scan {
			stopped = true
			return false
		}
		scanned++
		if rec.TS >= doneAt {
			res.Complete = true
			return false
		}
		if rec.TS < q.from || rec.TS >= q.to || !q.matches(rec.Topic) {
			last = rec.Offset
			return true
		}
		entry := FetchLogsRecord{Offset: rec.Offset, TS: rec.TS, Topic: rec.Topic, Payload: rec.Payload}
		if !json.Valid(rec.Payload) || nestedDeeperThan(rec.Payload, MaxPayloadDepth-fetchLogsEntryDepth) {
			entry.Payload, entry.Truncated = nil, true
		}
		size := fetchLogsEntrySize(entry)
		if size > budget.bytes {
			entry.Payload, entry.Truncated = nil, true
			size = fetchLogsEntrySize(entry)
		}
		// The first record of a page always fits (truncated if need be), so every
		// page makes progress.
		if used > 0 && used+size > budget.bytes {
			stopped = true
			return false
		}
		used += size
		res.Records = append(res.Records, entry)
		last = rec.Offset
		if len(res.Records) >= q.limit {
			stopped = true
			return false
		}
		return true
	})
	if err != nil {
		return FetchLogsResult{}, err
	}
	if !res.Complete && !stopped {
		// The head was reached. A record can still be appended with a timestamp
		// up to FetchLogsSkew in the past, so the window is over only once its
		// end lies further back than that.
		res.Complete = doneAt <= now.UnixMilli()
	}
	if !res.Complete {
		res.Next = &last
	}
	return res, nil
}

// addSat is a + b, held at the int64 bounds instead of wrapping.
func addSat(a, b int64) int64 {
	s := a + b
	if b > 0 && s < a {
		return math.MaxInt64
	}
	if b < 0 && s > a {
		return math.MinInt64
	}
	return s
}

func fetchLogsEntrySize(entry FetchLogsRecord) int {
	b, err := json.Marshal(entry)
	if err != nil {
		return 0 // unreachable: the payload is valid JSON or omitted
	}
	return len(b) + 1 // the separating comma
}

// fetchLogsBudgetFor is the production budget: FetchLogsMaxScan records and
// FetchLogsMaxResultBytes, or half the node's record limit when that is
// smaller, so the ack always fits.
func fetchLogsBudgetFor(maxRecordBytes uint64) fetchLogsBudget {
	b := fetchLogsBudget{bytes: FetchLogsMaxResultBytes, scan: FetchLogsMaxScan}
	if maxRecordBytes > 0 && maxRecordBytes/2 < FetchLogsMaxResultBytes {
		b.bytes = int(maxRecordBytes / 2) //nolint:gosec // smaller than FetchLogsMaxResultBytes
	}
	return b
}

// pageBucket is the token bucket behind FetchLogsPagesPerHour.
type pageBucket struct {
	mu     sync.Mutex
	tokens float64
	at     time.Time
}

// take spends a token, or says how long until one is available.
func (b *pageBucket) take(now time.Time) (ok bool, wait time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	perSec := float64(FetchLogsPagesPerHour) / 3600
	if b.at.IsZero() {
		b.tokens = FetchLogsBurst
	} else if now.After(b.at) {
		b.tokens = math.Min(FetchLogsBurst, b.tokens+now.Sub(b.at).Seconds()*perSec)
	}
	b.at = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / perSec * float64(time.Second))
}

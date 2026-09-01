package repl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/config"
	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	// downlinkStream is a pseudo-stream name: the cursor tracks PARENT offsets,
	// which are unrelated to the local commands stream — never mix the two.
	downlinkStream = "commands-parent"
	// The definitions half of the same idea: a separate cursor over a separate
	// pseudo-stream, because a node caught up on commands may still be behind
	// on definitions (definition-stream design §5).
	downlinkDefStream = "definitions-parent"

	// metricsStream is the uplink floor — the one stream RunUplink pushes
	// outside the priority lanes. Named rather than written out at each use so
	// that priorityLanes plus this constant are the whole answer to "what
	// rises", and uplinkStreams can derive its set from them instead of
	// restating it.
	metricsStream = "metrics"

	// The pre-scoping cursor names (parent-scoped-cursors design §3.5). They
	// exist for exactly one purpose: to be adopted once under the parent-scoped
	// names on the first start after this change, and then deleted. Nothing
	// writes them any more.
	legacyUplinkCursor      = "uplink"
	legacyDownlinkCursor    = "downlink"
	legacyDownlinkDefCursor = "downlink-def"

	replBatch    = maxReplicateRecords
	uplinkIdle   = 150 * time.Millisecond
	downlinkWait = 20 * time.Second
	retryAfter   = 500 * time.Millisecond
)

type Client struct {
	base string
	// parentPub is the parent's PINNED public key — the identity every
	// connection verifies, and the scope key for this child's replication
	// cursors (parent-scoped-cursors design §3.1). Cursor NAMES are built from
	// it via uns.UplinkCursor/DownlinkCursor/DownlinkDefCursor at every call
	// site, so a reparent (a different parentPub) never resumes against the
	// old parent's offsets, and returning to a former parent finds its old
	// position intact.
	parentPub        string
	http             *http.Client
	log              *slog.Logger
	maxReplicateBody int64
	// Whether each replication lane is currently failing, so an outage logs
	// as a state change rather than once per retry (see linkstate.go).
	links *linkState
}

// NewClient: TLS client presenting the child's cert, pinning the parent's pubkey.
func NewClient(baseURL, parentPubHex string, id *identity.Identity, maxRecordBytes ...uint64) (*Client, error) {
	cert, err := id.SelfSignedCert("colca-child")
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true, // trust = key pinning below, not CAs
		MinVersion:         tls.VersionTLS13,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return fmt.Errorf("parent presented no certificate")
			}
			pub, err := identity.PeerPubHex(rawCerts[0])
			if err != nil {
				return err
			}
			if pub != parentPubHex {
				return fmt.Errorf("parent key mismatch: got %s want %s", short(pub), short(parentPubHex))
			}
			return nil
		},
	}
	limits := config.Limits{}
	if len(maxRecordBytes) > 0 {
		limits.MaxRecordBytes = config.ByteSize(maxRecordBytes[0])
	}
	return &Client{
		base:             baseURL,
		parentPub:        parentPubHex,
		http:             &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}, Timeout: 30 * time.Second},
		log:              slog.Default().With("comp", "repl-client"),
		maxReplicateBody: replicateBodyLimit(&config.Config{Limits: limits}),
		links:            newLinkState(),
	}, nil
}

// ParentPub is the pinned parent key this client is bound to — the scope of
// every cursor the repl loops maintain against it.
func (c *Client) ParentPub() string { return c.parentPub }

func (c *Client) Replicate(stream string, recs []store.ReplRecord) (hwm uint64, err error) {
	hwm, _, err = c.replicate(context.Background(), stream, recs)
	return hwm, err
}

// replicate is the ctx-carrying implementation Replicate and RunUplink share.
// nowMS is the parent's now_ms from the response envelope (time-sync design
// §2.1) — callers that care about clock sync (RunUplink) apply it via
// engine.ApplyClockSample; Replicate's exported wrapper drops it, since
// direct callers (tests) do not need it.
func (c *Client) replicate(ctx context.Context, stream string, recs []store.ReplRecord) (hwm uint64, nowMS int64, err error) {
	if len(recs) > maxReplicateRecords {
		return 0, 0, fmt.Errorf("replicate: batch has %d records, maximum is %d", len(recs), maxReplicateRecords)
	}
	body, err := marshalReplication(stream, recs)
	if err != nil {
		return 0, 0, err
	}
	if int64(len(body)) > c.maxReplicateBody {
		return 0, 0, fmt.Errorf("replicate: request is %d bytes, maximum is %d", len(body), c.maxReplicateBody)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/replicate", bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("replicate to %s: %s", c.base, replicationStatusMeaning(resp.StatusCode))
	}
	var out struct {
		HWM   uint64 `json:"hwm"`
		NowMS int64  `json:"now_ms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, 0, err
	}
	return out.HWM, out.NowMS, nil
}

func marshalReplication(stream string, recs []store.ReplRecord) ([]byte, error) {
	wire := make([]wireRec, len(recs))
	for i, r := range recs {
		wire[i] = wireRec{
			O: r.ChildOffset, OO: r.OriginOffset, T: r.Topic, P: r.Payload, TS: r.TS,
			WB: r.WrittenBy, AID: r.ActorID, AL: r.ActorLabel, AK: r.ActorKind,
		}
	}
	return json.Marshal(map[string]any{"stream": stream, "records": wire})
}

// fitReplicationBatch returns the largest non-empty prefix whose exact JSON
// envelope fits the server contract. The normal 200-record batch needs one
// marshal; binary search is used only for unusually large records.
func fitReplicationBatch(stream string, batch []store.ReplRecord, maxBytes int64) ([]store.ReplRecord, error) {
	body, err := marshalReplication(stream, batch)
	if err != nil || int64(len(body)) <= maxBytes {
		return batch, err
	}
	low, high := 1, len(batch)-1
	best := 0
	for low <= high {
		mid := low + (high-low)/2
		body, err := marshalReplication(stream, batch[:mid])
		if err != nil {
			return nil, err
		}
		if int64(len(body)) <= maxBytes {
			best = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	if best == 0 {
		return nil, fmt.Errorf("replicate: one record exceeds the %d-byte request limit", maxBytes)
	}
	return batch[:best], nil
}

type DownRec struct {
	ParentOffset uint64
	Topic        string
	Payload      []byte
	TS           int64
	WrittenBy    string
	ActorID      string
	ActorLabel   string
	ActorKind    string
}

// downResult is one decoded downlink response. Definitions ride the same
// response as commands but keep their own records and their own cursor: the two
// streams advance independently (definition-stream design §5).
type downResult struct {
	Records     []DownRec
	Next        uint64
	Gap         *store.GapSpan
	NowMS       int64
	Ancestry    *uns.Ancestry
	Definitions []DownRec
	DefNext     uint64
	// Head is the parent's own commands stream head (NextOffset("commands"))
	// at hello time — the position a child with no cursor for THIS parent must
	// start from (parent-scoped-cursors design §3.2/§3.3). The HELLO response
	// carries it and nothing else does: it is consumed once, before the first
	// poll, so a copy on every poll would be an integer no one reads. 0 means
	// the parent did not send one, which on the hello path means a parent that
	// predates the field — RunDownlink reports that.
	Head uint64
}

// Downlink polls the parent once. gap is non-nil when the poll position lies
// inside a hole the parent's retention pruned (spec §6.2) — next then already
// points past it.
func (c *Client) Downlink(after uint64, max int, timeout time.Duration) ([]DownRec, uint64, *store.GapSpan, error) {
	res, err := c.downlink(context.Background(), after, 1, max, timeout)
	if err != nil {
		return nil, 0, nil, err
	}
	return res.Records, res.Next, res.Gap, nil
}

// DownlinkWithAncestry is Downlink plus the parent-taught position (id-grants
// design §4); ancestry is nil when the parent did not hand one down.
func (c *Client) DownlinkWithAncestry(after uint64, max int, timeout time.Duration) ([]DownRec, uint64, *store.GapSpan, *uns.Ancestry, error) {
	res, err := c.downlink(context.Background(), after, 1, max, timeout)
	if err != nil {
		return nil, 0, nil, nil, err
	}
	return res.Records, res.Next, res.Gap, res.Ancestry, nil
}

// DownlinkDefinitions is Downlink from the definitions side: the records the
// parent handed down and the next position on that stream
// (definition-stream design §5).
func (c *Client) DownlinkDefinitions(defAfter uint64, max int, timeout time.Duration) ([]DownRec, uint64, error) {
	res, err := c.downlink(context.Background(), 1, defAfter, max, timeout)
	if err != nil {
		return nil, 0, err
	}
	return res.Definitions, res.DefNext, nil
}

// hello performs the immediate-answer first contact (id-grants design §4):
// no long poll, no records consumed — it exists to learn the position, and
// whatever definitions are already waiting, in one RTT right after the loop
// starts.
func (c *Client) hello(ctx context.Context, defAfter uint64) (downResult, error) {
	return c.downlinkURL(ctx, fmt.Sprintf("%s/downlink?after=1&max=1&hello=1&def_after=%d",
		c.base, defAfter), 10*time.Second)
}

// downlink is the ctx-carrying implementation the wrappers and RunDownlink
// share. NowMS is the parent's now_ms from the response envelope (time-sync
// design §2.1) — RunDownlink applies it via engine.ApplyClockSample before
// ingesting anything (design §2.3 rule 4). Ancestry is the parent-taught
// position (id-grants design §4), nil when the parent did not hand one down.
func (c *Client) downlink(ctx context.Context, after, defAfter uint64, max int, timeout time.Duration) (downResult, error) {
	return c.downlinkURL(ctx, fmt.Sprintf("%s/downlink?after=%d&def_after=%d&max=%d",
		c.base, after, defAfter, max), timeout)
}

func (c *Client) downlinkURL(ctx context.Context, url string, timeout time.Duration) (downResult, error) {
	hc := *c.http
	hc.Timeout = timeout + 10*time.Second
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return downResult{}, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return downResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return downResult{}, fmt.Errorf("downlink from %s: %s", c.base, replicationStatusMeaning(resp.StatusCode))
	}
	var out struct {
		Records     []wireRec      `json:"records"`
		Next        uint64         `json:"next"`
		Gap         *store.GapSpan `json:"gap"`
		NowMS       int64          `json:"now_ms"`
		Ancestry    *uns.Ancestry  `json:"ancestry"`
		Definitions []wireRec      `json:"definitions"`
		DefNext     uint64         `json:"def_next"`
		Head        uint64         `json:"head"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return downResult{}, err
	}
	return downResult{
		Records:     toDownRecs(out.Records),
		Next:        out.Next,
		Gap:         out.Gap,
		NowMS:       out.NowMS,
		Ancestry:    out.Ancestry,
		Definitions: toDownRecs(out.Definitions),
		DefNext:     out.DefNext,
		Head:        out.Head,
	}, nil
}

func toDownRecs(in []wireRec) []DownRec {
	out := make([]DownRec, len(in))
	for i, r := range in {
		out[i] = DownRec{
			ParentOffset: r.O, Topic: r.T, Payload: r.P, TS: r.TS,
			WrittenBy: r.WB, ActorID: r.AID,
			ActorLabel: r.AL, ActorKind: r.AK,
		}
	}
	return out
}

// ackOnly passes exactly the two command-stream records that may rise. A
// command is never mirrored back to the node it came from; _StreamGap markers
// must pass so a pruned commands stream stays honest upstream (spec §6.4: the
// marker replicates like any other record).
func ackOnly(topic string) bool {
	p, err := uns.Parse(topic)
	return err == nil && (p.Contract == "_Ack" || p.Contract == "_StreamGap")
}

// priorityLanes are drained to empty, in this order, before `metrics` is
// touched at all (alarm-stream design §4).
//
// Acks first: a parent's move-drain cannot complete until they rise, so an
// undelivered ack blocks a node move, and their volume is the smallest of all.
// Then alarms and entities — small in volume, high in value, and the two the
// operator is waiting for after an outage. Then audit, low-volume and
// long-retention. Then annotations — dataops-evaluator design §8: still ahead
// of the metrics backlog, so a producer's output reaches the root promptly,
// but behind audit because a security event outranks it.
//
// `definitions` is deliberately absent and must stay absent (definition-stream
// design §4): definitions descend. A child pushing them upward would let a
// leaf author policy for the whole tree.
var priorityLanes = []struct {
	name   string
	filter func(string) bool
}{
	{"commands", ackOnly},
	{"alarms", nil},
	{"entities", nil},
	{"audit", nil},
	{"annotations", nil},
	// Last of the priority lanes, ahead of the metrics floor: a log line is
	// worth less than an alarm and more than a sample, and it must never
	// starve the samples the way it did when it shared their lane.
	{"logs", nil},
}

// uplinkStreams is exactly the set RunUplink pushes: every priority lane plus
// the metrics floor. Derived from the lane list rather than restated, because a
// second hand-written copy of the stream set is a copy someone eventually
// forgets to extend — and the one that silently stops seeding a cursor here
// would be invisible until a reparented node came up deaf on that stream.
func uplinkStreams() []string {
	streams := make([]string, 0, len(priorityLanes)+1)
	for _, lane := range priorityLanes {
		streams = append(streams, lane.name)
	}
	return append(streams, metricsStream)
}

// adoptLegacy moves a pre-scoping cursor's VALUE to its parent-scoped name and
// removes the old key (design §3.5). It reports whether there was anything to
// adopt, so the caller can tell "migrated" from "genuinely first contact".
//
// One shot by construction: the legacy key is gone afterwards, so a second
// start finds nothing here and the scoped cursor — which by then is the only
// one — carries the position on alone. A delete that fails is logged rather
// than retried; the adoption itself already landed, so the worst case is that
// the next start adopts the same value again, which is a no-op against a
// forward-only cursor.
func adoptLegacy(c *Client, st *store.Store, legacyName, scopedName, stream string) bool {
	pos := st.CursorGet(legacyName, stream)
	if pos <= 1 {
		return false // never advanced (or never existed): nothing to carry over
	}
	st.CursorAck(scopedName, stream, pos)
	if err := st.CursorDelete(legacyName, stream); err != nil {
		c.log.Warn("legacy cursor not deleted after adoption — its value is already carried under the scoped name",
			"cursor", legacyName, "stream", stream, "err", err)
	}
	return true
}

// initCursors settles this child's position against the CONFIGURED parent
// before either loop reads a cursor (parent-scoped-cursors design §3.2/§3.5).
//
// Three cases, evaluated per cursor in this order:
//
//  1. A scoped cursor already exists — leave it alone. This is every
//     steady-state start, and every return to a FORMER parent, which resumes
//     exactly where it left off because its cursors were never touched while
//     the node was attached elsewhere.
//  2. A legacy un-scoped cursor exists and a scoped one does not — adopt the
//     legacy VALUE under the scoped name and delete the legacy key (§3.5).
//     Correct for every node that is not mid-reparent, which is every running
//     node, since a reparent already requires a restart.
//  3. Neither exists — first contact with THIS parent, so initialize rather
//     than assume:
//     - Uplink starts at each stream's LWM: offer everything still retained
//     and let the parent's per-child HWM dedup whatever it already has. This
//     is the state transfer reparenting was missing. Seeding it here rather
//     than letting pushOnce's §6.3 clamp arrive at the same offset also keeps
//     first contact from REPORTING a gap: a gap means records were lost
//     against a position this parent held, and it never held one.
//     - Definitions start at 1, which is simply the default — not special
//     handling, but exactly what a freshly enrolled child does. The stream is
//     compacted rather than pruned, so "from 1" is the current definition set.
//     - Commands start at the parent's head. Instructions issued before this
//     child attached were addressed to whatever occupied the mount then;
//     delivering them to a newcomer would run a command its author never
//     meant for it.
//
// The uplink and definitions halves test case 1 with "position above 1", which
// is imprecise but harmless there: re-running case 3 on a cursor sitting at 1
// re-seeds the LWM, and a forward-only ack makes that a no-op whenever it would
// not already have happened. The commands half cannot afford the imprecision
// and probes for the key itself — see there.
//
// head is the parent's commands NextOffset from a hello response. 0 means no
// hello has answered — RunUplink always passes 0, because the uplink half needs
// no head and a metric it can never emit is worse than none.
//
// Idempotent, and safe under the race the two loops create by both calling it.
// Every write here either moves a cursor forward (CursorAck) or claims one that
// does not exist (CursorSetIfAbsent), and every delete is a no-op on an absent
// key — so concurrent calls converge on the same set no matter which order they
// interleave in, and none of them can move a cursor backwards.
//
// m may be nil (every Metrics method is nil-safe).
func initCursors(c *Client, eng *engine.Engine, m *metrics.Metrics, head uint64) {
	st := eng.Store()

	up := uns.UplinkCursor(c.parentPub)
	for _, stream := range uplinkStreams() {
		if st.CursorGet(up, stream) > 1 {
			continue // case 1
		}
		if adoptLegacy(c, st, legacyUplinkCursor, up, stream) { // case 2
			continue
		}
		if lwm := st.LWM(stream); lwm > 1 { // case 3
			st.CursorAck(up, stream, lwm)
		}
	}

	// Definitions: case 3 IS the default position, so there is nothing to write
	// unless a legacy cursor has a position to hand over.
	def := uns.DownlinkDefCursor(c.parentPub)
	if st.CursorGet(def, downlinkDefStream) <= 1 {
		adoptLegacy(c, st, legacyDownlinkDefCursor, def, downlinkDefStream)
	}

	// Commands are the one cursor where the three cases must be told apart
	// EXACTLY, so this branch probes for the key rather than for a position
	// above 1. CursorGet answers 1 both for "never met this parent" and for "met
	// it, and its stream was empty at the time" — and those two demand opposite
	// behaviour: the first adopts the head, the second must deliver everything
	// from offset 1, which is exactly the offline-catch-up contract (cmdadmin
	// design §10: a command issued while its target is down waits durably and
	// executes on restart). Adopting a head there would drop it.
	cmd := uns.DownlinkCursor(c.parentPub)
	if !adoptLegacy(c, st, legacyDownlinkCursor, cmd, downlinkStream) { // case 2
		// Case 3, and only on genuine first contact: SetIfAbsent leaves a cursor
		// that already exists untouched (case 1) and records the position even
		// when it is 1, so the next start knows this parent has been met. head 0
		// means no hello has answered — claim nothing and let the caller that
		// has a head settle it.
		if head > 0 {
			if _, err := st.CursorSetIfAbsent(cmd, downlinkStream, head); err != nil {
				// Same consequence as a parent with no head at all: with nothing
				// recorded, CursorGet answers 1 and the first poll reads the
				// pre-attachment commands §3.2 declines.
				c.log.Warn("first-contact commands cursor not recorded — this node polls from 1 and may "+
					"execute commands issued under its mount before it attached",
					"cursor", cmd, "head", head, "err", err)
			}
		}
	}
	// The one diagnostic head buys (§7), evaluated after the cases above so a
	// cursor this call just created (pos == head) can never trip it. A position
	// past the parent's head means the parent pruned past it or was rebuilt from
	// empty: Read(after > head) returns nothing, next == after, and the node
	// stays command-deaf without ever failing at anything — and being
	// command-deaf, it cannot be repaired remotely either.
	//
	// Detection only — never reset it here. A parent whose stream is shorter
	// than this cursor is also exactly what a legitimately pruned parent looks
	// like, and rewinding would re-deliver commands that already ran.
	if pos := st.CursorGet(cmd, downlinkStream); head > 0 && pos > head {
		c.log.Warn("downlink commands cursor is past the parent's head — the parent pruned past "+
			"this position or was rebuilt; this node will receive no commands until the parent's "+
			"stream grows past it",
			"position", pos, "parent_head", head, "parent_key", short(c.parentPub))
		m.DownlinkCursorBeyondHead()
	}
}

// RunUplink pushes this node's streams to its parent forever (until stop is
// closed), draining the priority lanes to empty before each metrics batch.
//
// Order within a stream is never changed, so a lane is the ONLY way a record
// can overtake a backlog — which is why alarms have their own stream rather
// than a place in the metrics queue.
//
// The single metrics batch per pass is a floor, not a courtesy. Without it a
// lane that never empties holds the metrics cursor still; once the local
// pruner passes that cursor the backlog is gone and only a §6.4 marker
// remains, so lane pressure would quietly become data loss.
//
// blobs is the local blob store this node offers to its parent; nil-safe:
// syncBlobs no-ops when it is nil. m may be nil (every Metrics method is
// nil-safe).
func RunUplink(c *Client, eng *engine.Engine, blobs *blobstore.Store, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	// Settle this node's position against the configured parent before the
	// first cursor read below. head is 0: the uplink half needs none, and
	// RunDownlink's hello supplies it for the commands cursor. m is nil for the
	// same reason — the only metric this can emit belongs to the commands
	// cursor, which a head of 0 never reaches.
	initCursors(c, eng, nil, 0)

	// Scoped to this client, and therefore to this pinned parent key. Keyed
	// by digest, valued by the local blob's Modified time at confirmation
	// (blobs.go's syncBlobs doc comment) so a swept-then-recreated blob is
	// recognized as needing a re-push rather than skipped forever.
	confirmedBlobs := map[string]time.Time{}
	// A blob this parent has permanently refused (a 4xx: bad digest, or over
	// its cap). Session-scoped like confirmedBlobs, for the same reason: a
	// reparent re-offers everything, because a new parent may accept what
	// this one wouldn't.
	rejectedBlobs := map[string]bool{}

	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// pushOnce moves at most one batch of stream and reports whether it
	// scanned anything — which is what "drained to empty" is measured by.
	// aborted distinguishes our own shutdown from a real failure.
	pushOnce := func(stream string, filter func(string) bool) (scanned, aborted bool) {
		from := eng.Store().CursorGet(uns.UplinkCursor(c.parentPub), stream)
		// Spec §6.3, uplink half: a cursor below the local LWM means the
		// local pruner overrode it (only possible after the explicit §5.2
		// staleness opt-in — the parent was gone longer than the window).
		// The data is gone and the durable §6.4 marker already carries the
		// fact upstream, so never stall: jump to the LWM and keep going.
		if lwm := eng.Store().LWM(stream); from < lwm {
			c.log.Error("uplink cursor below the stream LWM — local retention pruned past it (spec §6.3): jumping to the LWM",
				"stream", stream, "position", from, "lwm", lwm)
			m.GapReceived(stream)
			eng.Store().CursorAck(uns.UplinkCursor(c.parentPub), stream, lwm)
			from = lwm
		}
		recs, next, err := eng.Store().Read(stream, from, replBatch, filter)
		if err != nil {
			c.log.Error("uplink read", "stream", stream, "err", err)
			return false, false
		}
		if next == from {
			return false, false // nothing scanned: this lane is empty
		}
		pushed := false
		if len(recs) > 0 {
			batch := make([]store.ReplRecord, len(recs))
			for i, r := range recs {
				batch[i] = store.ReplRecord{
					ChildOffset: r.Offset, OriginOffset: r.OriginOffset,
					Topic: r.Topic, Payload: r.Payload, TS: r.TS,
					WrittenBy: r.WrittenBy, ActorID: r.ActorID,
					ActorLabel: r.ActorLabel, ActorKind: r.ActorKind,
				}
			}
			batch, err = fitReplicationBatch(stream, batch, c.maxReplicateBody)
			if err != nil {
				c.log.Error("uplink batch cannot fit the replication request bound", "stream", stream, "err", err)
				m.UplinkPushFailed(stream)
				return false, false
			}
			if len(batch) < len(recs) {
				next = batch[len(batch)-1].ChildOffset + 1
			}
			_, nowMS, err := c.replicate(ctx, stream, batch)
			if err != nil {
				if stopped() {
					return false, true // aborted by our own shutdown, not a failure
				}
				if report, attempts, down := c.links.Failed("uplink:"+stream, time.Now()); report {
					c.log.Warn("uplink is down (retrying)",
						"stream", stream, "parent", c.base,
						"attempts", attempts, "down_for", down.Round(time.Second), "err", err)
				}
				m.UplinkPushFailed(stream)
				// Parent down → cursor stays, offline buffering in action. Report
				// "not scanned" so a dead parent ends the drain loop instead of
				// spinning on a lane that cannot advance.
				return false, false
			}
			if wasFailing, attempts, down := c.links.Recovered("uplink:"+stream, time.Now()); wasFailing {
				c.log.Info("uplink recovered",
					"stream", stream, "parent", c.base,
					"attempts", attempts, "down_for", down.Round(time.Second))
			}
			// Every /replicate response carries the parent's now_ms
			// (time-sync design §2.1) — keep the offset fresh regardless
			// of which stream happened to trigger this push.
			eng.ApplyClockSample(nowMS)
			pushed = true
		}
		eng.Store().CursorAck(uns.UplinkCursor(c.parentPub), stream, next)
		if pushed {
			m.UplinkPushed(stream, time.Now())
		}
		return true, false
	}

	for {
		if stopped() {
			return
		}
		idle := true
		for _, lane := range priorityLanes {
			// Snapshot the lane's end at pass start and drain only to there.
			// Everything already queued goes before metrics is touched; records
			// written DURING the pass wait for the next one.
			//
			// Draining to "empty" instead would be unbounded: a lane written
			// faster than it drains never empties, the loop never reaches the
			// metrics floor below, and the floor stops holding in exactly the
			// case it exists for.
			target := eng.Store().NextOffset(lane.name)
			for eng.Store().CursorGet(uns.UplinkCursor(c.parentPub), lane.name) < target {
				scanned, aborted := pushOnce(lane.name, lane.filter)
				if aborted {
					return
				}
				if !scanned {
					break // nothing scanned (empty, or the parent is unreachable)
				}
				idle = false
				if stopped() {
					return
				}
			}
		}
		// The floor: exactly one metrics batch per pass, whatever the lanes
		// above are doing.
		scanned, aborted := pushOnce(metricsStream, nil)
		if aborted {
			return
		}
		if scanned {
			idle = false
		}
		// Blobs last: they are not in any stream, and a file must never delay
		// a record. A pass that pushed nothing costs one HEAD per unconfirmed
		// blob, which is why confirmations are remembered.
		syncBlobs(c, blobs, m, confirmedBlobs, rejectedBlobs)
		if idle {
			select {
			case <-stop:
				return
			case <-time.After(uplinkIdle):
			}
		}
	}
}

// RunDownlink fetches from the parent and hands the result to the engine:
// commands are executed at their target, definitions are applied wherever they
// land (definition-stream design §5). m may be nil (every Metrics method is
// nil-safe).
func RunDownlink(c *Client, eng *engine.Engine, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	// First contact: learn the node's position, and whatever definitions are
	// already waiting, in one RTT (id-grants design §4) instead of after the
	// first long-poll drains.
	//
	// Retried until it answers. A failed hello used to be harmless — position
	// and definitions ride every poll anyway — but since parent-scoped cursors
	// the head it carries is a PRECONDITION of the first poll: a child with no
	// cursor for this parent must adopt that head before it reads anything
	// (§3.2), and polling first would hand it every instruction issued before it
	// was attached. One transient failure would otherwise defeat the rule
	// outright. Waiting costs nothing — while hello is failing the parent is
	// unreachable, so the poll below would be failing too, and it counts as the
	// fetch failure it is.
	for {
		res, err := c.hello(ctx, eng.Store().CursorGet(uns.DownlinkDefCursor(c.parentPub), downlinkDefStream))
		if err == nil {
			// A parent that answers without a head predates the field (§3.3) —
			// a mixed-version tree during a leaf-first rolling upgrade. Nothing
			// here can repair that: with no head there is no position to adopt,
			// so a commands cursor this node does not have yet stays at its
			// default of 1 and the poll below hands it every retained command
			// issued under its mount before it attached, which is exactly what
			// §3.2 exists to refuse.
			//
			// Said on every start against such a parent, not only on genuine
			// first contact — hello runs once per process and this branch reads
			// the response alone, before initCursors has asked whether a cursor
			// for this parent exists. A node that already holds one adopts
			// nothing, risks nothing, and still logs: the line names a
			// mixed-version parent, and only the first-contact case behind it is
			// a hazard. Leaving the distinction to the reader rather than
			// probing the store here keeps one meaning per branch — the §7
			// diagnostic below cannot speak for this case either, since it needs
			// a head of its own to compare against.
			if res.Head == 0 {
				c.log.Warn("parent answered hello without a command head — it predates parent-scoped "+
					"cursors; if this node has no commands cursor for it yet, that cursor starts at 1 "+
					"and it may execute commands issued under its mount before it attached "+
					"(design §3.2/§3.3)",
					"parent", c.base, "parent_key", short(c.parentPub))
				m.DownlinkHeadAbsent()
			}
			// Before anything is applied: a first-contact definitions cursor must
			// be settled at 1 when this batch lands, and a first-contact commands
			// cursor must have adopted the head this response carries before the
			// poll below reads it (§3.2).
			initCursors(c, eng, m, res.Head)
			if res.Ancestry != nil {
				eng.SetAncestry(*res.Ancestry)
			}
			_ = applyDefinitions(c, eng, m, res)
			if wasFailing, attempts, waited := c.links.Recovered("hello", time.Now()); wasFailing {
				c.log.Info("first contact with the parent succeeded",
					"parent", c.base, "attempts", attempts, "waited", waited.Round(time.Second))
			}
			break
		}
		select {
		case <-stop:
			return // the request was aborted by our own shutdown
		default:
		}
		if report, attempts, down := c.links.Failed("hello", time.Now()); report {
			c.log.Warn("first contact with the parent has not succeeded yet (retrying) — "+
				"its command head must be known before the first poll",
				"parent", c.base, "attempts", attempts, "down_for", down.Round(time.Second), "err", err)
		}
		m.DownlinkFetchFailed()
		select {
		case <-stop:
			return
		case <-time.After(retryAfter):
		}
	}
	for {
		select {
		case <-stop:
			return
		default:
		}
		after := eng.Store().CursorGet(uns.DownlinkCursor(c.parentPub), downlinkStream)
		defAfter := eng.Store().CursorGet(uns.DownlinkDefCursor(c.parentPub), downlinkDefStream)
		res, err := c.downlink(ctx, after, defAfter, replBatch, downlinkWait)
		recs, next, gap, nowMS, ancestry := res.Records, res.Next, res.Gap, res.NowMS, res.Ancestry
		if err != nil {
			select {
			case <-stop:
				return // the request was aborted by our own shutdown
			default:
			}
			c.log.Warn("downlink fetch failed (will retry)", "err", err)
			m.DownlinkFetchFailed()
			select {
			case <-stop:
				return
			case <-time.After(retryAfter):
			}
			continue
		}
		m.DownlinkFetched(time.Now())
		// Time-sync design §2.3 rule 4: the offset learned from this
		// response is applied BEFORE its records are ingested, so the first
		// poll after reconnect refreshes time before any expiry decision
		// downstream of it.
		eng.ApplyClockSample(nowMS)
		if ancestry != nil {
			eng.SetAncestry(*ancestry) // idempotent: every poll may carry it
		}
		if gap != nil {
			// Spec §6.3, downlink half: log, count, continue — next already
			// points past the hole and the cursor advances through the normal
			// ack below. Nothing propagates further down: pruned commands are,
			// by the §3.4 config rule, commands whose TTL had already expired.
			c.log.Error("downlink gap: the parent pruned commands this node never received (spec §6.3) — continuing past the hole",
				"from_offset", gap.FromOffset, "to_offset", gap.ToOffset,
				"first_ts", gap.FirstTS, "last_ts", gap.LastTS, "approx", gap.Approx)
			m.GapReceived("commands")
		}
		// A record this node REFUSED is skipped; a record it could not STORE
		// holds the cursor at its offset, so the parent offers it again next
		// poll.
		//
		// The two are the same error return and opposite obligations. A
		// refusal (draining, grammar — every one an engine.RejectError) is
		// this node's own decision and will be made identically forever, so
		// re-reading it would stall the lane for nothing. A store failure —
		// a full disk, an I/O error, a record larger than this node's
		// max_record_bytes — says nothing about the record: acking past it
		// drops a command that survived a whole parent outage durably, with
		// one log line, and leaves its issuer watching a target that will
		// never answer. Definitions on the same response always had this
		// treatment; commands did not.
		ackTo := next
		for _, r := range recs {
			if _, err := eng.IngestDownlinkAttributed(r.Topic, r.Payload, r.TS, engine.Attribution{
				WrittenBy: r.WrittenBy, ActorID: r.ActorID,
				ActorLabel: r.ActorLabel, ActorKind: r.ActorKind,
			}); err != nil {
				var refused *engine.RejectError
				if errors.As(err, &refused) {
					c.log.Error("downlink command refused by this node — skipping past it",
						"topic", r.Topic, "reason", refused.Reason, "err", err)
					continue
				}
				c.log.Error("downlink command not stored — holding the cursor so the parent offers it again",
					"topic", r.Topic, "parent_offset", r.ParentOffset, "err", err)
				ackTo = r.ParentOffset
				break
			}
		}
		progressed := applyDefinitions(c, eng, m, res)
		if ackTo > after {
			eng.Store().CursorAck(uns.DownlinkCursor(c.parentPub), downlinkStream, ackTo)
			progressed = true
		}
		if !progressed {
			// Neither cursor moved. Usually that is an idle long poll, which
			// already waited out its 20s and loses nothing by waiting a little
			// longer. The case that matters is a poll that answered
			// IMMEDIATELY and left this node exactly where it was — a parent
			// answering with a position we already hold, or a record this node
			// could not apply — where polling straight back would spin at the
			// rate limit for as long as the condition lasts.
			select {
			case <-stop:
				return
			case <-time.After(retryAfter):
			}
		}
	}
}

// applyDefinitions stores what the parent handed down and advances the
// definitions cursor. It reports whether the cursor moved.
//
// The same rule as the command half above, for the same reasons: a definition
// this node could not STORE holds the cursor at its offset, because unlike a
// command there is no read side to recover it from later (which is also why
// the stream is compacted rather than pruned — definition-stream design §6).
// A definition this node REFUSED is skipped.
//
// That second case is a rolling upgrade, not a fault. A hub upgraded before
// its edges authors a definition contract the older bundle downstream does
// not know (this branch added _DataModel and PAT records exactly that way);
// every child classifies it as unknown and refuses it. Re-offering it forever
// would park the channel there, so no LATER definition — new groups, new
// types, a revoked group's tombstone — would reach that node until someone
// upgraded it. One warning per skipped record, and the rest of policy keeps
// flowing.
func applyDefinitions(c *Client, eng *engine.Engine, m *metrics.Metrics, res downResult) bool {
	cursor := uns.DownlinkDefCursor(c.parentPub)
	advanceTo := res.DefNext
	for _, r := range res.Definitions {
		if _, err := eng.IngestDownlinkDefinitionAttributed(r.Topic, r.Payload, r.TS, engine.Attribution{
			WrittenBy: r.WrittenBy, ActorID: r.ActorID,
			ActorLabel: r.ActorLabel, ActorKind: r.ActorKind,
		}); err != nil {
			m.DefinitionRejected()
			var refused *engine.RejectError
			if errors.As(err, &refused) {
				c.log.Warn("downlink definition refused by this node's contracts — skipping past it; "+
					"a parent running a newer bundle authors definitions this node cannot apply until it is upgraded",
					"topic", r.Topic, "reason", refused.Reason, "err", err)
				continue
			}
			c.log.Error("downlink definition not stored — holding the cursor so it is offered again",
				"topic", r.Topic, "parent_offset", r.ParentOffset, "err", err)
			advanceTo = r.ParentOffset
			break
		}
		m.DefinitionApplied()
	}
	if advanceTo > eng.Store().CursorGet(cursor, downlinkDefStream) {
		eng.Store().CursorAck(cursor, downlinkDefStream, advanceTo)
		return true
	}
	return false
}

// contextFromStop derives a context that is cancelled when stop is closed, so a
// loop parked in a 20s long poll aborts its request immediately on shutdown
// instead of blocking the node's Stop for up to that long.
func contextFromStop(stop <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

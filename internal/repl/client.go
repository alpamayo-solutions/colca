package repl

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
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
	// downlinkStream is a pseudo-stream: its cursor tracks the parent's offsets,
	// which have nothing to do with the local commands stream.
	downlinkStream = "commands-parent"
	// downlinkDefStream does the same for definitions, which advance independently
	// of commands.
	downlinkDefStream = "definitions-parent"

	// metricsStream is the uplink floor, the one stream RunUplink pushes outside the
	// priority lanes. uplinkStreams derives the full set from it and priorityLanes.
	metricsStream = "metrics"

	// Cursor names from before cursors were scoped by parent. They are adopted once
	// under the scoped names and then deleted; nothing writes them.
	legacyUplinkCursor      = "uplink"
	legacyDownlinkCursor    = "downlink"
	legacyDownlinkDefCursor = "downlink-def"

	replBatch    = maxReplicateRecords
	uplinkIdle   = 150 * time.Millisecond
	downlinkWait = 20 * time.Second
	retryAfter   = 500 * time.Millisecond

	// A request's deadline grows with what it carries: transferGrace covers the
	// round trip and minTransferBPS is the slowest link this must still work on. A
	// fixed timeout would cut a large blob off mid-body on a slow edge uplink.
	transferGrace  = 30 * time.Second
	minTransferBPS = 32 * 1024 // 256 kbit/s
)

// transferDeadline is how long a request carrying size bytes may take.
func transferDeadline(size int64) time.Duration {
	if size <= 0 {
		return transferGrace
	}
	return transferGrace + time.Duration(size/minTransferBPS)*time.Second
}

type Client struct {
	base string
	// parentPub is the parent's pinned public key. Every connection verifies it, and
	// cursor names are derived from it (uns.UplinkCursor and friends), so a reparent
	// never resumes against the old parent's offsets and a return to a former parent
	// finds its position intact.
	parentPub        string
	http             *http.Client
	log              *slog.Logger
	maxReplicateBody int64
	// Whether each replication lane is currently failing, so an outage logs
	// as a state change rather than once per retry (see linkstate.go).
	links *linkState
	// status is the current uplink condition, read by /healthz. It is an
	// atomic.Value so a /healthz read never contends with the loops that write it.
	status atomic.Value
}

// NewClient returns a TLS client that presents this node's certificate and pins
// the parent's public key.
func NewClient(baseURL, parentPubHex string, id *identity.Identity, maxRecordBytes ...uint64) (*Client, error) {
	cert, err := id.SelfSignedCert("colca-child")
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true, //nolint:gosec // trust is the pinned parent key, checked below
		MinVersion:         tls.VersionTLS13,
		// Unlike VerifyPeerCertificate, VerifyConnection also runs on resumed sessions.
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return fmt.Errorf("parent presented no certificate")
			}
			pub, err := identity.PeerPubHex(cs.PeerCertificates[0].Raw)
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
	// No client-wide Timeout: it would cover the body too, so one number would have
	// to fit a tiny poll and a 32 MiB blob (see transferDeadline). Only getting a
	// connection has a fixed bound.
	transport := &http.Transport{
		TLSClientConfig:       tlsCfg,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		IdleConnTimeout:       90 * time.Second,
	}
	cl := &Client{
		base:             baseURL,
		parentPub:        parentPubHex,
		http:             &http.Client{Transport: transport},
		log:              slog.Default().With("comp", "repl-client"),
		maxReplicateBody: replicateBodyLimit(&config.Config{Limits: limits}),
		links:            newLinkState(),
	}
	cl.status.Store(Status{State: UplinkConnecting, Since: time.Now().UTC()})
	return cl, nil
}

// ParentPub is the pinned parent key this client is bound to, and the scope of
// every replication cursor against it.
func (c *Client) ParentPub() string { return c.parentPub }

func (c *Client) Replicate(stream string, recs []store.ReplRecord) (hwm uint64, err error) {
	hwm, _, err = c.replicate(context.Background(), stream, recs)
	return hwm, err
}

// replicate is the context-aware implementation behind Replicate and RunUplink.
// nowMS is the parent's clock from the response; RunUplink applies it and
// Replicate drops it.
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
	ctx, cancel := context.WithTimeout(ctx, transferDeadline(int64(len(body))))
	defer cancel()
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
		return 0, 0, &replError{Route: "replicate to " + c.base, Status: resp.StatusCode, Body: readReason(resp)}
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
			WB: r.WrittenBy, AID: r.ActorID, AL: r.ActorLabel, AK: r.ActorKind, AG: r.ActorGroups,
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
	ActorGroups  []string
}

// downResult is one decoded downlink response. Definitions share the response
// with commands but keep their own records and cursor.
type downResult struct {
	Records     []DownRec
	Next        uint64
	Gap         *store.GapSpan
	NowMS       int64
	Ancestry    *uns.Ancestry
	Definitions []DownRec
	DefNext     uint64
	// Head is the parent's commands stream head at hello time, where a child with no
	// cursor for this parent must start. Only the hello response carries it; 0 means
	// the parent sent none.
	Head uint64
}

// Downlink polls the parent once. gap is non-nil when the position falls in a
// range the parent's retention pruned; next then already points past it.
func (c *Client) Downlink(after uint64, limit int, timeout time.Duration) ([]DownRec, uint64, *store.GapSpan, error) {
	res, err := c.downlink(context.Background(), after, 1, limit, timeout)
	if err != nil {
		return nil, 0, nil, err
	}
	return res.Records, res.Next, res.Gap, nil
}

// DownlinkWithAncestry is Downlink plus the position the parent teaches;
// ancestry is nil when the parent sent none.
func (c *Client) DownlinkWithAncestry(after uint64, limit int, timeout time.Duration) ([]DownRec, uint64, *store.GapSpan, *uns.Ancestry, error) {
	res, err := c.downlink(context.Background(), after, 1, limit, timeout)
	if err != nil {
		return nil, 0, nil, nil, err
	}
	return res.Records, res.Next, res.Gap, res.Ancestry, nil
}

// DownlinkDefinitions is Downlink for the definitions stream: the records the
// parent handed down and the next position on that stream.
func (c *Client) DownlinkDefinitions(defAfter uint64, limit int, timeout time.Duration) ([]DownRec, uint64, error) {
	res, err := c.downlink(context.Background(), 1, defAfter, limit, timeout)
	if err != nil {
		return nil, 0, err
	}
	return res.Definitions, res.DefNext, nil
}

// hello is the first contact: an immediate answer, with no long poll and no
// records consumed, that returns the position and any waiting definitions in one
// round trip.
func (c *Client) hello(ctx context.Context, defAfter uint64) (downResult, error) {
	return c.downlinkURL(ctx, fmt.Sprintf("%s/downlink?after=1&max=1&hello=1&def_after=%d",
		c.base, defAfter), 10*time.Second)
}

// downlink is the context-aware implementation behind the wrappers and
// RunDownlink. NowMS is the parent's clock, which RunDownlink applies before
// ingesting anything. Ancestry is the position the parent teaches, or nil.
func (c *Client) downlink(ctx context.Context, after, defAfter uint64, limit int, timeout time.Duration) (downResult, error) {
	return c.downlinkURL(ctx, fmt.Sprintf("%s/downlink?after=%d&def_after=%d&max=%d",
		c.base, after, defAfter, limit), timeout)
}

func (c *Client) downlinkURL(ctx context.Context, url string, timeout time.Duration) (downResult, error) {
	// The long poll's own wait plus a round trip: this request carries almost
	// nothing, so its bound is time, not size.
	ctx, cancel := context.WithTimeout(ctx, timeout+transferGrace)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return downResult{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return downResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return downResult{}, &replError{Route: "downlink from " + c.base, Status: resp.StatusCode, Body: readReason(resp)}
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
			ActorLabel: r.AL, ActorKind: r.AK, ActorGroups: r.AG,
		}
	}
	return out
}

// ackOnly lets exactly two kinds of commands-stream record rise: acks, and
// _StreamGap markers so a pruned commands stream is reported upstream. A command
// never goes back to the node it came from.
func ackOnly(topic string) bool {
	p, err := uns.Parse(topic)
	return err == nil && (p.Contract == "_Ack" || p.Contract == "_StreamGap")
}

// leavesTheNode runs on every lane and the metrics floor before the lane's own
// filter: node-private records (uns.IsNodePrivate, such as Edit replay receipts
// and their tombstones) never go to the parent. Read still advances past what it
// filters, so a private record cannot hold a lane. Unparseable topics pass.
func leavesTheNode(topic string) bool {
	p, err := uns.Parse(topic)
	return err != nil || !uns.IsNodePrivate(p.Contract)
}

// pushable composes the universal keep-home rule with a lane's own filter.
func pushable(laneFilter func(string) bool) func(string) bool {
	if laneFilter == nil {
		return leavesTheNode
	}
	return func(topic string) bool { return leavesTheNode(topic) && laneFilter(topic) }
}

// priorityLanes are drained to empty, in this order, before metrics. Acks come
// first because a parent's move-drain waits for them, then alarms and entities,
// which operators wait for after an outage, then audit, then annotations.
//
// definitions is absent on purpose: definitions only descend, and a child
// pushing them up could author policy for the whole tree. The node-private rule
// (leavesTheNode) applies to every lane and is added in pushOnce.
var priorityLanes = []struct {
	name   string
	filter func(string) bool
}{
	{"commands", ackOnly},
	{"alarms", nil},
	{"entities", nil},
	{"audit", nil},
	{"annotations", nil},
	// Logs go last among the lanes: worth less than an alarm, more than a sample,
	// and on their own lane they cannot starve the samples.
	{"logs", nil},
}

// uplinkStreams is the set RunUplink pushes: every priority lane plus the
// metrics floor, derived so a new lane also gets its cursor seeded.
func uplinkStreams() []string {
	streams := make([]string, 0, len(priorityLanes)+1)
	for _, lane := range priorityLanes {
		streams = append(streams, lane.name)
	}
	return append(streams, metricsStream)
}

// adoptLegacy moves a legacy cursor's value to its parent-scoped name, deletes
// the old key and reports whether there was anything to adopt. A failed delete
// is only logged: adopting the same value again later is a no-op on a
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

// initCursors settles this child's cursors against the configured parent before
// either loop reads them. For each cursor:
//
//  1. A scoped cursor exists: keep it (every normal start, or a return to a
//     former parent).
//  2. Only a legacy cursor exists: adopt its value under the scoped name.
//  3. Neither exists, so this is first contact. Uplink starts at each stream's
//     LWM and the parent's HWM drops duplicates; definitions start at 1;
//     commands start at the parent's head, since commands issued before this
//     child attached were not meant for it.
//
// head is the parent's commands head from hello, or 0 without one. Every write
// moves a cursor forward or claims a missing one, so the two loops calling this
// concurrently converge. m may be nil.
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

	// Commands need the exact case, so probe for the key: CursorGet returns 1 both
	// for "never met this parent" and for "met it while its stream was empty", and
	// the second must deliver from offset 1 so a command queued while the target was
	// down still runs.
	cmd := uns.DownlinkCursor(c.parentPub)
	if !adoptLegacy(c, st, legacyDownlinkCursor, cmd, downlinkStream) { // case 2
		// First contact only: CursorSetIfAbsent leaves an existing cursor alone and
		// records even position 1. With head 0 nothing is claimed; a caller with a head
		// settles it.
		if head > 0 {
			if _, err := st.CursorSetIfAbsent(cmd, downlinkStream, head); err != nil {
				// Nothing recorded, so CursorGet returns 1 and the first poll reads commands
				// issued before this node attached.
				c.log.Warn("first-contact commands cursor not recorded — this node polls from 1 and may "+
					"execute commands issued under its mount before it attached",
					"cursor", cmd, "head", head, "err", err)
			}
		}
	}
	// A position past the parent's head means the parent pruned past it or was
	// rebuilt empty, and this node would stay deaf to commands without an error.
	// Only report it: rewinding would re-deliver commands that already ran.
	if pos := st.CursorGet(cmd, downlinkStream); head > 0 && pos > head {
		c.log.Warn("downlink commands cursor is past the parent's head — the parent pruned past "+
			"this position or was rebuilt; this node will receive no commands until the parent's "+
			"stream grows past it",
			"position", pos, "parent_head", head, "parent_key", short(c.parentPub))
		m.DownlinkCursorBeyondHead()
	}
}

// RunUplink pushes this node's streams to its parent until stop is closed,
// draining the priority lanes before each metrics batch. Order within a stream
// never changes, so a lane is the only way past a backlog. Each pass sends at
// least one metrics batch, so a lane that never empties cannot hold the metrics
// cursor until retention prunes past it. blobs and m may be nil.
func RunUplink(c *Client, eng *engine.Engine, blobs *blobstore.Store, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	// Settle the position before the first cursor read. Neither a head nor metrics
	// are needed here; RunDownlink's hello supplies the head for commands.
	initCursors(c, eng, nil, 0)

	// Blobs this parent confirmed, keyed by digest, with the local blob's
	// modification time so a swept and recreated blob is pushed again.
	confirmedBlobs := map[string]time.Time{}
	// Blobs this parent permanently refused (a 4xx). Kept per session, because a new
	// parent may accept them.
	rejectedBlobs := map[string]bool{}

	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// pushOnce pushes at most one batch of stream and reports whether it scanned
	// anything, which is how "drained" is measured. aborted means our own shutdown.
	pushOnce := func(stream string, filter func(string) bool) (scanned, aborted bool) {
		from := eng.Store().CursorGet(uns.UplinkCursor(c.parentPub), stream)
		// A cursor below the local LWM means retention pruned past it after the parent
		// was gone longer than the staleness window. The gap marker already tells the
		// parent, so jump to the LWM instead of stalling.
		if lwm := eng.Store().LWM(stream); from < lwm {
			c.log.Error("uplink cursor below the stream LWM: local retention pruned past it, jumping to the LWM",
				"stream", stream, "position", from, "lwm", lwm)
			m.GapReceived(stream)
			eng.Store().CursorAck(uns.UplinkCursor(c.parentPub), stream, lwm)
			from = lwm
		}
		recs, next, err := eng.Store().Read(stream, from, replBatch, pushable(filter))
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
					ActorLabel: r.ActorLabel, ActorKind: r.ActorKind, ActorGroups: r.ActorGroups,
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
				// A parent that answered and refused (4xx) is not a parent that is down, and the
				// log must say which.
				//
				// A refused batch is held and retried, never skipped. Only retention decides to
				// drop data, and it leaves a _StreamGap marker; skipping here would lose records
				// without a trace over what is usually a config mismatch (max_record_bytes).
				// colca_uplink_refused_total and this error make the stalled lane visible.
				var refusal *replError
				refused := errors.As(err, &refusal) && refusal.Refused()
				if report, attempts, down := c.links.Failed("uplink:"+stream, time.Now()); report {
					if refused {
						c.log.Error("uplink refused by the parent — the batch is held and retried, nothing is dropped",
							"stream", stream, "parent", c.base, "status", refusal.Status,
							"meaning", replicationStatusMeaning(refusal.Status),
							"parent_said", refusal.Body,
							"attempts", attempts, "held_for", down.Round(time.Second))
					} else {
						c.log.Warn("uplink is down (retrying)",
							"stream", stream, "parent", c.base,
							"attempts", attempts, "down_for", down.Round(time.Second), "err", err)
					}
				}
				m.UplinkPushFailed(stream)
				if refused {
					m.UplinkRefused(stream)
				}
				// The cursor stays put. Report "not scanned" so a batch the parent cannot take
				// ends the drain loop instead of spinning.
				return false, false
			}
			if wasFailing, attempts, down := c.links.Recovered("uplink:"+stream, time.Now()); wasFailing {
				c.log.Info("uplink recovered",
					"stream", stream, "parent", c.base,
					"attempts", attempts, "down_for", down.Round(time.Second))
			}
			// Every /replicate response carries the parent's clock; keep the offset fresh
			// whichever stream triggered the push.
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
			// Drain only up to the lane's end at pass start. Draining until empty would
			// never finish on a lane written faster than it drains, and metrics would never
			// get their turn.
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
		// Blobs last: they are not in any stream and must never delay a record. A quiet
		// pass costs one HEAD per unconfirmed blob, which is why confirmations are kept.
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
// commands run at their target, definitions are applied where they land. m may
// be nil.
func RunDownlink(c *Client, eng *engine.Engine, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	// First contact: learn the position and waiting definitions in one round trip.
	// Retry until it answers: a child with no cursor for this parent must adopt the
	// head before its first poll, or it would receive every command issued before it
	// attached. While hello fails the parent is unreachable anyway.
	for {
		res, err := c.hello(ctx, eng.Store().CursorGet(uns.DownlinkDefCursor(c.parentPub), downlinkDefStream))
		if err == nil {
			// A parent without a head predates the field, as during a rolling upgrade.
			// There is nothing to adopt, so a missing commands cursor stays at 1 and the
			// poll delivers commands issued before this node attached. This is logged on
			// every start against such a parent, even when a cursor already exists.
			if res.Head == 0 {
				c.log.Warn("parent answered hello without a command head — it predates parent-scoped "+
					"cursors; if this node has no commands cursor for it yet, that cursor starts at 1 "+
					"and it may execute commands issued under its mount before it attached",
					"parent", c.base, "parent_key", short(c.parentPub))
				m.DownlinkHeadAbsent()
			}
			// Before anything is applied, settle first-contact cursors, including adopting
			// the head from this response before the poll reads commands.
			initCursors(c, eng, m, res.Head)
			if res.Ancestry != nil {
				eng.SetAncestry(*res.Ancestry)
			}
			_ = applyDefinitions(c, eng, m, res)
			if wasFailing, attempts, waited := c.links.Recovered("hello", time.Now()); wasFailing {
				c.log.Info("first contact with the parent succeeded",
					"parent", c.base, "attempts", attempts, "waited", waited.Round(time.Second))
			}
			c.setStatus(UplinkConnected)
			break
		}
		c.setStatus(classifyUplinkErr(err))
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
			c.setStatus(classifyUplinkErr(err))
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
		c.setStatus(UplinkConnected)
		m.DownlinkFetched(time.Now())
		// Apply the clock offset from this response before ingesting its records, so
		// expiry decisions after a reconnect use fresh time.
		eng.ApplyClockSample(nowMS)
		if ancestry != nil {
			eng.SetAncestry(*ancestry) // idempotent: every poll may carry it
		}
		if gap != nil {
			// The parent pruned commands this node never got. Log, count and continue; next
			// already points past the gap. Commands are retained past their TTL, so these
			// had expired anyway.
			c.log.Error("downlink gap: the parent pruned commands this node never received; continuing past the hole",
				"from_offset", gap.FromOffset, "to_offset", gap.ToOffset,
				"first_ts", gap.FirstTS, "last_ts", gap.LastTS, "approx", gap.Approx)
			m.GapReceived("commands")
		}
		// A record this node refused is skipped; a record it could not store holds the
		// cursor so the parent offers it again. A refusal (engine.RejectError) will be
		// made the same way every time, so rereading it would stall the lane. A store
		// failure (full disk, I/O error, record over max_record_bytes) says nothing about
		// the record, and acking past it would silently drop the command.
		ackTo := next
		for _, r := range recs {
			if _, err := eng.IngestDownlinkAttributed(r.Topic, r.Payload, r.TS, engine.Attribution{
				WrittenBy: r.WrittenBy, ActorID: r.ActorID,
				ActorLabel: r.ActorLabel, ActorKind: r.ActorKind, ActorGroups: r.ActorGroups,
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
			// Neither cursor moved. Usually that is an idle long poll, but a poll that
			// answered immediately without progress would otherwise spin at the rate limit.
			select {
			case <-stop:
				return
			case <-time.After(retryAfter):
			}
		}
	}
}

// applyDefinitions stores what the parent handed down, advances the definitions
// cursor and reports whether it moved. As with commands, a definition that could
// not be stored holds the cursor, since there is nothing to recover it from
// later. A refused definition is skipped with a warning: in a rolling upgrade a
// newer parent sends contracts an older child does not know, and holding on them
// would block every later definition, such as a group revocation.
func applyDefinitions(c *Client, eng *engine.Engine, m *metrics.Metrics, res downResult) bool {
	cursor := uns.DownlinkDefCursor(c.parentPub)
	advanceTo := res.DefNext
	for _, r := range res.Definitions {
		if _, err := eng.IngestDownlinkDefinitionAttributed(r.Topic, r.Payload, r.TS, engine.Attribution{
			WrittenBy: r.WrittenBy, ActorID: r.ActorID,
			ActorLabel: r.ActorLabel, ActorKind: r.ActorKind, ActorGroups: r.ActorGroups,
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

package repl

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpamayo-solutions/colca/internal/blobstore"
	"github.com/alpamayo-solutions/colca/internal/clock"
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
	uplinkIdle   = 30 * time.Second
	downlinkWait = 20 * time.Second
	retryAfter   = 500 * time.Millisecond
	// expectContinueBytes is the push size from which the uplink asks the
	// parent before sending the body: a backlog's full batches, not the few
	// records of a live push, whose extra round trip would cost more.
	expectContinueBytes = 64 << 10

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
	// unenrolled spaces every request while the parent answers 401.
	unenrolled unenrolledBackoff
	// status is the current uplink condition, read by /healthz. It is an
	// atomic.Value so a /healthz read never contends with the loops that write it.
	status        atomic.Value
	statusMu      sync.Mutex
	statusChanged chan struct{}
	// storeID is this node's store incarnation, sent with every replication
	// batch so the parent can tell a rebuilt store from a resumed one. It is
	// read per request: the incarnation can change while the node runs.
	storeID func() string
	// syncMu guards what this node keeps about the parent's store (resync.go):
	// state, the epoch of the uplink positions, and the cancel of the poll in
	// flight. It is never held across a request.
	syncMu      sync.Mutex
	state       parentState
	// resyncView is state.Resync as last set under syncMu, for ResyncStatus to
	// read without it: syncMu is held across synced writes, and /healthz must
	// not wait on the disk. A published Resync is never changed in place.
	resyncView atomic.Pointer[Resync]
	stateLoaded bool
	epoch       uint64
	pollCancel  context.CancelFunc
	// resyncWanted is set by the uplink loop when the parent refused a push
	// for a re-sync; the downlink loop runs the handshake and clears it.
	resyncWanted atomic.Bool
	// replaying is true while a re-sync still sends records again, so the
	// uplink loop checks for its completion only then.
	replaying atomic.Bool
	// logs decides which logs-stream records rise (parent.logs). The default
	// forwards WARNING and above.
	logs logForwarding
	// setAsideGrace overrides defaultSetAsideGrace in tests; see setaside.go.
	setAsideGrace time.Duration
}

// SetStoreID names the store incarnation replication batches carry, for a
// pusher that has no store of its own. Call it before the uplink starts.
func (c *Client) SetStoreID(id string) { c.storeID = func() string { return id } }

// FollowStore makes replication batches carry st's incarnation as it is when
// each is sent (see store.Store.StoreID). Call it before the uplink starts.
func (c *Client) FollowStore(st *store.Store) { c.storeID = st.StoreID }

// SetLogForwarding applies a validated parent.logs block. Call it before the
// uplink starts; without it the client forwards WARNING and above.
func (c *Client) SetLogForwarding(cfg config.ParentLogs) { c.logs = newLogForwarding(cfg) }

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
		logs:             newLogForwarding(config.ParentLogs{}),
		storeID:          func() string { return "" },
	}
	cl.status.Store(Status{State: UplinkConnecting, Since: time.Now().UTC()})
	return cl, nil
}

// ParentPub is the pinned parent key this client is bound to, and the scope of
// every replication cursor against it.
func (c *Client) ParentPub() string { return c.parentPub }

func (c *Client) Replicate(stream string, recs []store.ReplRecord) (hwm uint64, err error) {
	hwm, _, err = c.replicate(context.Background(), envelope{store: c.storeID(), stream: stream}, recs)
	return hwm, err
}

// envelope is what a replication request says besides its records.
type envelope struct {
	// store is this node's own incarnation.
	store  string
	stream string
	// handshake is set by the uplink loop: the request then states the parent
	// incarnation this node recorded and the highest offset of stream the
	// parent confirmed, and the parent refuses it when they no longer hold.
	handshake bool
	parent    string
	acked     uint64
}

// parentClock is what a replication response says about the parent's clock:
// its wall clock when it wrote the response, and how long it had the request by
// then. Held is 0 from a parent that predates the field.
type parentClock struct {
	NowMS  int64
	HeldMS int64
}

// sample is the reading as the clock package takes it, with this node's clock
// before the request and after the response.
func (p parentClock) sample(sent, received time.Time) clock.ParentSample {
	return clock.ParentSample{NowMS: p.NowMS, Held: time.Duration(p.HeldMS) * time.Millisecond, Sent: sent, Received: received}
}

// replicate is the context-aware implementation behind Replicate and RunUplink.
// pc is the parent's clock from the response; RunUplink records the skew to
// it and Replicate drops it.
func (c *Client) replicate(ctx context.Context, env envelope, recs []store.ReplRecord) (hwm uint64, pc parentClock, err error) {
	if len(recs) > maxReplicateRecords {
		return 0, parentClock{}, fmt.Errorf("replicate: batch has %d records, maximum is %d", len(recs), maxReplicateRecords)
	}
	body, err := marshalReplication(env, recs)
	if err != nil {
		return 0, parentClock{}, err
	}
	if int64(len(body)) > c.maxReplicateBody {
		return 0, parentClock{}, fmt.Errorf("replicate: request is %d bytes, maximum is %d", len(body), c.maxReplicateBody)
	}
	ctx, cancel := context.WithTimeout(ctx, transferDeadline(int64(len(body))))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/replicate", bytes.NewReader(body))
	if err != nil {
		return 0, parentClock{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if len(body) >= expectContinueBytes {
		// A busy parent answers 429 before reading a push; asked first, it
		// does so before a large body crosses the link (and is resent).
		req.Header.Set("Expect", "100-continue")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, parentClock{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, parentClock{}, &replError{Route: "replicate to " + c.base, Status: resp.StatusCode, Body: readReason(resp),
			RetryAfter: retryAfterHeader(resp), Store: resp.Header.Get(storeHeader)}
	}
	var out struct {
		HWM    uint64 `json:"hwm"`
		NowMS  int64  `json:"now_ms"`
		HeldMS int64  `json:"held_ms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, parentClock{}, err
	}
	return out.HWM, parentClock{NowMS: out.NowMS, HeldMS: out.HeldMS}, nil
}

func marshalReplication(env envelope, recs []store.ReplRecord) ([]byte, error) {
	wire := make([]wireRec, len(recs))
	for i, r := range recs {
		wire[i] = wireRec{
			SkipFrom: r.SkipFrom,
			O:        r.ChildOffset, OO: r.OriginOffset, T: r.Topic, P: r.Payload, TS: r.TS,
			WB: r.WrittenBy, AID: r.ActorID, AL: r.ActorLabel, AK: r.ActorKind, AG: r.ActorGroups, AC: r.ActorClient,
		}
	}
	msg := map[string]any{"stream": env.stream, "records": wire}
	if env.store != "" {
		msg["store"] = env.store
	}
	if env.handshake {
		// Always present, also empty: its presence is what tells the parent
		// this node can re-sync.
		msg["parent"] = env.parent
		if env.acked > 0 {
			msg["acked"] = env.acked
		}
	}
	return json.Marshal(msg)
}

// fitReplicationBatch returns the largest non-empty prefix whose exact JSON
// envelope fits the server contract. The normal 200-record batch needs one
// marshal; binary search is used only for unusually large records.
func fitReplicationBatch(env envelope, batch []store.ReplRecord, maxBytes int64) ([]store.ReplRecord, error) {
	body, err := marshalReplication(env, batch)
	if err != nil || int64(len(body)) <= maxBytes {
		return batch, err
	}
	low, high := 1, len(batch)-1
	best := 0
	for low <= high {
		mid := low + (high-low)/2
		body, err := marshalReplication(env, batch[:mid])
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
	ActorClient  string
}

// downResult is one decoded downlink response. Definitions share the response
// with commands but keep their own records and cursor.
type downResult struct {
	Records     []DownRec
	Next        uint64
	Gap         *store.GapSpan
	Clock       parentClock
	Ancestry    *uns.Ancestry
	Definitions []DownRec
	DefNext     uint64
	// Confirms is true from a parent that answers a confirm request at once
	// (see confirmPage). One that predates it would hold the request as a long
	// poll, so it is never asked.
	Confirms bool
	// Head is the parent's commands stream head at hello time, where a child with no
	// cursor for this parent must start. Only the hello response carries it; 0 means
	// the parent sent none.
	Head uint64
	// Store is the incarnation of the parent's store, "" from a parent that
	// predates incarnations.
	Store string
	// The rest is the hello response's handshake. Resync: the parent asks for
	// a re-sync. Resume and DefResume: where this node reads commands and
	// definitions from after one. HWMs: the highest offset of each uplink
	// stream the parent holds from this node.
	Resync    bool
	Resume    uint64
	DefResume uint64
	HWMs      map[string]uint64
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

// Poll is one downlink request at both positions, for callers that run their
// own loop instead of RunDownlink (the fanout benchmark's protocol children). It
// returns the next positions on commands and definitions.
func (c *Client) Poll(ctx context.Context, after, defAfter uint64, limit int, timeout time.Duration) (next, defNext uint64, err error) {
	res, err := c.downlink(ctx, after, defAfter, limit, timeout)
	if err != nil {
		return after, defAfter, err
	}
	if res.DefNext == 0 {
		res.DefNext = defAfter
	}
	return res.Next, res.DefNext, nil
}

// hello is the first contact: an immediate answer, with no long poll and no
// records consumed, that returns the position and any waiting definitions in one
// round trip.
//
// It is also the handshake: this node states the parent incarnation it
// recorded, its own, how far it read and what the parent confirmed, and the
// parent answers where its store stands (see resync.go).
func (c *Client) hello(ctx context.Context, g greeting) (downResult, error) {
	q := url.Values{}
	q.Set("hello", "1")
	q.Set("max", "1")
	q.Set("after", strconv.FormatUint(max(g.after, 1), 10))
	q.Set("def_after", strconv.FormatUint(max(g.defAfter, 1), 10))
	q.Set("parent", c.parentStore())
	q.Set("store", c.storeID())
	if v := formatAcked(g.acked); v != "" {
		q.Set("acked", v)
	}
	return c.downlinkURL(ctx, c.base+"/downlink?"+q.Encode(), 10*time.Second)
}

// greeting is what a node states about its positions in a hello: the next
// command and definition offsets it reads, and per uplink stream the highest
// offset the parent confirmed.
type greeting struct {
	after, defAfter uint64
	acked           map[string]uint64
}

// greet reads this node's positions against its parent from st.
func (c *Client) greet(st *store.Store) greeting {
	g := greeting{
		after:    st.CursorGet(uns.DownlinkCursor(c.parentPub), downlinkStream),
		defAfter: st.CursorGet(uns.DownlinkDefCursor(c.parentPub), downlinkDefStream),
		acked:    map[string]uint64{},
	}
	for _, stream := range uplinkStreams() {
		g.acked[stream] = st.AckedMark(c.parentPub, stream)
	}
	return g
}

// downlink is the context-aware implementation behind the wrappers and
// RunDownlink. Clock is the parent's clock, which RunDownlink measures this
// node's skew against. Ancestry is the position the parent teaches, or nil.
func (c *Client) downlink(ctx context.Context, after, defAfter uint64, limit int, timeout time.Duration) (downResult, error) {
	return c.downlinkURL(ctx, fmt.Sprintf("%s/downlink?after=%d&def_after=%d&max=%d",
		c.base, after, defAfter, limit), timeout)
}

// poll is downlink for the replication loop: it states the parent incarnation
// this node recorded, so a parent whose store changed refuses instead of
// answering a position of another history.
//
// confirm asks the parent to answer at once with what it holds, instead of
// holding the request until there is something: the request then has a round
// trip's time and no more.
func (c *Client) poll(ctx context.Context, after, defAfter uint64, confirm bool) (downResult, error) {
	u := fmt.Sprintf("%s/downlink?after=%d&def_after=%d&max=%d&parent=%s&store=%s",
		c.base, after, defAfter, replBatch, url.QueryEscape(c.parentStore()), url.QueryEscape(c.storeID()))
	if confirm {
		return c.downlinkURL(ctx, u+"&confirm=1", 0)
	}
	return c.downlinkURL(ctx, u, downlinkWait)
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
		return downResult{}, &replError{Route: "downlink from " + c.base, Status: resp.StatusCode, Body: readReason(resp),
			Store: resp.Header.Get(storeHeader)}
	}
	var out struct {
		Records     []wireRec         `json:"records"`
		Next        uint64            `json:"next"`
		Gap         *store.GapSpan    `json:"gap"`
		NowMS       int64             `json:"now_ms"`
		HeldMS      int64             `json:"held_ms"`
		Confirms    bool              `json:"confirms"`
		Ancestry    *uns.Ancestry     `json:"ancestry"`
		Definitions []wireRec         `json:"definitions"`
		DefNext     uint64            `json:"def_next"`
		Head        uint64            `json:"head"`
		Store       string            `json:"store"`
		Resync      bool              `json:"resync"`
		Resume      uint64            `json:"resume"`
		DefResume   uint64            `json:"def_resume"`
		HWMs        map[string]uint64 `json:"hwms"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return downResult{}, err
	}
	return downResult{
		Records:     toDownRecs(out.Records),
		Next:        out.Next,
		Gap:         out.Gap,
		Clock:       parentClock{NowMS: out.NowMS, HeldMS: out.HeldMS},
		Confirms:    out.Confirms,
		Ancestry:    out.Ancestry,
		Definitions: toDownRecs(out.Definitions),
		DefNext:     out.DefNext,
		Head:        out.Head,
		Store:       out.Store,
		Resync:      out.Resync,
		Resume:      out.Resume,
		DefResume:   out.DefResume,
		HWMs:        out.HWMs,
	}, nil
}

func toDownRecs(in []wireRec) []DownRec {
	out := make([]DownRec, len(in))
	for i, r := range in {
		out[i] = DownRec{
			ParentOffset: r.O, Topic: r.T, Payload: r.P, TS: r.TS,
			WrittenBy: r.WB, ActorID: r.AID,
			ActorLabel: r.AL, ActorKind: r.AK, ActorGroups: r.AG, ActorClient: r.AC,
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
// and their tombstones) and records of retired contracts (uns.IsRetired) never
// go to the parent. Read still advances past what it filters, so such a record
// cannot hold a lane. Unparseable topics pass.
func leavesTheNode(topic string) bool {
	p, err := uns.Parse(topic)
	return err != nil || (!uns.IsNodePrivate(p.Contract) && !uns.IsRetired(p.Contract))
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
	// and on their own lane they cannot starve the samples. Which _Log records
	// rise is per node (parent.logs), so that filter is the Client's and is
	// applied in pushOnce.
	{logsStream, nil},
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

// PrepareUplink persists retention protection for the configured parent before
// any pruning can run. It needs no connection to the parent. For each stream:
//
//  1. A scoped cursor exists: keep it (every normal start, or a return to a
//     former parent).
//  2. Only a legacy cursor exists: adopt its value under the scoped name.
//  3. Neither exists: persist the stream's LWM, including position 1 on an empty
//     stream. CursorGet's implicit 1 alone does not protect anything from pruning.
//
// Node startup calls this synchronously before starting retention; RunUplink also
// calls it for users of the replication loop outside that lifecycle. Repeating
// it never rewinds a cursor or refreshes an existing cursor's staleness timestamp.
func PrepareUplink(c *Client, st *store.Store) error {
	if err := c.loadState(st); err != nil {
		return err
	}
	up := uns.UplinkCursor(c.parentPub)
	for _, stream := range uplinkStreams() {
		if st.CursorGet(up, stream) > 1 {
			continue // case 1
		}
		if adoptLegacy(c, st, legacyUplinkCursor, up, stream) { // case 2
			continue
		}
		if _, err := st.CursorSetIfAbsent(up, stream, st.LWM(stream)); err != nil { // case 3
			return fmt.Errorf("persist uplink cursor %s for %s: %w", up, stream, err)
		}
	}
	return nil
}

// adoptLegacyDownlink carries the downlink positions kept under the names from
// before cursors were scoped by parent over to the scoped names. It needs no
// contact and runs before the first hello, which states these positions.
func adoptLegacyDownlink(c *Client, st *store.Store) {
	if def := uns.DownlinkDefCursor(c.parentPub); st.CursorGet(def, downlinkDefStream) <= 1 {
		adoptLegacy(c, st, legacyDownlinkDefCursor, def, downlinkDefStream)
	}
	adoptLegacy(c, st, legacyDownlinkCursor, uns.DownlinkCursor(c.parentPub), downlinkStream)
}

// initDownlinkCursors settles the commands position after hello. First-contact
// commands start at the parent's head because earlier commands were not meant
// for this attachment; an existing position is preserved. Definitions start at
// 1, which is the default position, so there is nothing to write. m may be nil.
func initDownlinkCursors(c *Client, eng *engine.Engine, m *metrics.Metrics, head uint64) {
	st := eng.Store()
	// Commands need the exact case, so probe for the key: CursorGet returns 1 both
	// for "never met this parent" and for "met it while its stream was empty", and
	// the second must deliver from offset 1 so a command queued while the target was
	// down still runs.
	cmd := uns.DownlinkCursor(c.parentPub)
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
	// A position past the head of a parent that sends no store incarnation: it
	// pruned past the position or was rebuilt, and this node stays deaf to commands
	// without an error. A parent that sends one is re-synced with instead
	// (resync.go); this one can only be reported.
	if pos := st.CursorGet(cmd, downlinkStream); head > 0 && pos > head {
		c.log.Warn("downlink commands cursor is past the parent's head — the parent's store was restored "+
			"or rebuilt and its colca predates the re-sync; this node will receive no commands until the "+
			"parent's stream grows past it",
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
	if err := PrepareUplink(c, eng.Store()); err != nil {
		c.log.Error("uplink retention protection could not be persisted", "err", err)
		return
	}

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
	retryPending := false
	// busyUntil is, per lane, until when a parent that answered 429 asked to be
	// left alone. A lane waits it out even while other lanes make progress.
	busyUntil := map[string]time.Time{}
	// holds isolates, per lane, a record the parent refuses (see setaside.go).
	holds := map[string]*laneHold{}
	if err := c.publishSetAsideFinding(eng); err != nil {
		c.log.Warn("uplink_set_aside finding not brought up to date at start", "err", err)
	}
	pushOnce := func(stream string, filter func(string) bool) (scanned, aborted bool) {
		if c.resyncWanted.Load() {
			// The parent's store is not the one this node's positions belong
			// to. Every lane waits for the downlink loop to re-sync: a batch
			// stored now would move the parent's mark past what it lacks.
			retryPending = true
			return false, false
		}
		if until, ok := busyUntil[stream]; ok {
			if time.Now().Before(until) {
				retryPending = true
				return false, false
			}
			delete(busyUntil, stream)
		}
		if c.unenrolled.wait(time.Now()) > 0 {
			// The parent does not know this node's key: no lane asks again
			// before the wait is over.
			retryPending = true
			return false, false
		}
		// The epoch and the parent incarnation are read together and before the
		// cursor: a page read at a position a re-sync has since moved names the
		// incarnation recorded before it. The parent refuses it when that is
		// another than its own, so the page cannot move its mark past the range
		// about to be sent again. (A re-sync that keeps the incarnation has no
		// such range: the parent's store did not regress.)
		epoch, parentStore := c.uplinkEpoch()
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
		// Read a bounded physical page before filtering, including local-only records.
		recs, next, err := eng.Store().Read(stream, from, replBatch, nil)
		if err != nil {
			c.log.Error("uplink read", "stream", stream, "err", err)
			retryPending = true
			return false, false
		}
		if next == from {
			return false, false // nothing scanned: this lane is empty
		}
		pushed := false
		var hwm uint64
		batch := make([]store.ReplRecord, 0, len(recs))
		allowed := pushable(filter)
		// Records the log policy keeps home. Like every filtered record, the cursor
		// moves past them, so they neither hold the lane nor pin retention. They are
		// counted only once the cursor has moved, so a retried page counts once.
		type withheldLog struct {
			offset uint64
			level  string
		}
		var withheld []withheldLog
		for _, r := range recs {
			if !allowed(r.Topic) {
				continue
			}
			if stream == logsStream {
				if ok, level := c.logs.forward(r.Topic); !ok {
					withheld = append(withheld, withheldLog{r.Offset, level})
					continue
				}
			}
			if stream == "metrics" && r.SourceLocalOnly {
				if n := len(batch); n > 0 && batch[n-1].SkipFrom != 0 && batch[n-1].ChildOffset+1 == r.Offset {
					batch[n-1].ChildOffset = r.Offset
				} else {
					batch = append(batch, store.ReplRecord{ChildOffset: r.Offset, SkipFrom: r.Offset})
				}
				continue
			}
			batch = append(batch, store.ReplRecord{
				ChildOffset: r.Offset, OriginOffset: r.OriginOffset,
				Topic: r.Topic, Payload: r.Payload, TS: r.TS,
				WrittenBy: r.WrittenBy, ActorID: r.ActorID,
				ActorLabel: r.ActorLabel, ActorKind: r.ActorKind, ActorGroups: r.ActorGroups, ActorClient: r.ActorClient,
			})
		}
		if len(batch) > 0 {
			fullBatchLen := len(batch)
			env := envelope{store: c.storeID(), stream: stream, handshake: true, parent: parentStore,
				acked: eng.Store().AckedMark(c.parentPub, stream)}
			batch, err = fitReplicationBatch(env, batch, c.maxReplicateBody)
			if err != nil {
				c.log.Error("uplink batch cannot fit the replication request bound", "stream", stream, "err", err)
				retryPending = true
				m.UplinkPushFailed(stream)
				return false, false
			}
			hold := holds[stream]
			if hold == nil {
				hold = &laneHold{}
				holds[stream] = hold
			}
			if hold.limit > 0 && len(batch) > hold.limit {
				// Isolating a refused record: the records before it go first.
				batch = batch[:hold.limit]
			}
			if len(batch) < fullBatchLen {
				next = batch[len(batch)-1].ChildOffset + 1
			}
			var pc parentClock
			sent := eng.Now()
			hwm, pc, err = c.replicate(ctx, env, batch)
			received := eng.Now()
			if err == nil && hwm < batch[len(batch)-1].ChildOffset {
				err = fmt.Errorf("parent acknowledged offset %d below batch end %d", hwm, batch[len(batch)-1].ChildOffset)
			}
			if err != nil {
				if stopped() {
					return false, true // aborted by our own shutdown, not a failure
				}
				// A parent that answered and refused (4xx) is not a parent that is down, and the
				// log must say which.
				//
				// A refused batch is held and retried, not skipped: a refusal is usually a config
				// mismatch (max_record_bytes) that the parent's operator fixes. The retry sends
				// half the batch, down to the one record the parent refuses, so the records
				// before it rise; that record alone is set aside only after the grace, with a
				// copy, a finding and an error that say it did not reach the parent (setaside.go).
				// colca_uplink_refused_total and this error make the held lane visible.
				var refusal *replError
				if errors.As(err, &refusal) && refusal.StoreChanged() {
					// Not a refusal of the batch: the parent's store changed.
					// Hand it to the downlink loop and hold every lane.
					c.requestResync()
					c.log.Warn("uplink held: the parent's store has another incarnation than this node recorded — "+
						"re-syncing before anything else is sent",
						"stream", stream, "parent", c.base, "parent_store", refusal.Store, "recorded", parentStore)
					retryPending = true
					return false, false
				}
				refused := errors.As(err, &refusal) && refusal.Refused()
				busy := refusal != nil && refusal.Busy()
				if refused && refusal.Unauthorized() {
					c.unenrolled.refused(time.Now())
				}
				if busy {
					// The parent is shedding load: wait as long as it asked, with
					// jitter so its children do not return in step.
					wait := max(refusal.RetryAfter, retryAfter)
					busyUntil[stream] = time.Now().Add(wait/2 + rand.N(wait)) //nolint:gosec // retry jitter
				}
				if report, attempts, down := c.links.Failed("uplink:"+stream, time.Now()); report {
					switch {
					case busy:
						c.log.Warn("the parent is busy and asked the uplink to retry later — the batch is held, nothing is dropped",
							"stream", stream, "parent", c.base, "retry_after", refusal.RetryAfter,
							"attempts", attempts, "busy_for", down.Round(time.Second))
					case refused:
						c.log.Error("uplink refused by the parent — the batch is held and retried; a record refused alone past the grace is set aside",
							"stream", stream, "parent", c.base, "status", refusal.Status,
							"meaning", replicationStatusMeaning(refusal.Status),
							"parent_said", refusal.Body,
							"attempts", attempts, "held_for", down.Round(time.Second))
					default:
						c.log.Warn("uplink is down (retrying)",
							"stream", stream, "parent", c.base,
							"attempts", attempts, "down_for", down.Round(time.Second), "err", err)
					}
				}
				retryPending = true
				m.UplinkPushFailed(stream)
				if refused {
					m.UplinkRefused(stream)
				}
				if refused && !refusal.Unauthorized() && hold.refused(batch, time.Now(), c.grace()) {
					for _, r := range recs {
						if r.Offset == batch[0].ChildOffset {
							if c.setAside(eng, m, epoch, stream, r, refusal, hold.since) {
								*hold = laneHold{}
								return true, false
							}
							break
						}
					}
				}
				// The cursor stays put. Report "not scanned" so a batch the parent cannot take
				// ends the drain loop instead of spinning.
				return false, false
			}
			c.unenrolled.accepted()
			hold.accepted()
			if wasFailing, attempts, down := c.links.Recovered("uplink:"+stream, time.Now()); wasFailing {
				c.log.Info("uplink recovered",
					"stream", stream, "parent", c.base,
					"attempts", attempts, "down_for", down.Round(time.Second))
			}
			// Every /replicate response carries the parent's clock; keep the skew
			// measurement fresh whichever stream triggered the push.
			eng.ObserveParentClock(pc.sample(sent, received))
			if hwm >= eng.Store().NextOffset(stream) &&
				(env.store != c.storeID() || c.ownStoreBehind(eng.Store(), m, stream, hwm)) {
				// The parent holds an offset this store has not handed out: it
				// dropped the page as records it has. The store has a new
				// incarnation, taken just now or while the page was out, and
				// the cursor stays: the page is sent again under it.
				return true, false
			}
			pushed = true
		}
		if !c.ackUplink(eng.Store(), epoch, stream, next, pushed, hwm) {
			// A re-sync placed the cursors while this page was out: read again
			// from where they stand now.
			return true, false
		}
		if pushed {
			m.UplinkPushed(stream, time.Now())
		}
		c.noteReplayProgress(eng.Store(), m)
		for _, w := range withheld {
			if w.offset < next { // past a trimmed batch's end the page is read again
				m.UplinkLogWithheld(w.level)
			}
		}
		return true, false
	}

	for {
		if stopped() {
			return
		}
		wake, _ := eng.Store().Changes()
		retryPending = false
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
			delay := uplinkIdle // recovery/blob discovery, not ordinary stream pacing
			if retryPending {
				delay = retryAfter
				// A busy parent's wait, when every lane that failed is waiting
				// one out: the soonest of them.
				var soonest time.Duration
				for _, until := range busyUntil {
					if left := time.Until(until); left > 0 && (soonest == 0 || left < soonest) {
						soonest = left
					}
				}
				delay = max(delay, soonest, c.unenrolled.wait(time.Now()))
				wake = nil // commits cannot shorten a failed-request backoff
			}
			select {
			case <-stop:
				return
			case <-wake:
			case <-time.After(delay):
			}
		}
	}
}

// RunDownlink fetches from the parent and hands the result to the engine:
// commands run at their target, definitions are applied where they land. m may
// be nil.
//
// Every contact after an interruption starts with the handshake (hello): the
// parent may have come back on another store, and this node must know before it
// asks from a position or lets the uplink push.
func RunDownlink(c *Client, eng *engine.Engine, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	st := eng.Store()
	if err := c.loadState(st); err != nil {
		c.log.Error("downlink not started: what this node recorded about its parent cannot be read", "err", err)
		return
	}
	m.SetUplinkResyncActive(c.replaying.Load())
	adoptLegacyDownlink(c, st)
	// Read before the first command is taken: it is what keeps a command this
	// node already holds from running again when the parent hands it at
	// another offset.
	memory, err := loadCommandMemory(eng)
	if err != nil {
		c.log.Error("downlink not started: the commands this node holds cannot be read", "err", err)
		return
	}
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}
	pause := func() bool {
		select {
		case <-stop:
			return false
		case <-time.After(max(retryAfter, c.unenrolled.wait(time.Now()))):
			return true
		}
	}
	// handshake retries hello until the parent answered and this node is
	// placed against its store. It returns false on shutdown. first is the
	// contact at start: a child with no cursor for this parent must adopt the
	// head before its first poll, or it would receive every command issued
	// before it attached.
	handshake := func(first bool) bool {
		for {
			res, err := c.hello(ctx, c.greet(st))
			if err == nil {
				c.unenrolled.accepted()
			} else if isUnauthorized(err) {
				c.unenrolled.refused(time.Now())
			}
			if err == nil && c.settle(eng, m, res, first) {
				if wasFailing, attempts, waited := c.links.Recovered("hello", time.Now()); wasFailing {
					c.log.Info("contact with the parent succeeded",
						"parent", c.base, "attempts", attempts, "waited", waited.Round(time.Second))
				}
				c.setStatus(UplinkConnected)
				return true
			}
			if stopped() {
				return false // the request was aborted by our own shutdown
			}
			if err != nil {
				c.setStatus(classifyUplinkErr(err))
				if report, attempts, down := c.links.Failed("hello", time.Now()); report {
					c.log.Warn("contact with the parent has not succeeded yet (retrying) — "+
						"its store and command head must be known before the next poll",
						"parent", c.base, "attempts", attempts, "down_for", down.Round(time.Second), "err", err)
				}
				m.DownlinkFetchFailed()
			}
			if !pause() {
				return false
			}
		}
	}
	if !handshake(true) {
		return
	}
	rehello := false
	for {
		if stopped() {
			return
		}
		if rehello || c.resyncWanted.Load() {
			if !handshake(false) {
				return
			}
			rehello = false
		}
		after := st.CursorGet(uns.DownlinkCursor(c.parentPub), downlinkStream)
		defAfter := st.CursorGet(uns.DownlinkDefCursor(c.parentPub), downlinkDefStream)
		sent := eng.Now()
		res, err := c.pollInterruptibly(ctx, after, defAfter, false)
		received := eng.Now()
		if err == nil {
			c.setStatus(UplinkConnected)
			m.DownlinkFetched(time.Now())
			// The response carries the parent's clock: a skew measurement, nothing more,
			// and none at all when the exchange took too long on the link to bound one.
			// Expiry is judged on this node's own clock.
			eng.ObserveParentClock(res.Clock.sample(sent, received))
			if len(res.Records) > 0 && res.Confirms {
				// The parent wrote this page when its first command arrived there, and
				// the page may have been on the link for a long time since: a link that
				// stalls keeps a written response back for seconds. Ask once more,
				// without waiting, for everything that waits now, so what queued up
				// behind the page is stored with it rather than a moment after it.
				res, err = c.confirmPage(ctx, eng, res, after, defAfter)
			}
		}
		if err != nil {
			if stopped() {
				return // the request was aborted by our own shutdown
			}
			if c.resyncWanted.Load() {
				continue // the uplink loop ended the poll: re-sync first
			}
			var refusal *replError
			if errors.As(err, &refusal) && refusal.StoreChanged() {
				c.resyncWanted.Store(true)
				continue
			}
			if isUnauthorized(err) {
				c.unenrolled.refused(time.Now())
			}
			c.setStatus(classifyUplinkErr(err))
			c.log.Warn("downlink fetch failed (will retry)", "err", err)
			m.DownlinkFetchFailed()
			if !pause() {
				return
			}
			rehello = true
			continue
		}
		recs, next, gap, ancestry := res.Records, res.Next, res.Gap, res.Ancestry
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
		ackTo := c.storePage(eng, m, memory, recs, next)
		progressed := applyDefinitions(c, eng, m, res)
		if ackTo > after {
			st.CursorAck(uns.DownlinkCursor(c.parentPub), downlinkStream, ackTo)
			progressed = true
		}
		if !progressed {
			// Neither cursor moved. Usually that is an idle long poll, but a poll that
			// answered immediately without progress would otherwise spin at the rate limit.
			if !pause() {
				return
			}
		}
	}
}

// confirmPage asks the parent again, without waiting, for what it holds from
// the position page was read at, and returns that answer in page's place. The
// position is the same one, so the parent takes no receipt from the request;
// the commands of page come again, with any issued since. When the request
// fails the page in hand is returned as it is: a command this node holds is not
// kept back on a link that is failing, and the next poll reports the failure.
//
// The one exception is a parent that refuses this node's key: it has revoked
// or retired this node while the page was on the link. A drain that retires a
// child resolved every command it had not seen acknowledged as expired, and a
// revoke ended the parent's obligation to deliver, so storing the page would
// deliver what the parent recorded as never delivered ("deliver OR expire",
// never both). The page is dropped unacknowledged and the refusal is returned
// for the loop to handle like any other refused poll.
func (c *Client) confirmPage(ctx context.Context, eng *engine.Engine, page downResult, after, defAfter uint64) (downResult, error) {
	sent := eng.Now()
	confirmed, err := c.pollInterruptibly(ctx, after, defAfter, true)
	received := eng.Now()
	if isUnauthorized(err) {
		c.log.Warn("downlink page dropped: the parent no longer enrolls this node, so it has resolved the "+
			"commands in hand without this node (revoked, or retired by a drain that let them expire)",
			"parent", c.base, "commands", len(page.Records), "err", err)
		return downResult{}, err
	}
	if err != nil {
		c.log.Warn("downlink page not confirmed with the parent — storing the commands in hand; "+
			"any that queued behind them follow with the next poll",
			"parent", c.base, "commands", len(page.Records), "err", err)
		return page, nil
	}
	eng.ObserveParentClock(confirmed.Clock.sample(sent, received))
	return confirmed, nil
}

// storePage stores the commands of one downlink page and returns the position
// to acknowledge. The page is one write (engine.IngestDownlinkPage): a consumer
// that is handed its first command finds the others behind it.
//
// A command this node refused is skipped; one it could not store holds the
// cursor so the parent offers it again. A refusal (engine.RejectError) will be
// made the same way every time, so rereading it would stall the lane. A store
// failure (full disk, I/O error, record over max_record_bytes) says nothing about
// the command, and acking past it would silently drop it. A page that cannot be
// written as one is therefore stored command by command, which finds the one
// that fails.
func (c *Client) storePage(eng *engine.Engine, m *metrics.Metrics, memory *commandMemory, recs []DownRec, next uint64) (ackTo uint64) {
	st := eng.Store()
	rehanded := c.commandsRehandedBelow()
	fresh := make([]DownRec, 0, len(recs))
	for _, r := range recs {
		// A command this node already holds is the same command at whatever
		// offset it arrives: the parent's store regressed and reuses offsets,
		// or the command came down to the parent a second time.
		if memory.holds(r.Topic, r.Payload, r.TS) && (hasCorrelationID(r.Payload) || r.ParentOffset < rehanded) {
			c.log.Warn("command already held by this node — not stored or executed again",
				"topic", r.Topic, "parent_offset", r.ParentOffset)
			m.DownlinkCommandKnown()
			continue
		}
		fresh = append(fresh, r)
	}
	if len(fresh) == 0 {
		return next
	}
	attribution := func(r DownRec) engine.Attribution {
		return engine.Attribution{
			WrittenBy: r.WrittenBy, ActorID: r.ActorID,
			ActorLabel: r.ActorLabel, ActorKind: r.ActorKind, ActorGroups: r.ActorGroups, ActorClient: r.ActorClient,
		}
	}
	cursor := uns.DownlinkCursor(c.parentPub)
	cmds := make([]engine.DownlinkCommand, len(fresh))
	for i, r := range fresh {
		cmds[i] = engine.DownlinkCommand{Topic: r.Topic, Payload: r.Payload, TS: r.TS, Attribution: attribution(r)}
	}
	// The commands and the cursor past the page are one write: a crash or a lost
	// response makes the parent hand the page again, and it is then recognized as
	// stored instead of being stored and executed a second time.
	results, err := eng.IngestDownlinkPage(cmds, store.CursorAdvance{Name: cursor, Stream: downlinkStream, To: next})
	if err == nil {
		for i, res := range results {
			r := fresh[i]
			switch {
			case res.Err != nil:
				c.logRefused(r, res.Err)
			case res.Persisted:
				memory.add(r.Topic, r.Payload, r.TS, res.Offset, st.LWM("commands"))
			}
		}
		return next
	}
	if !errors.Is(err, engine.ErrNotOnePage) {
		c.log.Warn("downlink page not stored in one write — storing it command by command",
			"commands", len(fresh), "err", err)
	}
	for _, r := range fresh {
		stored, err := eng.IngestDownlinkAttributed(r.Topic, r.Payload, r.TS, attribution(r),
			&store.CursorAdvance{Name: cursor, Stream: downlinkStream, To: r.ParentOffset + 1})
		if err != nil {
			var refused *engine.RejectError
			if errors.As(err, &refused) {
				c.logRefused(r, err)
				continue
			}
			c.log.Error("downlink command not stored — holding the cursor so the parent offers it again",
				"topic", r.Topic, "parent_offset", r.ParentOffset, "err", err)
			return r.ParentOffset
		}
		if stored.Persisted {
			memory.add(r.Topic, r.Payload, r.TS, stored.Offset, st.LWM("commands"))
		}
	}
	return next
}

func (c *Client) logRefused(r DownRec, err error) {
	reason := ""
	var refused *engine.RejectError
	if errors.As(err, &refused) {
		reason = refused.Reason
	}
	c.log.Error("downlink command refused by this node — skipping past it",
		"topic", r.Topic, "reason", reason, "err", err)
}

// pollInterruptibly is one poll that requestResync can end. confirm asks the
// parent to answer at once (see confirmPage).
func (c *Client) pollInterruptibly(ctx context.Context, after, defAfter uint64, confirm bool) (downResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.syncMu.Lock()
	c.pollCancel = cancel
	c.syncMu.Unlock()
	defer func() {
		c.syncMu.Lock()
		c.pollCancel = nil
		c.syncMu.Unlock()
	}()
	if c.resyncWanted.Load() {
		// Asked for between the loop's check and the cancel being in place.
		return downResult{}, context.Canceled
	}
	return c.poll(ctx, after, defAfter, confirm)
}

// settle places this node against the store the parent described in a hello
// response, and applies what the response carries. It reports false when a
// re-sync could not be persisted: nothing moved, and the handshake is repeated.
func (c *Client) settle(eng *engine.Engine, m *metrics.Metrics, res downResult, first bool) bool {
	st := eng.Store()
	// A parent without a head predates the field, as during a rolling upgrade.
	// There is nothing to adopt, so a missing commands cursor stays at 1 and the
	// poll delivers commands issued before this node attached. This is logged on
	// every start against such a parent, even when a cursor already exists.
	if first && res.Head == 0 {
		c.log.Warn("parent answered hello without a command head — it predates parent-scoped "+
			"cursors; if this node has no commands cursor for it yet, that cursor starts at 1 "+
			"and it may execute commands issued under its mount before it attached",
			"parent", c.base, "parent_key", short(c.parentPub))
		m.DownlinkHeadAbsent()
	}
	ownStoreRegressed := c.checkOwnStore(st, m, res)
	switch judgeParent(c.parentStore(), res.Store, res.Resync) {
	case parentChanged:
		if ownStoreRegressed {
			// The parent's marks name offsets of the incarnation this store
			// just left; it clears them at the next push. Nothing of this
			// node's streams is placed by them.
			res.HWMs = nil
		}
		if !c.resync(eng, m, res, !ownStoreRegressed) {
			return false
		}
	case parentFirstSeen:
		c.recordParent(st, res.Store)
	case parentUnchanged, parentPredatesIncarnations:
	}
	c.resyncWanted.Store(false)
	// Before anything is applied, settle first-contact cursors, including adopting
	// the head from this response before the poll reads commands.
	initDownlinkCursors(c, eng, m, res.Head)
	if res.Ancestry != nil {
		eng.SetAncestry(*res.Ancestry)
	}
	_ = applyDefinitions(c, eng, m, res)
	return true
}

// checkOwnStore compares the parent's marks with this node's own streams and
// gives this store a new incarnation when one proves it regressed. It reports
// whether it did.
func (c *Client) checkOwnStore(st *store.Store, m *metrics.Metrics, res downResult) bool {
	for _, stream := range uplinkStreams() {
		if c.ownStoreBehind(st, m, stream, res.HWMs[stream]) {
			return true
		}
	}
	return false
}

// ownStoreBehind handles a parent's mark at or past the head of this node's own
// stream: an offset this store has not handed out. The store regressed,
// typically restored from a backup, and will hand those offsets out again for
// other records; the parent would drop them as already stored and report them
// delivered. A new incarnation makes the parent clear its marks instead. It
// reports whether the store took one.
func (c *Client) ownStoreBehind(st *store.Store, m *metrics.Metrics, stream string, parentHWM uint64) bool {
	head := st.NextOffset(stream)
	if head == 0 || parentHWM < head {
		return false
	}
	if !st.MayHaveRegressed() {
		// The incarnation began in this run, so its history did not regress.
		// The parent holds marks of an earlier one and did not clear them: its
		// colca predates store incarnations. Nothing here changes that.
		return false
	}
	previous := st.StoreID()
	id, err := st.Reincarnate(fmt.Sprintf("the parent holds this node's %s records up to offset %d, the stream here ends at %d",
		stream, parentHWM, head-1))
	if err != nil {
		c.log.Error("this store is behind what its parent holds of it, and could not take a new incarnation — "+
			"the parent drops what this node writes at offsets it holds already", "stream", stream, "err", err)
		return false
	}
	m.StoreReincarnated("regression")
	c.log.Error("this store is behind what its parent holds of it — it was restored from a backup. It takes "+
		"a new incarnation, so the parent accepts what it writes from here on; what it held after the backup is lost here",
		"stream", stream, "parent_holds_up_to", parentHWM, "stream_ends_at", head-1, "previous_store", previous, "store", id)
	return true
}

// hasCorrelationID reports whether a command names itself.
func hasCorrelationID(payload []byte) bool {
	var body struct {
		CorrelationID string `json:"correlation_id"`
	}
	return json.Unmarshal(payload, &body) == nil && body.CorrelationID != ""
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
		if holdsDefinition(eng.Store(), r) {
			continue // read again after a re-sync: nothing to store or hand on
		}
		if _, err := eng.IngestDownlinkDefinitionAttributed(r.Topic, r.Payload, r.TS, engine.Attribution{
			WrittenBy: r.WrittenBy, ActorID: r.ActorID,
			ActorLabel: r.ActorLabel, ActorKind: r.ActorKind, ActorGroups: r.ActorGroups, ActorClient: r.ActorClient,
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

// holdsDefinition reports whether r is, byte for byte, the definition this node
// already holds at that topic. After a re-sync a node reads definitions again
// from where the parent's store stands; storing one it holds would only hand
// the same record to every node below.
func holdsDefinition(st *store.Store, r DownRec) bool {
	if len(r.Payload) == 0 {
		return false // a tombstone is always applied
	}
	p, err := uns.Parse(r.Topic)
	if err != nil {
		return false
	}
	held, ok, err := st.KVGet(p.Path, p.NodeID, r.Topic)
	return err == nil && ok && held.TS == r.TS && bytes.Equal(held.Payload, r.Payload)
}

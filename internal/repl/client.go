package repl

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/alpamayo-solutions/colca/internal/engine"
	"github.com/alpamayo-solutions/colca/internal/identity"
	"github.com/alpamayo-solutions/colca/internal/metrics"
	"github.com/alpamayo-solutions/colca/internal/store"
	"github.com/alpamayo-solutions/colca/plugins/uns"
)

const (
	uplinkCursor   = "uplink"
	downlinkCursor = "downlink"
	// downlinkStream is a pseudo-stream name: the cursor tracks PARENT offsets,
	// which are unrelated to the local commands stream — never mix the two.
	downlinkStream = "commands-parent"
	// The definitions half of the same idea: a separate cursor over a separate
	// pseudo-stream, because a node caught up on commands may still be behind
	// on definitions (definition-stream design §5).
	downlinkDefCursor = "downlink-def"
	downlinkDefStream = "definitions-parent"

	replBatch    = 200
	uplinkIdle   = 150 * time.Millisecond
	downlinkWait = 20 * time.Second
	retryAfter   = 500 * time.Millisecond
)

type Client struct {
	base string
	http *http.Client
	log  *slog.Logger
}

// NewClient: TLS client presenting the child's cert, pinning the parent's pubkey.
func NewClient(baseURL, parentPubHex string, id *identity.Identity) (*Client, error) {
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
	return &Client{
		base: baseURL,
		http: &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}, Timeout: 30 * time.Second},
		log:  slog.Default().With("comp", "repl-client"),
	}, nil
}

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
	wire := make([]wireRec, len(recs))
	for i, r := range recs {
		wire[i] = wireRec{
			O: r.ChildOffset, OO: r.OriginOffset, T: r.Topic, P: r.Payload, TS: r.TS,
			WB: r.WrittenBy, AID: r.ActorID, AL: r.ActorLabel, AK: r.ActorKind,
		}
	}
	body, err := json.Marshal(map[string]any{"stream": stream, "records": wire})
	if err != nil {
		return 0, 0, err
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
		return 0, 0, fmt.Errorf("replicate: http %d", resp.StatusCode)
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
		return downResult{}, fmt.Errorf("downlink: http %d", resp.StatusCode)
	}
	var out struct {
		Records     []wireRec      `json:"records"`
		Next        uint64         `json:"next"`
		Gap         *store.GapSpan `json:"gap"`
		NowMS       int64          `json:"now_ms"`
		Ancestry    *uns.Ancestry  `json:"ancestry"`
		Definitions []wireRec      `json:"definitions"`
		DefNext     uint64         `json:"def_next"`
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

// RunUplink pushes metrics+entities+audit fully and only _Ack and _StreamGap from
// commands, forever (until stop is closed). Commands flow down, acks flow up —
// a command is never mirrored back to the node it came from; _StreamGap
// markers must pass so a pruned commands stream stays honest upstream (spec
// §6.4: the marker replicates like any other record). m may be nil (every
// Metrics method is nil-safe).
func RunUplink(c *Client, eng *engine.Engine, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	streams := []struct {
		name   string
		filter func(string) bool
	}{
		{"metrics", nil},
		{"entities", nil},
		{"audit", nil},
		// `definitions` is deliberately absent and must stay absent
		// (definition-stream design §4): definitions descend. A child pushing
		// them upward would let a leaf author policy for the whole tree.
		{"commands", func(topic string) bool {
			p, err := uns.Parse(topic)
			return err == nil && (p.Contract == "_Ack" || p.Contract == "_StreamGap")
		}},
	}
	for {
		select {
		case <-stop:
			return
		default:
		}
		idle := true
		for _, st := range streams {
			from := eng.Store().CursorGet(uplinkCursor, st.name)
			// Spec §6.3, uplink half: a cursor below the local LWM means the
			// local pruner overrode it (only possible after the explicit §5.2
			// staleness opt-in — the parent was gone longer than the window).
			// The data is gone and the durable §6.4 marker already carries the
			// fact upstream, so never stall: jump to the LWM and keep going.
			if lwm := eng.Store().LWM(st.name); from < lwm {
				c.log.Error("uplink cursor below the stream LWM — local retention pruned past it (spec §6.3): jumping to the LWM",
					"stream", st.name, "position", from, "lwm", lwm)
				m.GapReceived(st.name)
				eng.Store().CursorAck(uplinkCursor, st.name, lwm)
				from = lwm
			}
			recs, next, err := eng.Store().Read(st.name, from, replBatch, st.filter)
			if err != nil {
				c.log.Error("uplink read", "stream", st.name, "err", err)
				continue
			}
			if next == from {
				continue // nothing scanned
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
				_, nowMS, err := c.replicate(ctx, st.name, batch)
				if err != nil {
					select {
					case <-stop:
						return // the request was aborted by our own shutdown, not a real failure
					default:
					}
					c.log.Warn("uplink push failed (will retry)", "stream", st.name, "err", err)
					m.UplinkPushFailed(st.name)
					continue // parent down → cursor stays, offline buffering in action
				}
				// Every /replicate response carries the parent's now_ms
				// (time-sync design §2.1) — keep the offset fresh regardless
				// of which stream happened to trigger this push.
				eng.ApplyClockSample(nowMS)
				pushed = true
			}
			eng.Store().CursorAck(uplinkCursor, st.name, next)
			if pushed {
				m.UplinkPushed(st.name, time.Now())
			}
			idle = false
		}
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
	// first long-poll drains. Failure is fine — the regular polls below carry
	// both on every response.
	if res, err := c.hello(ctx, eng.Store().CursorGet(downlinkDefCursor, downlinkDefStream)); err == nil {
		if res.Ancestry != nil {
			eng.SetAncestry(*res.Ancestry)
		}
		applyDefinitions(c, eng, m, res)
	}
	for {
		select {
		case <-stop:
			return
		default:
		}
		after := eng.Store().CursorGet(downlinkCursor, downlinkStream)
		defAfter := eng.Store().CursorGet(downlinkDefCursor, downlinkDefStream)
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
		for _, r := range recs {
			if _, err := eng.IngestDownlinkAttributed(r.Topic, r.Payload, r.TS, engine.Attribution{
				WrittenBy: r.WrittenBy, ActorID: r.ActorID,
				ActorLabel: r.ActorLabel, ActorKind: r.ActorKind,
			}); err != nil {
				c.log.Error("downlink ingest", "topic", r.Topic, "err", err)
			}
		}
		applyDefinitions(c, eng, m, res)
		if next > after {
			eng.Store().CursorAck(downlinkCursor, downlinkStream, next)
		}
	}
}

// applyDefinitions stores what the parent handed down and advances the
// definitions cursor.
//
// The cursor moves only after every record in the batch was applied, and a
// record that fails leaves it where it was: a definition the node failed to
// store must be offered again, because unlike a command there is no read side
// to recover it from later. That is the same reason the stream is compacted
// rather than pruned (definition-stream design §6).
func applyDefinitions(c *Client, eng *engine.Engine, m *metrics.Metrics, res downResult) {
	for _, r := range res.Definitions {
		if _, err := eng.IngestDownlinkDefinitionAttributed(r.Topic, r.Payload, r.TS, engine.Attribution{
			WrittenBy: r.WrittenBy, ActorID: r.ActorID,
			ActorLabel: r.ActorLabel, ActorKind: r.ActorKind,
		}); err != nil {
			c.log.Error("downlink definition not applied — leaving the cursor so it is offered again",
				"topic", r.Topic, "err", err)
			m.DefinitionRejected()
			return
		}
		m.DefinitionApplied()
	}
	if res.DefNext > eng.Store().CursorGet(downlinkDefCursor, downlinkDefStream) {
		eng.Store().CursorAck(downlinkDefCursor, downlinkDefStream, res.DefNext)
	}
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

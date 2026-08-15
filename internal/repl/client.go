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
	return c.replicate(context.Background(), stream, recs)
}

func (c *Client) replicate(ctx context.Context, stream string, recs []store.ReplRecord) (hwm uint64, err error) {
	wire := make([]wireRec, len(recs))
	for i, r := range recs {
		wire[i] = wireRec{O: r.ChildOffset, T: r.Topic, P: r.Payload, TS: r.TS}
	}
	body, err := json.Marshal(map[string]any{"stream": stream, "records": wire})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/replicate", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("replicate: http %d", resp.StatusCode)
	}
	var out struct {
		HWM uint64 `json:"hwm"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	return out.HWM, nil
}

type DownRec struct {
	ParentOffset uint64
	Topic        string
	Payload      []byte
	TS           int64
}

func (c *Client) Downlink(after uint64, max int, timeout time.Duration) ([]DownRec, uint64, error) {
	return c.downlink(context.Background(), after, max, timeout)
}

func (c *Client) downlink(ctx context.Context, after uint64, max int, timeout time.Duration) ([]DownRec, uint64, error) {
	hc := *c.http
	hc.Timeout = timeout + 10*time.Second
	url := fmt.Sprintf("%s/downlink?after=%d&max=%d", c.base, after, max)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, after, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, after, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, after, fmt.Errorf("downlink: http %d", resp.StatusCode)
	}
	var out struct {
		Records []wireRec `json:"records"`
		Next    uint64    `json:"next"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, after, err
	}
	recs := make([]DownRec, len(out.Records))
	for i, r := range out.Records {
		recs[i] = DownRec{ParentOffset: r.O, Topic: r.T, Payload: r.P, TS: r.TS}
	}
	return recs, out.Next, nil
}

// RunUplink pushes metrics+entities fully and only _Ack from commands, forever
// (until stop is closed). Commands flow down, acks flow up — a command is never
// mirrored back to the node it came from. m may be nil (every Metrics method is
// nil-safe).
func RunUplink(c *Client, eng *engine.Engine, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	streams := []struct {
		name   string
		filter func(string) bool
	}{
		{"metrics", nil},
		{"entities", nil},
		{"commands", func(topic string) bool {
			p, err := uns.Parse(topic)
			return err == nil && p.Contract == "_Ack"
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
					batch[i] = store.ReplRecord{ChildOffset: r.Offset, Topic: r.Topic, Payload: r.Payload, TS: r.TS}
				}
				if _, err := c.replicate(ctx, st.name, batch); err != nil {
					c.log.Warn("uplink push failed (will retry)", "stream", st.name, "err", err)
					m.UplinkPushFailed(st.name)
					continue // parent down → cursor stays, offline buffering in action
				}
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

// RunDownlink fetches commands from the parent and hands them to the engine
// (persist with the parent's original timestamp + local delivery). m may be
// nil (every Metrics method is nil-safe).
func RunDownlink(c *Client, eng *engine.Engine, m *metrics.Metrics, stop <-chan struct{}) {
	ctx, cancel := contextFromStop(stop)
	defer cancel()
	for {
		select {
		case <-stop:
			return
		default:
		}
		after := eng.Store().CursorGet(downlinkCursor, downlinkStream)
		recs, next, err := c.downlink(ctx, after, replBatch, downlinkWait)
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
		for _, r := range recs {
			if _, err := eng.IngestDownlink(r.Topic, r.Payload, r.TS); err != nil {
				c.log.Error("downlink ingest", "topic", r.Topic, "err", err)
			}
		}
		if next > after {
			eng.Store().CursorAck(downlinkCursor, downlinkStream, next)
		}
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

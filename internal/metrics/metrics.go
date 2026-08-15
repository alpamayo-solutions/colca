// Package metrics owns Colca's Prometheus surface: cheap
// atomic counters incremented from the hot paths, and a store-reading
// collector that derives every gauge (stream offsets, cursor lag, child HWMs)
// from the store AT SCRAPE TIME — no background sampling, no self-reported
// state.
//
// The family names and labels below are a contract: the retention plan
// (Plan C) references them. `colca_stream_low_water_mark` is reserved for
// Plan C — do not reuse the name.
//
// Every increment method is safe on a nil *Metrics receiver (no-op), so the
// engine, broker and repl packages stay testable without a registry.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/alpamayo-solutions/colca/internal/store"
)

// Reject reasons — the allowed label values of colca_rejected_publishes_total.
const (
	ReasonIdentity   = "identity"    // topic level 4 does not match the sender's identity
	ReasonGrammar    = "grammar"     // topic does not parse as uns grammar
	ReasonValidation = "validation"  // payload fails the contract's schema
	ReasonNoMount    = "no_mount"    // no mount configured for the sender
	ReasonNotCommand = "not_command" // a client published a _Cmd* topic
	ReasonAuth       = "auth"        // broker CONNECT authentication failed
)

var reasons = []string{ReasonIdentity, ReasonGrammar, ReasonValidation, ReasonNoMount, ReasonNotCommand, ReasonAuth}

// streams mirrors the store's fixed stream set (store.streams; the same list
// the /debug/state route enumerates).
var streams = []string{"metrics", "entities", "commands"}

// Metrics is the node's metric registry plus the pre-created children the hot
// paths increment. Labels are resolved once at construction — never per
// message.
type Metrics struct {
	reg *prometheus.Registry

	ingest       *prometheus.CounterVec
	rejected     *prometheus.CounterVec
	uplinkOK     *prometheus.GaugeVec
	uplinkFail   *prometheus.CounterVec
	downlinkOK   prometheus.Gauge
	downlinkFail prometheus.Counter
	reseed       prometheus.Gauge

	ingestBy     map[string]prometheus.Counter
	rejectedBy   map[string]prometheus.Counter
	uplinkOKBy   map[string]prometheus.Gauge
	uplinkFailBy map[string]prometheus.Counter
}

// New builds the registry: the store collector plus every counter/gauge
// family, pre-created and zero-valued so all families are present from the
// first scrape.
func New(st *store.Store) *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		ingest: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_ingest_records_total",
			Help: "Records durably persisted, by stream. Resets on restart.",
		}, []string{"stream"}),
		rejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_rejected_publishes_total",
			Help: "Publishes rejected before persistence, by reason. Resets on restart.",
		}, []string{"reason"}),
		uplinkOK: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "colca_uplink_last_success_timestamp_seconds",
			Help: "Unix time of the last successful uplink push+ack, by stream (0 = never this process).",
		}, []string{"stream"}),
		uplinkFail: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_uplink_push_failures_total",
			Help: "Failed uplink pushes, by stream. Resets on restart.",
		}, []string{"stream"}),
		downlinkOK: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_downlink_last_success_timestamp_seconds",
			Help: "Unix time of the last successful downlink fetch, empty fetches included (0 = never this process).",
		}),
		downlinkFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "colca_downlink_fetch_failures_total",
			Help: "Failed downlink fetches. Resets on restart.",
		}),
		reseed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_retained_reseed_records",
			Help: "KV entries replayed into the broker's retained set at startup.",
		}),
	}
	m.ingestBy = counterChildren(m.ingest, streams)
	m.rejectedBy = counterChildren(m.rejected, reasons)
	m.uplinkFailBy = counterChildren(m.uplinkFail, streams)
	m.uplinkOKBy = make(map[string]prometheus.Gauge, len(streams))
	for _, s := range streams {
		m.uplinkOKBy[s] = m.uplinkOK.WithLabelValues(s)
	}
	m.reg.MustRegister(m.ingest, m.rejected, m.uplinkOK, m.uplinkFail,
		m.downlinkOK, m.downlinkFail, m.reseed, &storeCollector{st: st})
	return m
}

// counterChildren pre-resolves one child per known label value, so incrementing
// is a plain map hit — no per-message label allocation or vec lookup.
func counterChildren(vec *prometheus.CounterVec, labels []string) map[string]prometheus.Counter {
	out := make(map[string]prometheus.Counter, len(labels))
	for _, l := range labels {
		out[l] = vec.WithLabelValues(l)
	}
	return out
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// IngestRecord counts one durably persisted record on a stream.
func (m *Metrics) IngestRecord(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.ingestBy[stream]; ok {
		c.Inc()
		return
	}
	m.ingest.WithLabelValues(stream).Inc()
}

// RejectPublish counts one rejected publish. reason should be one of the
// Reason* constants.
func (m *Metrics) RejectPublish(reason string) {
	if m == nil {
		return
	}
	if c, ok := m.rejectedBy[reason]; ok {
		c.Inc()
		return
	}
	m.rejected.WithLabelValues(reason).Inc()
}

// UplinkPushed records a successful uplink push+ack cycle for a stream.
func (m *Metrics) UplinkPushed(stream string, at time.Time) {
	if m == nil {
		return
	}
	g, ok := m.uplinkOKBy[stream]
	if !ok {
		g = m.uplinkOK.WithLabelValues(stream)
	}
	g.Set(float64(at.UnixNano()) / 1e9)
}

// UplinkPushFailed counts one failed uplink push for a stream.
func (m *Metrics) UplinkPushFailed(stream string) {
	if m == nil {
		return
	}
	if c, ok := m.uplinkFailBy[stream]; ok {
		c.Inc()
		return
	}
	m.uplinkFail.WithLabelValues(stream).Inc()
}

// DownlinkFetched records a successful downlink fetch — empty fetches count:
// progress means the loop is alive, not that data flowed.
func (m *Metrics) DownlinkFetched(at time.Time) {
	if m == nil {
		return
	}
	m.downlinkOK.Set(float64(at.UnixNano()) / 1e9)
}

// DownlinkFetchFailed counts one failed downlink fetch.
func (m *Metrics) DownlinkFetchFailed() {
	if m == nil {
		return
	}
	m.downlinkFail.Inc()
}

// SetReseedCount records how many KV entries the startup reseed replayed.
func (m *Metrics) SetReseedCount(n int) {
	if m == nil {
		return
	}
	m.reseed.Set(float64(n))
}

// storeCollector derives the gauge families from the store at scrape time.
type storeCollector struct {
	st *store.Store
}

var (
	descNextOffset = prometheus.NewDesc("colca_stream_next_offset",
		"Next offset the stream will assign (derived from the store at scrape time).",
		[]string{"stream"}, nil)
	descCursorPos = prometheus.NewDesc("colca_cursor_position",
		"Next offset the named cursor will read (derived from the store at scrape time).",
		[]string{"cursor", "stream"}, nil)
	descCursorLag = prometheus.NewDesc("colca_cursor_lag_records",
		"Records the cursor has not read yet: next_offset - position, floored at 0.",
		[]string{"cursor", "stream"}, nil)
	descChildHWM = prometheus.NewDesc("colca_child_hwm",
		"Highest child offset already applied, per (child, stream).",
		[]string{"child", "stream"}, nil)
)

func (c *storeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- descNextOffset
	ch <- descCursorPos
	ch <- descCursorLag
	ch <- descChildHWM
}

func (c *storeCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range streams {
		ch <- prometheus.MustNewConstMetric(descNextOffset, prometheus.GaugeValue,
			float64(c.st.NextOffset(s)), s)
	}
	for _, cur := range c.st.Cursors() {
		ch <- prometheus.MustNewConstMetric(descCursorPos, prometheus.GaugeValue,
			float64(cur.Position), cur.Name, cur.Stream)
		var lag uint64
		if next := c.st.NextOffset(cur.Stream); next > cur.Position {
			lag = next - cur.Position
		}
		ch <- prometheus.MustNewConstMetric(descCursorLag, prometheus.GaugeValue,
			float64(lag), cur.Name, cur.Stream)
	}
	for _, h := range c.st.HWMs() {
		ch <- prometheus.MustNewConstMetric(descChildHWM, prometheus.GaugeValue,
			float64(h.HWM), h.Child, h.Stream)
	}
}

package grantsync

import "github.com/prometheus/client_golang/prometheus"

// Metrics is the service's Prometheus surface. Every method is safe on a nil
// receiver, so the package stays testable without a registry — the same
// convention colca's own metrics package uses.
type Metrics struct {
	cycles      *prometheus.CounterVec
	resources   *prometheus.CounterVec
	definitions *prometheus.CounterVec
	problems    prometheus.Gauge
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		cycles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_grantsync_cycles_total",
			Help: "Sync cycles, by outcome. A failed cycle wrote nothing.",
		}, []string{"result"}),
		resources: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_grantsync_resources_total",
			Help: "Element resources written to Keycloak, by action.",
		}, []string{"action"}),
		definitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "colca_grantsync_definitions_total",
			Help: "_Group definitions authored into the tree, by action.",
		}, []string{"action"}),
		problems: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "colca_grantsync_problems",
			Help: "States seen in the last cycle that a human must resolve: " +
				"unmanaged resources, foreign definitions, grants on unknown elements.",
		}),
	}
	if reg != nil {
		reg.MustRegister(m.cycles, m.resources, m.definitions, m.problems)
	}
	return m
}

func (m *Metrics) cycle(result string) {
	if m != nil {
		m.cycles.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) resourceRegistered() {
	if m != nil {
		m.resources.WithLabelValues("registered").Inc()
	}
}

func (m *Metrics) resourceRemoved() {
	if m != nil {
		m.resources.WithLabelValues("removed").Inc()
	}
}

func (m *Metrics) definitionWritten() {
	if m != nil {
		m.definitions.WithLabelValues("written").Inc()
	}
}

func (m *Metrics) definitionRetracted() {
	if m != nil {
		m.definitions.WithLabelValues("retracted").Inc()
	}
}

func (m *Metrics) observeProblems(n int) {
	if m != nil {
		m.problems.Set(float64(n))
	}
}

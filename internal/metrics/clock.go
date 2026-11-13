package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpamayo-solutions/colca/internal/clock"
)

// clockCollector exports what the node knows about its clock, read at scrape
// time. A series is exported only while its value is known: the host series
// when the kernel's verdict can be read, the parent series on a node with a
// parent once a response from it arrived. An absent series is "not known",
// which a 0 could not say.
type clockCollector struct{ clk *clock.Clock }

var (
	hostClockSynchronized = prometheus.NewDesc("colca_host_clock_synchronized", "1 when the kernel reports the host clock as synchronized by an NTP daemon, 0 when not. Read with a read-only adjtimex. Absent when the kernel's verdict cannot be read (not Linux, or the call is refused).", nil, nil)
	hostClockEstimated    = prometheus.NewDesc("colca_host_clock_estimated_error_seconds", "The NTP service's estimate of the host clock's error, in seconds, as the kernel holds it. Absent when the kernel's verdict cannot be read, and on a host whose service keeps no estimate (systemd-timesyncd writes 0): an absent series is not an error of zero.", nil, nil)
	hostClockMaxError     = prometheus.NewDesc("colca_host_clock_max_error_seconds", "The kernel's bound on the host clock's error, in seconds. The NTP service sets it at every update and the kernel adds 0.0005 for each second since. Absent when the kernel's verdict cannot be read.", nil, nil)
	hostClockStaleBound   = prometheus.NewDesc("colca_host_clock_stale_bound_seconds", "The part of colca_host_clock_max_error_seconds one NTP poll interval does not explain (above 1.024 s): 0 while the clock is updated, then 0.0005 for each second since an update came due. Maintained under chrony, ntpd and systemd-timesyncd alike. Absent when the kernel's verdict cannot be read.", nil, nil)
	clockParentSkew       = prometheus.NewDesc("colca_clock_parent_skew_seconds", "Parent's clock minus this node's clock, in seconds, from the now_ms of the latest /downlink or /replicate response whose round trip was at most 0.5 s, taken at the middle of that round trip; positive when the parent is ahead. A measurement only: nothing is corrected by it. Absent on the root and before the first such response.", nil, nil)
	clockParentSampleAge  = prometheus.NewDesc("colca_clock_parent_sample_age_seconds", "Seconds since colca_clock_parent_skew_seconds was last measured. Absent on the root and before the first measurement.", nil, nil)
)

func (c *clockCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		hostClockSynchronized, hostClockEstimated, hostClockMaxError, hostClockStaleBound, clockParentSkew, clockParentSampleAge,
	} {
		ch <- d
	}
}

func (c *clockCollector) Collect(ch chan<- prometheus.Metric) {
	if c.clk == nil {
		return
	}
	gauge := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v)
	}
	if host := c.clk.Host(); host.Reported {
		synchronized := 0.0
		if host.Synchronized {
			synchronized = 1
		}
		gauge(hostClockSynchronized, synchronized)
		if host.EstimateReported {
			gauge(hostClockEstimated, host.EstimatedError.Seconds())
		}
		gauge(hostClockMaxError, host.MaxError.Seconds())
		gauge(hostClockStaleBound, host.StaleBound().Seconds())
	}
	if skew, ok := c.clk.ParentSkew(); ok {
		gauge(clockParentSkew, skew.Seconds())
	}
	// clk.Now() keeps tests with a fake clock deterministic.
	if age, ok := c.clk.ParentSampleAge(c.clk.Now()); ok {
		gauge(clockParentSampleAge, age.Seconds())
	}
}

package edgecdnxplugin

import (
	"github.com/coredns/coredns/plugin"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// requestCount exports a prometheus metric that is incremented every time a query is seen by the example plugin.
var requestCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: plugin.Namespace,
	Subsystem: "edgecdnx",
	Name:      "request_count_total",
	Help:      "Counter of requests made.",
}, []string{"server"})

var routingTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: plugin.Namespace,
	Subsystem: "edgeroute",
	Name:      "routing_total",
	Help:      "Bounded-cardinality EdgeRoute selection outcomes.",
}, []string{"location", "node", "result"})

var fallbackTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: plugin.Namespace,
	Subsystem: "edgeroute",
	Name:      "fallback_total",
	Help:      "Fallback attempts between configured EdgeCDN-X locations.",
}, []string{"from", "to", "reason"})

var nodeUnavailableTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: plugin.Namespace,
	Subsystem: "edgeroute",
	Name:      "node_unavailable_total",
	Help:      "Nodes excluded from an EdgeRoute candidate set.",
}, []string{"node", "reason"})

var selectionDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: plugin.Namespace,
	Subsystem: "edgeroute",
	Name:      "selection_duration_seconds",
	Help:      "Time spent filtering and selecting an edge node.",
	Buckets:   prometheus.ExponentialBuckets(0.000001, 2, 16),
})

var snapshotAge = promauto.NewGauge(prometheus.GaugeOpts{
	Namespace: plugin.Namespace,
	Subsystem: "edgeroute",
	Name:      "snapshot_age_seconds",
	Help:      "Age of the immutable NodeQuality snapshot used by DNS requests.",
})

package quality

import (
	"github.com/prometheus/client_golang/prometheus"
	controllermetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	ejectionOverflowTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "quality_controller_ejection_overflow_total",
		Help: "Number of node ejections blocked by the per-location safety limit.",
	}, []string{"location", "node"})
	stateTransitionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "quality_controller_state_transitions_total",
		Help: "Number of observed NodeQuality state transitions.",
	}, []string{"location", "node", "from", "to"})
)

func init() {
	controllermetrics.Registry.MustRegister(ejectionOverflowTotal, stateTransitionsTotal)
}

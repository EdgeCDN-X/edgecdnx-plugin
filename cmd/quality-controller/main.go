package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	adaptivev1alpha1 "github.com/EdgeCDN-X/edgecdnx-plugin/api/v1alpha1"
	"github.com/EdgeCDN-X/edgecdnx-plugin/internal/quality"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func main() {
	config := quality.LabDefaults()
	queries := quality.DefaultPrometheusQueries()
	var prometheusURL, recoverySteps string
	var interval, prometheusTimeout time.Duration
	var failureThreshold, maxEjectedPercent, healthySamples, minimumWeightDelta int
	var leaderElect bool
	flag.StringVar(&prometheusURL, "prometheus-url", "http://monitoring-kube-prometheus-prometheus.monitoring.svc:9090", "Prometheus base URL")
	flag.DurationVar(&interval, "reconcile-interval", 5*time.Second, "NodeQuality refresh interval")
	flag.DurationVar(&prometheusTimeout, "prometheus-timeout", 3*time.Second, "Prometheus query timeout")
	flag.BoolVar(&leaderElect, "leader-elect", true, "use a Kubernetes Lease so only one controller writes status")
	flag.DurationVar(&config.MetricStaleAfter, "metric-stale-after", config.MetricStaleAfter, "age at which last-known-good metrics become stale")
	flag.DurationVar(&config.HardStaleAfter, "hard-stale-after", config.HardStaleAfter, "age at which a stale node is removed when safety permits")
	flag.Float64Var(&config.LatencyAlpha, "latency-ewma-alpha", config.LatencyAlpha, "latency EWMA alpha")
	flag.Float64Var(&config.ErrorAlpha, "error-ewma-alpha", config.ErrorAlpha, "error-rate EWMA alpha")
	failureThreshold = int(config.ConsecutiveFailureThreshold)
	flag.IntVar(&failureThreshold, "consecutive-error-threshold", failureThreshold, "consecutive failures before ejection")
	flag.DurationVar(&config.BaseEjectionTime, "base-ejection-time", config.BaseEjectionTime, "first ejection cooldown")
	flag.DurationVar(&config.MaxEjectionTime, "max-ejection-time", config.MaxEjectionTime, "maximum ejection cooldown")
	maxEjectedPercent = int(config.MaxEjectedPercent)
	flag.IntVar(&maxEjectedPercent, "max-ejection-percent", maxEjectedPercent, "maximum ejected nodes per location")
	recoverySteps = "30s,60s,120s"
	flag.StringVar(&recoverySteps, "recovery-steps", recoverySteps, "three comma-separated progressive recovery durations")
	flag.Uint64Var(&config.MinimumSampleCount, "minimum-sample-count", config.MinimumSampleCount, "minimum requests required for window rules")
	flag.Float64Var(&config.ErrorRateThreshold, "error-rate-threshold", config.ErrorRateThreshold, "one-minute error-rate ejection threshold")
	flag.Float64Var(&config.LatencyDegradedFactor, "latency-degraded-factor", config.LatencyDegradedFactor, "latency EWMA multiplier that marks a node degraded")
	healthySamples = int(config.HealthySamplesToRecover)
	flag.IntVar(&healthySamples, "healthy-samples-to-recover", healthySamples, "healthy windows required before leaving Degraded")
	minimumWeightDelta = int(config.MinimumWeightDelta)
	flag.IntVar(&minimumWeightDelta, "minimum-weight-delta", minimumWeightDelta, "smallest published weight change")
	flag.StringVar(&queries.ActiveRequests, "query-active-requests", queries.ActiveRequests, "PromQL template; must contain $NODE")
	flag.StringVar(&queries.RequestCount, "query-request-count", queries.RequestCount, "PromQL template; must contain $NODE")
	flag.StringVar(&queries.ErrorCount, "query-error-count", queries.ErrorCount, "PromQL template; must contain $NODE")
	flag.StringVar(&queries.P95Latency, "query-p95-latency", queries.P95Latency, "PromQL template; must contain $NODE")
	flag.StringVar(&queries.BaselineLatency, "query-baseline-latency", queries.BaselineLatency, "PromQL template; must contain $NODE")
	flag.StringVar(&queries.CacheHits, "query-cache-hits", queries.CacheHits, "PromQL template; must contain $NODE")
	flag.StringVar(&queries.Cacheable, "query-cacheable", queries.Cacheable, "PromQL template; must contain $NODE")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	config.ConsecutiveFailureThreshold = int32(failureThreshold)
	config.MaxEjectedPercent = int32(maxEjectedPercent)
	config.HealthySamplesToRecover = int32(healthySamples)
	config.MinimumWeightDelta = int32(minimumWeightDelta)
	steps, err := quality.ParseRecoverySteps(recoverySteps)
	if err != nil {
		fatalConfig(err)
	}
	config.RecoverySteps = steps
	if interval <= 0 || prometheusTimeout <= 0 {
		fatalConfig(fmt.Errorf("reconcile interval and Prometheus timeout must be positive"))
	}
	if err := config.Validate(); err != nil {
		fatalConfig(err)
	}
	provider, err := quality.NewPrometheusProvider(prometheusURL, prometheusTimeout, queries)
	if err != nil {
		fatalConfig(err)
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		os.Exit(1)
	}
	if err := adaptivev1alpha1.AddToScheme(scheme); err != nil {
		os.Exit(1)
	}
	if err := infrastructurev1alpha1.AddToScheme(scheme); err != nil {
		os.Exit(1)
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Cache:                         cache.Options{DefaultNamespaces: map[string]cache.Config{"edge-system": {}}},
		Metrics:                       metricsserver.Options{BindAddress: ":8080"},
		HealthProbeBindAddress:        ":8081",
		LeaderElection:                leaderElect,
		LeaderElectionID:              "quality-controller.adaptive.edgecdnx.io",
		LeaderElectionNamespace:       "edge-system",
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		ctrl.Log.Error(err, "create manager")
		os.Exit(1)
	}
	reconciler := &quality.Reconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Provider: provider, Interval: interval, Engine: quality.Engine{Config: config}, Clock: quality.RealClock{}}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		ctrl.Log.Error(err, "setup controller")
		os.Exit(1)
	}
	_ = mgr.AddHealthzCheck("healthz", healthz.Ping)
	_ = mgr.AddReadyzCheck("readyz", healthz.Ping)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		ctrl.Log.Error(err, "run manager")
		os.Exit(1)
	}
}

func fatalConfig(err error) {
	fmt.Fprintf(os.Stderr, "invalid quality-controller configuration: %v\n", err)
	os.Exit(2)
}

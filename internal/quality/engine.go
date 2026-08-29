package quality

import (
	"fmt"
	"math"
	"strings"
	"time"

	adaptivev1alpha1 "github.com/EdgeCDN-X/edgecdnx-plugin/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	StateUnknown    = "Unknown"
	StateHealthy    = "Healthy"
	StateDegraded   = "Degraded"
	StateEjected    = "Ejected"
	StateRecovering = "Recovering"
	StateStale      = "Stale"
	StateDisabled   = "Disabled"
)

type Clock interface{ Now() time.Time }
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

type FakeClock struct{ Current time.Time }

func (f *FakeClock) Now() time.Time          { return f.Current }
func (f *FakeClock) Advance(d time.Duration) { f.Current = f.Current.Add(d) }

type Config struct {
	LatencyAlpha                float64
	ErrorAlpha                  float64
	MinimumSampleCount          uint64
	ConsecutiveFailureThreshold int32
	ErrorRateThreshold          float64
	LatencyDegradedFactor       float64
	MetricStaleAfter            time.Duration
	BaseEjectionTime            time.Duration
	MaxEjectionTime             time.Duration
	MaxEjectedPercent           int32
	HealthySamplesToRecover     int32
	MinimumWeightDelta          int32
	RecoverySteps               []time.Duration
	HardStaleAfter              time.Duration
}

func LabDefaults() Config {
	return Config{
		LatencyAlpha:                0.2,
		ErrorAlpha:                  0.3,
		MinimumSampleCount:          50,
		ConsecutiveFailureThreshold: 5,
		ErrorRateThreshold:          0.10,
		LatencyDegradedFactor:       12,
		MetricStaleAfter:            30 * time.Second,
		HardStaleAfter:              5 * time.Minute,
		BaseEjectionTime:            30 * time.Second,
		MaxEjectionTime:             5 * time.Minute,
		MaxEjectedPercent:           50,
		HealthySamplesToRecover:     3,
		MinimumWeightDelta:          5,
		RecoverySteps:               []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second},
	}
}

func ParseRecoverySteps(value string) ([]time.Duration, error) {
	parts := strings.Split(value, ",")
	steps := make([]time.Duration, len(parts))
	for i, part := range parts {
		duration, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("parse recovery step %q: %w", part, err)
		}
		steps[i] = duration
	}
	return steps, nil
}

func (c Config) Validate() error {
	if c.LatencyAlpha < 0 || c.LatencyAlpha > 1 || math.IsNaN(c.LatencyAlpha) {
		return fmt.Errorf("latency EWMA alpha must be within [0,1]")
	}
	if c.ErrorAlpha < 0 || c.ErrorAlpha > 1 || math.IsNaN(c.ErrorAlpha) {
		return fmt.Errorf("error EWMA alpha must be within [0,1]")
	}
	if c.MinimumSampleCount == 0 || c.ConsecutiveFailureThreshold <= 0 {
		return fmt.Errorf("sample and consecutive failure thresholds must be positive")
	}
	if c.ErrorRateThreshold < 0 || c.ErrorRateThreshold > 1 || math.IsNaN(c.ErrorRateThreshold) {
		return fmt.Errorf("error rate threshold must be within [0,1]")
	}
	if c.LatencyDegradedFactor < 1 || math.IsNaN(c.LatencyDegradedFactor) || math.IsInf(c.LatencyDegradedFactor, 0) {
		return fmt.Errorf("latency degraded factor must be at least 1")
	}
	if c.MetricStaleAfter <= 0 || c.HardStaleAfter <= c.MetricStaleAfter {
		return fmt.Errorf("hard stale duration must exceed metric stale duration")
	}
	if c.BaseEjectionTime <= 0 || c.MaxEjectionTime < c.BaseEjectionTime {
		return fmt.Errorf("maximum ejection time must be at least the base ejection time")
	}
	if c.MaxEjectedPercent < 1 || c.MaxEjectedPercent > 100 {
		return fmt.Errorf("maximum ejected percent must be within [1,100]")
	}
	if c.HealthySamplesToRecover <= 0 || c.MinimumWeightDelta < 0 || c.MinimumWeightDelta > 100 {
		return fmt.Errorf("recovery samples and weight delta are outside valid bounds")
	}
	if len(c.RecoverySteps) != 3 || c.RecoverySteps[0] <= 0 || c.RecoverySteps[1] <= c.RecoverySteps[0] || c.RecoverySteps[2] <= c.RecoverySteps[1] {
		return fmt.Errorf("recovery steps must contain three increasing positive durations")
	}
	return nil
}

type Cohort struct {
	TotalEnabled      int32
	Ejected           int32
	FallbackAvailable bool
}

func UpdateEWMA(previous, sample, alpha float64, initialized bool) (float64, error) {
	if math.IsNaN(previous) || math.IsInf(previous, 0) || math.IsNaN(sample) || math.IsInf(sample, 0) || sample < 0 {
		return 0, fmt.Errorf("invalid EWMA input")
	}
	if alpha < 0 || alpha > 1 || math.IsNaN(alpha) {
		return 0, fmt.Errorf("alpha must be within [0,1]")
	}
	if !initialized {
		return sample, nil
	}
	return alpha*sample + (1-alpha)*previous, nil
}

type Engine struct{ Config Config }

func (e Engine) Evaluate(spec adaptivev1alpha1.NodeQualitySpec, previous adaptivev1alpha1.NodeQualityStatus, sample NodeSample, cohort Cohort, now time.Time) (adaptivev1alpha1.NodeQualityStatus, error) {
	if err := e.Config.Validate(); err != nil {
		return previous, fmt.Errorf("invalid engine config: %w", err)
	}
	if err := validateSample(sample); err != nil {
		return previous, err
	}
	status := previous
	observed := metav1.NewTime(sample.Timestamp)
	if sample.Timestamp.IsZero() {
		observed = metav1.NewTime(now)
	}
	status.ObservedAt = &observed
	status.SampleCount = int64(sample.RequestCount)
	status.ActiveRequests = int32(sample.ActiveRequests)
	status.ConcurrencyLimit = spec.StaticCapacity
	status.CacheHitRatio = sample.CacheHitRatio
	if !spec.Enabled {
		return e.disabled(status, now), nil
	}
	initialized := previous.ObservedAt != nil
	latency, err := UpdateEWMA(previous.LatencyEWMAMillis, float64(sample.P95Latency)/float64(time.Millisecond), e.Config.LatencyAlpha, initialized)
	if err != nil {
		return previous, err
	}
	errorRate := 0.0
	if sample.RequestCount > 0 {
		errorRate = float64(sample.ErrorCount) / float64(sample.RequestCount)
	}
	errorEWMA, err := UpdateEWMA(previous.ErrorEWMA, errorRate, e.Config.ErrorAlpha, initialized)
	if err != nil {
		return previous, err
	}
	status.LatencyEWMAMillis, status.ErrorEWMA = latency, errorEWMA

	failing := sample.ProbeFailed || (sample.ErrorCount > 0 && sample.FailureKind != "client_cancel")
	if failing {
		status.ConsecutiveFailures++
		status.LastFailureType = normalizedFailure(sample)
	} else {
		status.ConsecutiveFailures = 0
	}
	enough := sample.RequestCount >= e.Config.MinimumSampleCount
	baselineMillis := float64(sample.BaselineLatency) / float64(time.Millisecond)
	latencyDegraded := baselineMillis > 0 && status.LatencyEWMAMillis > baselineMillis*e.Config.LatencyDegradedFactor
	eject := sample.ProbeFailed || status.ConsecutiveFailures >= e.Config.ConsecutiveFailureThreshold || (enough && errorRate >= e.Config.ErrorRateThreshold) || (previous.State == StateRecovering && failing)
	healthyWindow := enough && !eject && !latencyDegraded
	if healthyWindow {
		status.ConsecutiveHealthy++
	} else {
		status.ConsecutiveHealthy = 0
	}

	if previous.State == StateEjected && previous.EjectedUntil != nil && now.Before(previous.EjectedUntil.Time) {
		status.State, status.Reason, status.EffectiveWeight, status.QualityScore = StateEjected, "ejection cooldown active", 0, 0
		return status, nil
	}
	if previous.State == StateEjected && !eject {
		setState(&status, StateRecovering, "ejection cooldown elapsed", now)
		status.RecoveryStep = 0
	}
	if previous.State == StateStale && previous.EjectedUntil != nil && !eject {
		setState(&status, StateRecovering, "fresh metrics resumed interrupted recovery", now)
		status.RecoveryStep = 0
	}
	if eject {
		if e.canEject(cohort) || previous.State == StateEjected {
			e.eject(&status, now)
			return status, nil
		}
		setState(&status, StateDegraded, "ejection blocked by maximum percentage", now)
	} else if status.State == StateRecovering {
		e.advanceRecovery(&status, healthyWindow, now)
	} else if enough {
		if latencyDegraded {
			setState(&status, StateDegraded, "latency above degraded threshold", now)
		} else if status.State == StateDegraded && status.ConsecutiveHealthy < e.Config.HealthySamplesToRecover {
			status.Reason = "waiting for consecutive healthy samples"
		} else {
			setState(&status, StateHealthy, "metrics within lab thresholds", now)
		}
	} else if status.State == "" {
		setState(&status, StateUnknown, "minimum sample count not reached", now)
	}
	publishBoundary := previous.State != status.State || (status.State == StateRecovering && previous.RecoveryStep != status.RecoveryStep)
	e.computeWeight(spec, sample, &status, publishBoundary)
	return status, nil
}

func (e Engine) MarkMetricsUnavailable(spec adaptivev1alpha1.NodeQualitySpec, previous adaptivev1alpha1.NodeQualityStatus, cohort Cohort, now time.Time) adaptivev1alpha1.NodeQualityStatus {
	status := previous
	if !spec.Enabled {
		return e.disabled(status, now)
	}
	if previous.State == StateEjected {
		status.EffectiveWeight, status.QualityScore = 0, 0
		status.LastFailureType = "metrics_stale"
		if previous.EjectedUntil != nil && now.Before(previous.EjectedUntil.Time) {
			status.Reason = "ejection cooldown active; metrics unavailable"
		} else {
			status.Reason = "ejection cooldown elapsed; waiting for fresh metrics"
		}
		return status
	}
	if previous.ObservedAt != nil && now.Sub(previous.ObservedAt.Time) >= e.Config.MetricStaleAfter {
		setState(&status, StateStale, "Prometheus metrics are stale", now)
		status.LastFailureType = "metrics_stale"
		target := int32(math.Round(float64(spec.StaticCapacity) * 0.2))
		if previous.EffectiveWeight < target {
			target = previous.EffectiveWeight
		}
		status.QualityScore = 0
		if spec.StaticCapacity > 0 {
			status.QualityScore = float64(target) / float64(spec.StaticCapacity)
		}
		hardStale := now.Sub(previous.ObservedAt.Time) >= e.Config.HardStaleAfter && e.canEject(cohort)
		if hardStale {
			status.Reason = "Prometheus metrics exceeded hard-stale limit"
			status.QualityScore = 0
			target = 0
		}
		status.EffectiveWeight = quantizeWeight(previous.EffectiveWeight, target, e.Config.MinimumWeightDelta, hardStale)
	}
	return status
}

func validateSample(s NodeSample) error {
	values := []float64{s.CacheHitRatio, s.CPUUtilisation, s.BandwidthUtilisation}
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1 {
			return fmt.Errorf("ratio outside [0,1]")
		}
	}
	if s.P95Latency < 0 || s.BaselineLatency < 0 || s.ActiveRequests < 0 || s.ErrorCount > s.RequestCount {
		return fmt.Errorf("invalid node sample")
	}
	return nil
}

func normalizedFailure(s NodeSample) string {
	if s.ProbeFailed {
		return "probe_failure"
	}
	switch s.FailureKind {
	case "origin_5xx", "connect_timeout", "request_timeout", "client_cancel":
		return s.FailureKind
	}
	return "origin_5xx"
}

func (e Engine) canEject(c Cohort) bool {
	if c.TotalEnabled <= 0 {
		return false
	}
	if c.TotalEnabled-c.Ejected-1 <= 0 && !c.FallbackAvailable {
		return false
	}
	return (c.Ejected+1)*100 <= c.TotalEnabled*e.Config.MaxEjectedPercent
}

func (e Engine) eject(s *adaptivev1alpha1.NodeQualityStatus, now time.Time) {
	s.EjectionCount++
	duration := time.Duration(s.EjectionCount) * e.Config.BaseEjectionTime
	if duration > e.Config.MaxEjectionTime {
		duration = e.Config.MaxEjectionTime
	}
	until := metav1.NewTime(now.Add(duration))
	s.EjectedUntil = &until
	setState(s, StateEjected, "outlier threshold reached", now)
	s.EffectiveWeight, s.QualityScore, s.RecoveryStep = 0, 0, 0
}

func (e Engine) advanceRecovery(s *adaptivev1alpha1.NodeQualityStatus, healthyWindow bool, now time.Time) {
	if !healthyWindow {
		s.Reason = "recovery waiting for healthy samples"
		s.RecoveryStep = 0
		t := metav1.NewTime(now)
		s.StateSince = &t
		return
	}
	if s.StateSince == nil {
		s.Reason = "recovery timer is not initialized"
		return
	}
	age := now.Sub(s.StateSince.Time)
	switch {
	case age < e.Config.RecoverySteps[0]:
		s.RecoveryStep = 0
		s.Reason = "progressive recovery at 10% state factor"
	case age < e.Config.RecoverySteps[1]:
		s.RecoveryStep = 1
		s.Reason = "progressive recovery at 25% state factor"
	case age < e.Config.RecoverySteps[2]:
		s.RecoveryStep = 2
		s.Reason = "progressive recovery at 50% state factor"
	default:
		s.RecoveryStep = 3
		setState(s, StateHealthy, "progressive recovery completed", now)
		s.EjectedUntil = nil
	}
}

func (e Engine) computeWeight(spec adaptivev1alpha1.NodeQualitySpec, sample NodeSample, s *adaptivev1alpha1.NodeQualityStatus, publishBoundary bool) {
	stateFactor := map[string]float64{StateHealthy: 1, StateDegraded: 0.3, StateStale: 0.2, StateRecovering: []float64{0.1, 0.25, 0.5, 1}[clampInt(int(s.RecoveryStep), 0, 3)]}[s.State]
	latencyFactor := 1.0
	if s.LatencyEWMAMillis > 0 && sample.BaselineLatency > 0 {
		latencyFactor = clamp((float64(sample.BaselineLatency)/float64(time.Millisecond))/s.LatencyEWMAMillis, 0.1, 1)
	}
	reliability := clamp(1-s.ErrorEWMA, 0, 1)
	headroom := 0.05
	if spec.StaticCapacity > 0 {
		headroom = clamp(float64(spec.StaticCapacity-int32(sample.ActiveRequests))/float64(spec.StaticCapacity), 0.05, 1)
	}
	s.QualityScore = clamp(stateFactor*latencyFactor*reliability*headroom, 0, 1)
	target := int32(math.Round(float64(spec.StaticCapacity) * s.QualityScore))
	if target > 100 {
		target = 100
	}
	forceZero := s.State == StateEjected || s.State == StateDisabled || s.State == StateUnknown
	minimumDelta := e.Config.MinimumWeightDelta
	if publishBoundary {
		minimumDelta = 0
	}
	s.EffectiveWeight = quantizeWeight(s.EffectiveWeight, target, minimumDelta, forceZero)
}

func (e Engine) disabled(s adaptivev1alpha1.NodeQualityStatus, now time.Time) adaptivev1alpha1.NodeQualityStatus {
	setState(&s, StateDisabled, "spec.enabled is false", now)
	s.EffectiveWeight, s.QualityScore = 0, 0
	return s
}
func setState(s *adaptivev1alpha1.NodeQualityStatus, state, reason string, now time.Time) {
	if s.State != state || s.StateSince == nil {
		t := metav1.NewTime(now)
		s.StateSince = &t
	}
	s.State = state
	s.Reason = reason
}
func quantizeWeight(old, target, delta int32, forceZero bool) int32 {
	if forceZero {
		return 0
	}
	if target < 0 {
		target = 0
	}
	if target > 100 {
		target = 100
	}
	if old == 0 && target > 0 {
		return target
	}
	if abs32(target-old) < delta {
		return old
	}
	return target
}
func abs32(v int32) int32 {
	if v < 0 {
		return -v
	}
	return v
}
func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

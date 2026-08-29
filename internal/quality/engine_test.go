package quality

import (
	"math"
	"testing"
	"time"

	adaptivev1alpha1 "github.com/EdgeCDN-X/edgecdnx-plugin/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestUpdateEWMA(t *testing.T) {
	tests := []struct {
		name                    string
		previous, sample, alpha float64
		initialized             bool
		want                    float64
		wantErr                 bool
	}{
		{"first sample", 0, 100, .2, false, 100, false},
		{"stable", 100, 100, .2, true, 100, false},
		{"single spike", 100, 500, .2, true, 180, false},
		{"alpha zero", 100, 500, 0, true, 100, false},
		{"alpha one", 100, 500, 1, true, 500, false},
		{"negative", 100, -1, .2, true, 0, true},
		{"nan", 100, math.NaN(), .2, true, 0, true},
		{"infinite", 100, math.Inf(1), .2, true, 0, true},
		{"bad alpha", 100, 100, 1.1, true, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UpdateEWMA(tt.previous, tt.sample, tt.alpha, tt.initialized)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v", err)
			}
			if !tt.wantErr && math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}

func TestEWMASustainedDegradationAndRecovery(t *testing.T) {
	value := 100.0
	for i := 0; i < 5; i++ {
		value, _ = UpdateEWMA(value, 300, .2, true)
	}
	if value <= 200 {
		t.Fatalf("sustained degradation not reflected: %v", value)
	}
	degraded := value
	for i := 0; i < 10; i++ {
		value, _ = UpdateEWMA(value, 100, .2, true)
	}
	if value >= degraded || value > 120 {
		t.Fatalf("recovery not reflected: %v", value)
	}
}

func TestConsecutiveFailureEjection(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	status := healthyStatus(now)
	status.ConsecutiveFailures = 3
	sample := lowVolumeFailure(now)
	status, err := engine.Evaluate(spec(), status, sample, Cohort{TotalEnabled: 2}, now)
	if err != nil {
		t.Fatal(err)
	}
	if status.State == StateEjected || status.ConsecutiveFailures != 4 {
		t.Fatalf("fourth failure state=%s count=%d", status.State, status.ConsecutiveFailures)
	}
	status, err = engine.Evaluate(spec(), status, sample, Cohort{TotalEnabled: 2}, now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateEjected || status.EffectiveWeight != 0 {
		t.Fatalf("fifth failure did not eject: %+v", status)
	}
}

func TestSuccessResetsConsecutiveFailures(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	status := healthyStatus(now)
	status.ConsecutiveFailures = 4
	got, err := engine.Evaluate(spec(), status, healthySample(now), Cohort{TotalEnabled: 2}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConsecutiveFailures != 0 {
		t.Fatalf("count=%d", got.ConsecutiveFailures)
	}
}

func TestLowVolumeErrorRateDoesNotEject(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	sample := lowVolumeFailure(now)
	sample.RequestCount = 2
	sample.ErrorCount = 1
	got, err := engine.Evaluate(spec(), healthyStatus(now), sample, Cohort{TotalEnabled: 2}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.State == StateEjected {
		t.Fatal("low-volume error rate caused ejection")
	}
}

func TestMaximumEjectionPercentage(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	status := healthyStatus(now)
	status.ConsecutiveFailures = 4
	got, err := engine.Evaluate(spec(), status, lowVolumeFailure(now), Cohort{TotalEnabled: 2, Ejected: 1, FallbackAvailable: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateDegraded || got.EffectiveWeight == 0 {
		t.Fatalf("expected degraded overflow, got %+v", got)
	}
}

func TestEjectionBackoffAndCap(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	for _, tt := range []struct {
		name  string
		count int32
		want  time.Duration
	}{{"second", 1, time.Minute}, {"capped", 20, 5 * time.Minute}} {
		t.Run(tt.name, func(t *testing.T) {
			status := healthyStatus(now)
			status.EjectionCount = tt.count
			status.ConsecutiveFailures = 4
			got, err := engine.Evaluate(spec(), status, lowVolumeFailure(now), Cohort{TotalEnabled: 2}, now)
			if err != nil {
				t.Fatal(err)
			}
			if d := got.EjectedUntil.Sub(now); d != tt.want {
				t.Fatalf("duration=%s want %s", d, tt.want)
			}
		})
	}
}

func TestRestartPreservesEjection(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	clock := &FakeClock{Current: time.Now()}
	until := metav1.NewTime(clock.Now().Add(time.Minute))
	status := healthyStatus(clock.Now())
	status.State = StateEjected
	status.EjectedUntil = &until
	status.EffectiveWeight = 0
	got, err := engine.Evaluate(spec(), status, healthySample(clock.Now()), Cohort{TotalEnabled: 2, Ejected: 1}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateEjected || !got.EjectedUntil.Equal(&until) {
		t.Fatalf("ejection not restored: %+v", got)
	}
}

func TestProgressiveRecoveryAndReejection(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	clock := &FakeClock{Current: time.Now()}
	past := metav1.NewTime(clock.Now().Add(-time.Second))
	status := healthyStatus(clock.Now())
	status.State = StateEjected
	status.EjectedUntil = &past
	status, err := engine.Evaluate(spec(), status, healthySample(clock.Now()), Cohort{TotalEnabled: 2, Ejected: 1}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateRecovering || status.RecoveryStep != 0 {
		t.Fatalf("got %+v", status)
	}
	clock.Advance(35 * time.Second)
	status, err = engine.Evaluate(spec(), status, healthySample(clock.Now()), Cohort{TotalEnabled: 2}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if status.RecoveryStep != 1 {
		t.Fatalf("step=%d", status.RecoveryStep)
	}
	status.ConsecutiveFailures = 4
	status, err = engine.Evaluate(spec(), status, lowVolumeFailure(clock.Now()), Cohort{TotalEnabled: 2}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateEjected {
		t.Fatalf("recovery failure did not re-eject: %s", status.State)
	}
}

func TestRecoveryRequiresLatencyAndSampleHealth(t *testing.T) {
	config := LabDefaults()
	config.LatencyDegradedFactor = 2
	engine := Engine{Config: config}
	clock := &FakeClock{Current: time.Now()}
	past := metav1.NewTime(clock.Now().Add(-time.Second))
	status := healthyStatus(clock.Now())
	status.State = StateEjected
	status.EjectedUntil = &past
	status, err := engine.Evaluate(spec(), status, healthySample(clock.Now()), Cohort{TotalEnabled: 2, Ejected: 1}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(35 * time.Second)
	badLatency := healthySample(clock.Now())
	badLatency.P95Latency = time.Second
	status, err = engine.Evaluate(spec(), status, badLatency, Cohort{TotalEnabled: 2}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if status.RecoveryStep != 0 || status.ConsecutiveHealthy != 0 {
		t.Fatalf("unhealthy latency advanced recovery: %+v", status)
	}
	clock.Advance(35 * time.Second)
	status, err = engine.Evaluate(spec(), status, healthySample(clock.Now()), Cohort{TotalEnabled: 2}, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if status.RecoveryStep != 0 {
		t.Fatalf("unhealthy time was counted toward recovery: %+v", status)
	}
}

func TestSingleFailureDuringRecoveryReejects(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	status := healthyStatus(now)
	status.State = StateRecovering
	status.RecoveryStep = 1
	got, err := engine.Evaluate(spec(), status, lowVolumeFailure(now), Cohort{TotalEnabled: 2}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateEjected || got.EjectionCount != 1 {
		t.Fatalf("recovery failure was not immediately re-ejected: %+v", got)
	}
}

func TestDegradedNeedsConsecutiveHealthyWindows(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	status := healthyStatus(now)
	status.State = StateDegraded
	status.ConsecutiveHealthy = 0
	for i := int32(1); i <= engine.Config.HealthySamplesToRecover; i++ {
		var err error
		status, err = engine.Evaluate(spec(), status, healthySample(now.Add(time.Duration(i)*time.Second)), Cohort{TotalEnabled: 2}, now.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if i < engine.Config.HealthySamplesToRecover && status.State != StateDegraded {
			t.Fatalf("became healthy after only %d windows", i)
		}
	}
	if status.State != StateHealthy {
		t.Fatalf("did not recover after required windows: %+v", status)
	}
}

func TestStaleAndDisabled(t *testing.T) {
	config := LabDefaults()
	engine := Engine{Config: config}
	now := time.Now()
	status := healthyStatus(now.Add(-time.Minute))
	status.EffectiveWeight = 80
	stale := engine.MarkMetricsUnavailable(spec(), status, Cohort{TotalEnabled: 2}, now)
	if stale.State != StateStale || stale.EffectiveWeight == 0 {
		t.Fatalf("bad stale status: %+v", stale)
	}
	disabledSpec := spec()
	disabledSpec.Enabled = false
	disabled, err := engine.Evaluate(disabledSpec, status, NodeSample{Timestamp: now}, Cohort{}, now)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.State != StateDisabled || disabled.EffectiveWeight != 0 {
		t.Fatalf("bad disabled: %+v", disabled)
	}
}

func TestConfigValidationAndRecoveryStepParsing(t *testing.T) {
	config := LabDefaults()
	if err := config.Validate(); err != nil {
		t.Fatalf("lab defaults invalid: %v", err)
	}
	steps, err := ParseRecoverySteps("10s, 20s,1m")
	if err != nil || len(steps) != 3 || steps[2] != time.Minute {
		t.Fatalf("steps=%v err=%v", steps, err)
	}
	config.RecoverySteps = []time.Duration{30 * time.Second, 20 * time.Second, time.Minute}
	if err := config.Validate(); err == nil {
		t.Fatal("non-increasing recovery steps accepted")
	}
	if _, err := ParseRecoverySteps("30s,bad,2m"); err == nil {
		t.Fatal("invalid recovery duration accepted")
	}
}

func TestHardStaleHonorsLastNodeSafety(t *testing.T) {
	config := LabDefaults()
	engine := Engine{Config: config}
	now := time.Now()
	status := healthyStatus(now.Add(-config.HardStaleAfter - time.Second))
	status.EffectiveWeight = 80
	protected := engine.MarkMetricsUnavailable(spec(), status, Cohort{TotalEnabled: 1}, now)
	if protected.State != StateStale || protected.EffectiveWeight == 0 {
		t.Fatalf("last node should retain degraded last-known-good weight: %+v", protected)
	}
	removed := engine.MarkMetricsUnavailable(spec(), status, Cohort{TotalEnabled: 2, FallbackAvailable: true}, now)
	if removed.State != StateStale || removed.EffectiveWeight != 0 {
		t.Fatalf("hard-stale node should be removed when safety permits: %+v", removed)
	}
}

func TestEjectedNodeNeverRecoversWithoutFreshMetrics(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	status := healthyStatus(now.Add(-time.Hour))
	status.State = StateEjected
	status.EffectiveWeight = 0
	past := metav1.NewTime(now.Add(-time.Minute))
	status.EjectedUntil = &past
	got := engine.MarkMetricsUnavailable(spec(), status, Cohort{TotalEnabled: 2, FallbackAvailable: true}, now)
	if got.State != StateEjected || got.EffectiveWeight != 0 || got.Reason != "ejection cooldown elapsed; waiting for fresh metrics" {
		t.Fatalf("ejected node recovered without fresh metrics: %+v", got)
	}
}

func TestMinimumWeightDeltaDoesNotBlockZeroToPositiveTransition(t *testing.T) {
	if got := quantizeWeight(0, 1, 5, false); got != 1 {
		t.Fatalf("zero-to-positive transition was suppressed: %d", got)
	}
	if got := quantizeWeight(50, 53, 5, false); got != 50 {
		t.Fatalf("steady-state jitter was not suppressed: %d", got)
	}
}

func TestStaleInterruptedRecoveryNeverRaisesWeightOrSkipsRamp(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	status := healthyStatus(now.Add(-time.Minute))
	status.State = StateRecovering
	status.EffectiveWeight = 1
	past := metav1.NewTime(now.Add(-time.Minute))
	status.EjectedUntil = &past
	stale := engine.MarkMetricsUnavailable(spec(), status, Cohort{TotalEnabled: 2, FallbackAvailable: true}, now)
	if stale.State != StateStale || stale.EffectiveWeight != 1 {
		t.Fatalf("stale recovery raised or removed last-known-good weight: %+v", stale)
	}
	resumed, err := engine.Evaluate(spec(), stale, healthySample(now.Add(time.Second)), Cohort{TotalEnabled: 2, FallbackAvailable: true}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != StateRecovering || resumed.RecoveryStep != 0 {
		t.Fatalf("interrupted recovery skipped ramp: %+v", resumed)
	}
	completedAt := now.Add(time.Second + engine.Config.RecoverySteps[2] + time.Second)
	completed, err := engine.Evaluate(spec(), resumed, healthySample(completedAt), Cohort{TotalEnabled: 2, FallbackAvailable: true}, completedAt)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != StateHealthy || completed.EjectedUntil != nil {
		t.Fatalf("recovery transaction was not completed: %+v", completed)
	}
}

func TestRecoveryPublishesEveryWeightStep(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	clock := &FakeClock{Current: time.Now()}
	past := metav1.NewTime(clock.Now().Add(-time.Second))
	status := healthyStatus(clock.Now())
	status.State = StateEjected
	status.EjectedUntil = &past
	status.EffectiveWeight = 0
	wants := []struct {
		advance time.Duration
		state   string
		step    int32
		reason  string
	}{
		{0, StateRecovering, 0, "progressive recovery at 10% state factor"},
		{35 * time.Second, StateRecovering, 1, "progressive recovery at 25% state factor"},
		{35 * time.Second, StateRecovering, 2, "progressive recovery at 50% state factor"},
		{60 * time.Second, StateHealthy, 3, "progressive recovery completed"},
	}
	previousWeight := int32(-1)
	for _, want := range wants {
		clock.Advance(want.advance)
		var err error
		status, err = engine.Evaluate(spec(), status, healthySample(clock.Now()), Cohort{TotalEnabled: 2, FallbackAvailable: true}, clock.Now())
		if err != nil {
			t.Fatal(err)
		}
		if status.State != want.state || status.RecoveryStep != want.step || status.Reason != want.reason {
			t.Fatalf("state=%s step=%d reason=%q", status.State, status.RecoveryStep, status.Reason)
		}
		if status.EffectiveWeight <= previousWeight {
			t.Fatalf("weight did not increase at step %d: previous=%d current=%d", want.step, previousWeight, status.EffectiveWeight)
		}
		previousWeight = status.EffectiveWeight
	}
}

func TestInvalidSamplesRejected(t *testing.T) {
	engine := Engine{Config: LabDefaults()}
	now := time.Now()
	bad := healthySample(now)
	bad.CacheHitRatio = math.NaN()
	if _, err := engine.Evaluate(spec(), adaptivev1alpha1.NodeQualityStatus{}, bad, Cohort{}, now); err == nil {
		t.Fatal("NaN accepted")
	}
	bad = healthySample(now)
	bad.P95Latency = -time.Millisecond
	if _, err := engine.Evaluate(spec(), adaptivev1alpha1.NodeQualityStatus{}, bad, Cohort{}, now); err == nil {
		t.Fatal("negative latency accepted")
	}
}

func spec() adaptivev1alpha1.NodeQualitySpec {
	return adaptivev1alpha1.NodeQualitySpec{NodeName: "edge-syd-a", Location: "sydney", StaticCapacity: 100, Enabled: true}
}
func healthySample(now time.Time) NodeSample {
	return NodeSample{Timestamp: now, RequestCount: 100, P95Latency: 100 * time.Millisecond, BaselineLatency: 50 * time.Millisecond, CacheHitRatio: .8, ActiveRequests: 10}
}
func lowVolumeFailure(now time.Time) NodeSample {
	return NodeSample{Timestamp: now, RequestCount: 10, ErrorCount: 1, P95Latency: 100 * time.Millisecond, BaselineLatency: 50 * time.Millisecond, CacheHitRatio: .5, FailureKind: "origin_5xx"}
}
func healthyStatus(at time.Time) adaptivev1alpha1.NodeQualityStatus {
	observed := metav1.NewTime(at)
	since := metav1.NewTime(at)
	return adaptivev1alpha1.NodeQualityStatus{ObservedAt: &observed, State: StateHealthy, StateSince: &since, EffectiveWeight: 80, LatencyEWMAMillis: 100}
}

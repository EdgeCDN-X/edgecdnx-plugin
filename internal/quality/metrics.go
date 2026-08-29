package quality

import (
	"context"
	"time"
)

type NodeSample struct {
	Timestamp            time.Time
	RequestCount         uint64
	ErrorCount           uint64
	P95Latency           time.Duration
	BaselineLatency      time.Duration
	CacheHitRatio        float64
	ActiveRequests       int
	CPUUtilisation       float64
	BandwidthUtilisation float64
	ProbeFailed          bool
	FailureKind          string
}

type MetricsProvider interface {
	QueryNode(context.Context, string, time.Time) (NodeSample, error)
}

type FakeMetricsProvider struct {
	Sample NodeSample
	Err    error
}

func (f *FakeMetricsProvider) QueryNode(context.Context, string, time.Time) (NodeSample, error) {
	return f.Sample, f.Err
}

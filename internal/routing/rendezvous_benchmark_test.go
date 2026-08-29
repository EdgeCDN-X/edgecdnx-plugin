package routing

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkWeightedRendezvous(b *testing.B) {
	for _, size := range []int{8, 64, 512} {
		candidates := make([]Candidate, size)
		for i := range candidates {
			candidates[i] = Candidate{ID: fmt.Sprintf("node-%04d", i), Weight: float64(i%100 + 1)}
		}
		b.Run(fmt.Sprintf("nodes-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				SelectWeightedRendezvous("192.0.2.0/24|video.example.|1", candidates)
			}
		})
		b.Run(fmt.Sprintf("parallel-nodes-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					SelectWeightedRendezvous("192.0.2.0/24|video.example.|1", candidates)
				}
			})
		})
	}
}

func BenchmarkSnapshotUpdate512(b *testing.B) {
	entries := make([]NodeQuality, 512)
	for i := range entries {
		entries[i] = NodeQuality{Location: "sydney", Node: fmt.Sprintf("node-%04d", i), EffectiveWeight: 100, State: "Healthy"}
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = NewSnapshot(uint64(i), time.Unix(int64(i), 0), entries)
	}
}

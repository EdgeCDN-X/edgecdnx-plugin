package routing

import (
	"math"

	"github.com/cespare/xxhash/v2"
)

type Candidate struct {
	ID     string
	Weight float64
}

// SelectWeightedRendezvous implements exponential-rank weighted HRW. The
// candidate with the smallest -ln(u)/weight rank wins.
func SelectWeightedRendezvous(key string, candidates []Candidate) (string, bool) {
	selected := ""
	bestRank := math.Inf(1)
	for _, candidate := range candidates {
		if candidate.ID == "" || candidate.Weight <= 0 || math.IsNaN(candidate.Weight) || math.IsInf(candidate.Weight, 0) {
			continue
		}
		u := hashToUnitInterval(key, candidate.ID)
		rank := -math.Log(u) / candidate.Weight
		if rank < bestRank || (rank == bestRank && candidate.ID < selected) {
			selected, bestRank = candidate.ID, rank
		}
	}
	return selected, selected != ""
}

func hashToUnitInterval(key, candidate string) float64 {
	digest := xxhash.New()
	_, _ = digest.WriteString(key)
	_, _ = digest.WriteString("\x00")
	_, _ = digest.WriteString(candidate)
	return (float64(digest.Sum64()) + 1) / (float64(math.MaxUint64) + 1)
}

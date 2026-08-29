package routing

import "time"

type NodeQuality struct {
	Location        string
	Node            string
	EffectiveWeight int32
	State           string
	ObservedAt      time.Time
}

type Snapshot struct {
	Version   uint64
	UpdatedAt time.Time
	nodes     map[string]NodeQuality
}

func NewSnapshot(version uint64, updatedAt time.Time, entries []NodeQuality) *Snapshot {
	nodes := make(map[string]NodeQuality, len(entries))
	for _, entry := range entries {
		if entry.Location == "" || entry.Node == "" {
			continue
		}
		nodes[snapshotKey(entry.Location, entry.Node)] = entry
	}
	return &Snapshot{Version: version, UpdatedAt: updatedAt, nodes: nodes}
}

func EmptySnapshot() *Snapshot { return NewSnapshot(0, time.Time{}, nil) }

func (s *Snapshot) Lookup(location, node string) (NodeQuality, bool) {
	if s == nil {
		return NodeQuality{}, false
	}
	quality, ok := s.nodes[snapshotKey(location, node)]
	return quality, ok
}

func (s *Snapshot) Len() int {
	if s == nil {
		return 0
	}
	return len(s.nodes)
}

func snapshotKey(location, node string) string { return location + "\x00" + node }

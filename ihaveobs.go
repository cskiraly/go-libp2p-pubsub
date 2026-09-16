package pubsub

import (
	"fmt"
	"sync/atomic"

	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Announce-rate observation (measurement only) ---
//
// MaxIHaveMessages is compared against peerhave[p]: how many RPCs carrying *any* control message
// arrived from p within one heartbeat (HandleRPC returns early only when an RPC has no control at
// all, so a data push with piggybacked control counts too). Phase forwarding emits one RPC per
// (message, peer) for its announcements, so that count scales with the group size while the limit
// was sized for heartbeat gossip's single RPC per peer per heartbeat. Nothing recorded what the
// count actually is. This records it, per (peer, heartbeat) pair, and separately how many of those
// RPCs carried an IHAVE at all, which is the difference between an announcement problem and a
// control-traffic problem. It decides nothing.

// ihaveBuckets is the histogram width; the last bucket is "that many or more".
const ihaveBuckets = 256

// IHaveRPCStats is a histogram over (peer, heartbeat) pairs. Safe for concurrent use, and shared
// by every node of a cell so one snapshot describes the whole population.
type IHaveRPCStats struct {
	all    [ihaveBuckets]atomic.Int64 // RPCs bearing any control, which is what the limit counts
	ihave  [ihaveBuckets]atomic.Int64 // of those, the ones that actually carried an IHAVE
	pairs  atomic.Int64
	rpcs   atomic.Int64
	ihaves atomic.Int64
	maxAll atomic.Int64
	maxIH  atomic.Int64
}

// Reset drops everything recorded so far. The harness calls it at publish, so the histogram
// describes the measured diffusion rather than the mesh-settle heartbeats before it.
func (s *IHaveRPCStats) Reset() {
	if s == nil {
		return
	}
	for i := range s.all {
		s.all[i].Store(0)
		s.ihave[i].Store(0)
	}
	s.pairs.Store(0)
	s.rpcs.Store(0)
	s.ihaves.Store(0)
	s.maxAll.Store(0)
	s.maxIH.Store(0)
}

func (s *IHaveRPCStats) observe(all, ihave int) {
	if s == nil || all <= 0 {
		return
	}
	s.pairs.Add(1)
	s.rpcs.Add(int64(all))
	s.ihaves.Add(int64(ihave))
	s.all[min(all, ihaveBuckets-1)].Add(1)
	s.ihave[min(ihave, ihaveBuckets-1)].Add(1)
	for cur := s.maxAll.Load(); int64(all) > cur && !s.maxAll.CompareAndSwap(cur, int64(all)); cur = s.maxAll.Load() {
	}
	for cur := s.maxIH.Load(); int64(ihave) > cur && !s.maxIH.CompareAndSwap(cur, int64(ihave)); cur = s.maxIH.Load() {
	}
}

// Quantile returns the q-th quantile of the per-(peer, heartbeat) RPC count; ihaveOnly restricts it
// to RPCs that carried an IHAVE.
func (s *IHaveRPCStats) Quantile(q float64, ihaveOnly bool) int {
	b := &s.all
	if ihaveOnly {
		b = &s.ihave
	}
	total := int64(0)
	for i := range b {
		total += b[i].Load()
	}
	if total == 0 {
		return 0
	}
	want, seen := int64(float64(total)*q), int64(0)
	for i := range b {
		seen += b[i].Load()
		if seen > want {
			return i
		}
	}
	return ihaveBuckets - 1
}

// Over returns the share of (peer, heartbeat) pairs whose count exceeded n, which is the share the
// limit would have silently truncated at MaxIHaveMessages = n.
func (s *IHaveRPCStats) Over(n int, ihaveOnly bool) float64 {
	b := &s.all
	if ihaveOnly {
		b = &s.ihave
	}
	total, over := int64(0), int64(0)
	for i := range b {
		c := b[i].Load()
		total += c
		if i > n {
			over += c
		}
	}
	if total == 0 {
		return 0
	}
	return float64(over) / float64(total)
}

// Line renders the histogram for a cell's report.
func (s *IHaveRPCStats) Line() string {
	if s == nil || s.pairs.Load() == 0 {
		return "announce rate: no (peer, heartbeat) pairs observed"
	}
	return fmt.Sprintf("announce rate: %d (peer, heartbeat) pairs, %d control RPCs (%d carrying IHAVE); "+
		"control RPCs per peer per heartbeat p50 %d p90 %d p99 %d max %d, over 10: %.1f%%; "+
		"IHAVE-bearing p50 %d p90 %d p99 %d max %d, over 10: %.1f%%",
		s.pairs.Load(), s.rpcs.Load(), s.ihaves.Load(),
		s.Quantile(0.5, false), s.Quantile(0.9, false), s.Quantile(0.99, false), s.maxAll.Load(), 100*s.Over(10, false),
		s.Quantile(0.5, true), s.Quantile(0.9, true), s.Quantile(0.99, true), s.maxIH.Load(), 100*s.Over(10, true))
}

// WithIHaveRPCObservation records the announce rate into stats. It records; it never decides, and
// the limit itself is untouched.
func WithIHaveRPCObservation(stats *IHaveRPCStats) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("announce-rate observation requires a gossipsub router")
		}
		if stats == nil {
			return fmt.Errorf("announce-rate observation requires a non-nil histogram")
		}
		gs.ihaveObs = stats
		gs.peerhaveIHave = make(map[peer.ID]int)
		return nil
	}
}

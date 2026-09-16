package pubsub

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// --- The tail hedge's bounds and schedule (hedge-and-adaptivity plan, E2) ---
//
// The tail hedge as first built had a fan-out and nothing else: an id's outstanding count reset
// on move-on, so nothing bounded the asks over an id's life; nothing bounded what a group could
// spend or what one serving peer could be asked; and the flag that entered the tail was never
// cleared. These options give it a lifetime cap per id, a cap per group, a cap per serving
// peer, a fan-out that follows the pieces still missing, and an exit. Without them the hedge
// behaves exactly as before.

// WithIWantTailHedgeBounds bounds the tail hedge: perID caps the asks an id may ever receive
// beyond its first (every ask counts, retries included: the plan's lifetime bound); perGroup
// caps the extra asks the hedge issues in this group; perPeer caps the extra asks directed at
// one serving peer in this group. Zero leaves a bound off. The counters, when non-nil, record
// the asks each bound refused. Requires WithIWantTailHedge first.
func WithIWantTailHedgeBounds(perID, perGroup, perPeer int, refusedID, refusedGroup, refusedPeer *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.tailK == 0 {
			return fmt.Errorf("tail hedge bounds require WithIWantTailHedge first")
		}
		if perID < 0 || perGroup < 0 || perPeer < 0 {
			return fmt.Errorf("tail hedge bounds must be non-negative")
		}
		gs.tailBoundID, gs.tailBoundGroup, gs.tailBoundPeer = perID, perGroup, perPeer
		gs.tailRefusedID, gs.tailRefusedGroup, gs.tailRefusedPeer = refusedID, refusedGroup, refusedPeer
		return nil
	}
}

// WithIWantTailSchedule makes the fan-out follow the pieces still missing: k(m) = 1 + ⌈h/m⌉,
// capped at the tail hedge's k, where h is the tail window and m the count the application
// reports through TailProgress. Every outstanding id is topped up to the new fan-out when it
// rises. The counter, when non-nil, records the asks the top-ups issued. Requires
// WithIWantTailHedge first.
func WithIWantTailSchedule(h int, topUps *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || gs.tailK == 0 {
			return fmt.Errorf("tail hedge schedule requires WithIWantTailHedge first")
		}
		if h < 1 {
			return fmt.Errorf("tail hedge schedule needs a window of at least 1")
		}
		gs.tailSchedule = true
		gs.tailH = h
		gs.tailTopUps = topUps
		return nil
	}
}

// TailProgress reports how many pieces the node still needs: the schedule's m, and the number of
// outstanding ids a top-up may hedge (a code's shards are interchangeable, so hedging more ids
// than the deficit buys duplicates for nothing). Safe from any goroutine.
func (p *PubSub) TailProgress(missing int) {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.tailK == 0 {
		return
	}
	select {
	case p.eval <- func() {
		if !gs.tailOn {
			return
		}
		before := gs.tailKNow()
		gs.tailMissing = missing
		if gs.tailKNow() > before || missing > 0 {
			gs.tailTopUp(gs.tailTopUps)
		}
	}:
	case <-p.ctx.Done():
	}
}

// ExitTailHedge clears the tail: the node has completed the group, and nothing it asks for from
// now on is the group's. Safe from any goroutine.
func (p *PubSub) ExitTailHedge() {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.tailK == 0 {
		return
	}
	select {
	case p.eval <- func() {
		gs.tailOn = false
		gs.tailMissing = 0
	}:
	case <-p.ctx.Done():
	}
}

// tailKNow is the effective fan-out: the configured k, or the schedule's k(m) capped at it.
func (gs *GossipSubRouter) tailKNow() int {
	if !gs.tailSchedule {
		return gs.tailK
	}
	m := gs.tailMissing
	if m < 1 {
		m = gs.tailH
	}
	k := 1 + (gs.tailH+m-1)/m
	if k > gs.tailK {
		k = gs.tailK
	}
	return k
}

// tailExtraAllowed applies the bounds to one extra ask of mid to the peer and, if allowed,
// charges them. Router goroutine.
func (gs *GossipSubRouter) tailExtraAllowed(mid string, to peer.ID) bool {
	if gs.tailBoundID > 0 && gs.tailAsks[mid] >= 1+gs.tailBoundID {
		if gs.tailRefusedID != nil {
			gs.tailRefusedID.Add(1)
		}
		return false
	}
	if gs.tailBoundGroup > 0 && gs.tailExtraUsed >= gs.tailBoundGroup {
		if gs.tailRefusedGroup != nil {
			gs.tailRefusedGroup.Add(1)
		}
		return false
	}
	if gs.tailBoundPeer > 0 && gs.tailPeerExtra[to] >= gs.tailBoundPeer {
		if gs.tailRefusedPeer != nil {
			gs.tailRefusedPeer.Add(1)
		}
		return false
	}
	gs.tailExtraUsed++
	gs.tailPeerExtra[to]++
	return true
}

// tailTopUp hedges the ids in flight up to the effective fan-out, within the bounds, oldest asks
// first in a deterministic order; when the deficit is known, only as many ids as it needs. The
// counter, when non-nil, records the asks issued here.
func (gs *GossipSubRouter) tailTopUp(ctr *atomic.Int64) {
	if gs.iwantAsked == nil || gs.offerTable == nil {
		return
	}
	type live struct {
		mid string
		at  time.Time
	}
	ids := make([]live, 0, len(gs.iwantAsked))
	for mid, at := range gs.iwantAsked {
		if time.Since(at) >= gs.iwantWindow || gs.p.seenMessage(mid) {
			continue
		}
		ids = append(ids, live{mid, at})
	}
	sort.Slice(ids, func(i, j int) bool {
		if ids[i].at.Equal(ids[j].at) {
			return ids[i].mid < ids[j].mid
		}
		return ids[i].at.Before(ids[j].at)
	})
	if gs.tailMissing > 0 && gs.tailMissing < len(ids) {
		ids = ids[:gs.tailMissing]
	}
	for _, e := range ids {
		n := gs.tailHedgeMid(e.mid)
		if ctr != nil && n > 0 {
			ctr.Add(int64(n))
		}
	}
}

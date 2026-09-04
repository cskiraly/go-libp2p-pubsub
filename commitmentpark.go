package pubsub

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// Commitment park: an IHAVE is a binding offer.
//
// Under the IWANT discipline a receiver asks one announcer per id and moves on only when the
// window expires, so an announcer that never serves costs the receiver a window per id. The
// park makes the offer binding: each ask opens a per-(id, peer) promise with its own deadline;
// a promise still unserved at the deadline is a break, and a peer reaching k breaks is parked,
// its announcements ineligible for pull selection, for ttl. Delivery of the id from anyone
// keeps every open promise for it.
//
// The promise deadline is independent of the discipline window (two timers): a short window
// moves on and re-asks elsewhere while the slow peer still runs to judgment. Park gates pull
// selection only; pushes, scoring and holder evidence are untouched.
type commitmentPark struct {
	k       int           // broken promises before a peer is parked
	ttl     time.Duration // how long a parked peer's announcements are ineligible
	promise time.Duration // deadline for serving an ask

	promised map[string]map[peer.ID]time.Time // open promises per id: every peer asked, and when
	breaks   map[peer.ID]int                  // broken-promise counts
	parked   map[peer.ID]time.Time            // parked peers by expiry
}

// WithIHaveCommitmentPark treats an IHAVE as a binding offer under the IWANT discipline: an ask
// unserved after promise is a break, and a peer reaching k breaks is parked for ttl. Requires
// WithIWantDiscipline first.
func WithIHaveCommitmentPark(k int, promise, ttl time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("commitment park requires a gossipsub router")
		}
		if gs.iwantAsked == nil {
			return fmt.Errorf("commitment park requires WithIWantDiscipline first")
		}
		if k < 1 || ttl <= 0 || promise <= 0 {
			return fmt.Errorf("commitment park needs k >= 1, a positive ttl, and a positive promise")
		}
		gs.park = &commitmentPark{
			k:        k,
			ttl:      ttl,
			promise:  promise,
			promised: make(map[string]map[peer.ID]time.Time),
			breaks:   make(map[peer.ID]int),
			parked:   make(map[peer.ID]time.Time),
		}
		return nil
	}
}

// notePromise opens a promise for an ask just granted and schedules its judgment on the
// router goroutine.
func (gs *GossipSubRouter) notePromise(mid string, from peer.ID, askedAt time.Time) {
	pk := gs.park
	if pk == nil {
		return
	}
	if pk.promised[mid] == nil {
		pk.promised[mid] = make(map[peer.ID]time.Time)
	}
	pk.promised[mid][from] = askedAt
	time.AfterFunc(pk.promise, func() {
		select {
		case gs.p.eval <- func() { gs.promiseDue(mid, from, askedAt) }:
		case <-gs.p.ctx.Done():
		}
	})
}

// promiseDue runs on the router goroutine at this peer's promise deadline. A promise already
// kept (delivery of the id, from anyone) or re-made (a newer ask to the same peer) is not
// charged; an open one breaks: the asked peer is charged and possibly parked. The discipline
// slot is refunded only when this ask is still the current one; a shorter window has usually
// moved on already, and then there is nothing left to refund.
func (gs *GossipSubRouter) promiseDue(mid string, from peer.ID, askedAt time.Time) {
	pk := gs.park
	if pk == nil {
		return
	}
	if at, open := pk.promised[mid][from]; !open || !at.Equal(askedAt) {
		return
	}
	gs.notePromiseBreak(mid, from)
	if at, ok := gs.iwantAsked[mid]; ok && at.Equal(askedAt) {
		delete(gs.iwantAsked, mid)
	}
}

// notePromiseBreak charges peer p for an unserved ask of mid, parking it once its breaks
// reach k.
func (gs *GossipSubRouter) notePromiseBreak(mid string, p peer.ID) {
	pk := gs.park
	if pk == nil {
		return
	}
	if peers := pk.promised[mid]; peers != nil {
		delete(peers, p)
		if len(peers) == 0 {
			delete(pk.promised, mid)
		}
	}
	pk.breaks[p]++
	if pk.breaks[p] >= pk.k {
		pk.parked[p] = time.Now().Add(pk.ttl)
	}
}

// peerParked reports whether a peer's announcements are currently ineligible for pull
// selection. Expired parks clear lazily.
func (gs *GossipSubRouter) peerParked(p peer.ID) bool {
	pk := gs.park
	if pk == nil {
		return false
	}
	until, ok := pk.parked[p]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(pk.parked, p)
		return false
	}
	return true
}

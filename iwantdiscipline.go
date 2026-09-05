package pubsub

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// IWANT discipline: at most one outstanding IWANT per message id.
//
// Stock gossipsub re-requests a message id whenever another peer's IHAVE arrives while an
// earlier IWANT is still in flight: handleIHave only checks seenMessage, which is false until
// the payload lands. On announce-heavy paths that is the dominant duplicate source. The
// discipline allows one outstanding IWANT per id within a window; on expiry the next IHAVE may
// re-request, so a lost reply costs one window rather than liveness.

// WithIWantDiscipline enables one-outstanding-IWANT-per-id with the given expiry window on
// every topic the matcher accepts; a nil matcher covers every topic. The window must cover a
// round trip plus the peer's transmission of what was asked -- the same sizing rule as any
// request timeout; sub-RTT values reintroduce the duplicates.
func WithIWantDiscipline(match func(topic string) bool, window time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant discipline requires a gossipsub router")
		}
		if window <= 0 {
			return fmt.Errorf("iwant discipline window must be positive")
		}
		gs.iwantWindow = window
		gs.iwantMatch = match
		gs.iwantAsked = make(map[string]time.Time)
		return nil
	}
}

// disciplined reports whether the IWANT discipline applies to announcements on topic.
func (gs *GossipSubRouter) disciplined(topic string) bool {
	return gs.iwantAsked != nil && (gs.iwantMatch == nil || gs.iwantMatch(topic))
}

// iwantAllowed reports whether an IWANT for mid, announced on topic, may be issued to from
// now, recording it if so. Runs on pubsub's goroutine, like every router map.
//
// A peer whose earlier ask for this id is still unjudged is refused even after the window has
// moved on: the move-on exists to try someone else, and a repeat to the same peer could only
// duplicate a response already on its way or re-stamp a promise the withholder is about to
// break. With one announcer left the receiver waits for that judgment instead.
func (gs *GossipSubRouter) iwantAllowed(mid, topic string, from peer.ID) bool {
	if !gs.disciplined(topic) {
		return true
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		return false
	}
	if gs.promiseOpen(mid, from) {
		return false
	}
	gs.iwantAsked[mid] = time.Now()
	gs.notePromise(mid, from, gs.iwantAsked[mid])
	gs.noteOfferTried(mid, from)
	return true
}

// noteDelivered runs on every received message: delivery keeps every open promise for the
// id, from whichever peer served it, and the id's offers are no longer needed.
func (gs *GossipSubRouter) noteDelivered(mid string) {
	if gs.park != nil {
		delete(gs.park.promised, mid)
	}
	gs.clearOffers(mid)
}

// clearIWantAsked drops expired outstanding-request records; called from the heartbeat.
// Delivery deliberately does not clear an entry: a delivered id is seen, so no later IHAVE
// can re-request it, and the entry ages out here.
func (gs *GossipSubRouter) clearIWantAsked() {
	if gs.iwantAsked == nil {
		return
	}
	for mid, at := range gs.iwantAsked {
		if time.Since(at) >= gs.iwantWindow {
			delete(gs.iwantAsked, mid)
		}
	}
}

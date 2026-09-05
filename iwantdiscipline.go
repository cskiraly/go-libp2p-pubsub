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

// disciplinedAsksPerHeartbeat caps the asks granted to one peer per heartbeat on disciplined
// topics. Stock bounds a peer's asks per heartbeat by MaxIHaveLength (5000), sized for ids
// that cost a lookup each; here every granted ask also opens a promise and a retry check, and
// a withholder is judged only at its deadline, so one announcer could open thousands of them
// per heartbeat before its first break parks it. Honest announcers on a segmented payload
// path offer a few dozen ids per payload (a 1 MiB payload in 32 KiB segments is 32); 512
// leaves an order of magnitude above that and takes one off the attacker.
const disciplinedAsksPerHeartbeat = 512

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
//
// A peer at its per-heartbeat ask cap is refused too; the id stays unasked and open to the
// next announcer, or to this one after the heartbeat.
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
	if gs.iwantGranted[from] >= disciplinedAsksPerHeartbeat {
		return false
	}
	if gs.iwantGranted == nil {
		gs.iwantGranted = make(map[peer.ID]int)
	}
	gs.iwantGranted[from]++
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

// undoIWantDispatch reverses the bookkeeping of IWANTs that were never transmitted.
//
// sendRPC reports queue admission, and a refused RPC is not retried. Everything the dispatch
// path recorded beforehand -- the discipline window, the promise, the tracer promise, the
// application's per-claim charge, the per-peer ask count -- therefore describes a request the
// peer never received, and charging any of it blames a peer for our own send queue.
//
// The ask is deliberately not re-sent: a refusal at queue admission is backpressure on that
// peer's queue, and answering it with another send is the retry storm. The peer instead cools
// down for one window so the next selection rotates past it, and the offer table's record of
// the attempt stays, because selection prefers untried announcers and forgetting the attempt
// would make the peer whose queue is full the most attractive candidate again.
func (gs *GossipSubRouter) undoIWantDispatch(p peer.ID, mids []string) {
	if len(mids) == 0 {
		return
	}
	if n := gs.iasked[p] - len(mids); n > 0 {
		gs.iasked[p] = n
	} else {
		delete(gs.iasked, p)
	}
	gs.gossipTracer.dropPromise(p, mids)
	gs.gateFailed(p, mids)
	for _, mid := range mids {
		if pk := gs.park; pk != nil {
			if peers := pk.promised[mid]; peers != nil {
				delete(peers, p)
				if len(peers) == 0 {
					delete(pk.promised, mid)
				}
			}
		}
		// Only a disciplined id has a window record, and only that ask counted against the
		// peer's cap.
		if _, asked := gs.iwantAsked[mid]; asked {
			delete(gs.iwantAsked, mid)
			if gs.iwantGranted[p] > 1 {
				gs.iwantGranted[p]--
			} else {
				delete(gs.iwantGranted, p)
			}
		}
	}
	if gs.iwantAsked != nil {
		if gs.iwantRefused == nil {
			gs.iwantRefused = make(map[peer.ID]time.Time)
		}
		gs.iwantRefused[p] = time.Now()
	}
}

// sendRefusedRecently reports whether an IWANT to p was refused at queue admission within the
// last window, in which case selection should rotate past it.
func (gs *GossipSubRouter) sendRefusedRecently(p peer.ID) bool {
	at, ok := gs.iwantRefused[p]
	return ok && time.Since(at) < gs.iwantWindow
}

// clearIWantAsked drops expired outstanding-request records and lapsed cooldowns and resets the
// per-peer ask counts; called from the heartbeat. Delivery deliberately does not clear a
// request record: a delivered id is seen, so no later IHAVE can re-request it, and the entry
// ages out here.
func (gs *GossipSubRouter) clearIWantAsked() {
	if gs.iwantAsked == nil {
		return
	}
	if len(gs.iwantGranted) > 0 {
		gs.iwantGranted = make(map[peer.ID]int)
	}
	for mid, at := range gs.iwantAsked {
		if time.Since(at) >= gs.iwantWindow {
			delete(gs.iwantAsked, mid)
		}
	}
	for p, at := range gs.iwantRefused {
		if time.Since(at) >= gs.iwantWindow {
			delete(gs.iwantRefused, p)
		}
	}
}

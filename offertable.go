package pubsub

import (
	"fmt"
	"sort"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Offer table: remember who announced an id, and re-ask when the discipline moves on.
//
// Under the IWANT discipline a receiver acts on the first IHAVE for an id and ignores the
// others while its ask is in flight. Relays announce once, so when that ask goes unserved the
// other offers are gone and the receiver waits for a fresh announcement that may never come.
// The offer table keeps the announcers of every unseen id, and one window after each granted
// ask re-asks the best remembered announcer if the id is still missing: untried first, then
// least recently tried.
type offerTable struct {
	offers map[string]map[peer.ID]time.Time // unseen id -> announcers and when they offered
	tried  map[string]map[peer.ID]time.Time // asks already spent per id
	topic  map[string]string                // id -> topic it was announced under
	byPeer map[peer.ID]int                  // offers recorded per peer, for the per-peer cap
	armed  map[string]struct{}              // ids with a retry check queued; one at a time

	// retries is the retry-check queue in due order, with one timer armed for its head. Every
	// check is due one window after it is armed, so arm order is due order and a FIFO serves;
	// with a per-peer window the due time would depend on the peer asked, and this would
	// have to become a min-heap on the due time.
	retries []offerRetry
	timer   bool // a timer for the head of retries is in flight
}

// offerRetry is one queued retry check.
type offerRetry struct {
	mid string
	due time.Time
}

// Per-peer and per-id bounds on the offer table. A peer beyond its budget stops being
// recorded; a single id keeps at most offerPerID announcers. Entries also age out
// (offerTTL, swept from the heartbeat), so fabricated never-delivered ids cannot accumulate.
const (
	offerPerPeer = 256
	offerPerID   = 16
	offerTTL     = 30 * time.Second
)

// WithOfferTable remembers the announcers of unseen ids and re-asks the next one when an ask
// under the IWANT discipline goes unserved for a window. Requires WithIWantDiscipline first.
func WithOfferTable() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("offer table requires a gossipsub router")
		}
		if gs.iwantAsked == nil {
			return fmt.Errorf("offer table requires WithIWantDiscipline first")
		}
		gs.offers = &offerTable{
			offers: make(map[string]map[peer.ID]time.Time),
			tried:  make(map[string]map[peer.ID]time.Time),
			topic:  make(map[string]string),
			byPeer: make(map[peer.ID]int),
			armed:  make(map[string]struct{}),
		}
		return nil
	}
}

// recordOffer notes that peer p announced mid on a disciplined topic; a fact, kept until
// delivery or expiry.
func (gs *GossipSubRouter) recordOffer(mid, topic string, p peer.ID) {
	ot := gs.offers
	if ot == nil || !gs.disciplined(topic) || gs.p.seenMessage(mid) {
		return
	}
	if ot.byPeer[p] >= offerPerPeer {
		return
	}
	peers := ot.offers[mid]
	if peers == nil {
		peers = make(map[peer.ID]time.Time)
		ot.offers[mid] = peers
		ot.topic[mid] = topic
	} else if len(peers) >= offerPerID {
		if _, known := peers[p]; !known {
			return
		}
	}
	if _, known := peers[p]; !known {
		ot.byPeer[p]++
	}
	peers[p] = time.Now()
}

// noteOfferTried records an ask granted to p for mid and queues the retry check one window
// later. Tried history follows retained offers, so it shares their per-id bound and expiry.
// A tracked id still gets a retry when this announcer's offer was refused: other announcers
// can serve it once the granted ask's window expires.
func (gs *GossipSubRouter) noteOfferTried(mid string, p peer.ID) {
	ot := gs.offers
	if ot == nil {
		return
	}
	if _, tracked := ot.offers[mid]; !tracked {
		return
	}
	if _, retained := ot.offers[mid][p]; retained {
		if ot.tried[mid] == nil {
			ot.tried[mid] = make(map[peer.ID]time.Time)
		}
		ot.tried[mid][p] = time.Now()
	}
	gs.armOfferRetry(mid)
}

// armOfferRetry queues the retry check for mid one window out, unless one is already queued:
// an id owns at most one retry check, however many asks or skips it goes through, so the
// chain cannot multiply. It reports whether a check was queued.
func (gs *GossipSubRouter) armOfferRetry(mid string) bool {
	ot := gs.offers
	if ot == nil {
		return false
	}
	if _, inFlight := ot.armed[mid]; inFlight {
		return false
	}
	ot.armed[mid] = struct{}{}
	ot.retries = append(ot.retries, offerRetry{mid: mid, due: time.Now().Add(gs.iwantWindow + 5*time.Millisecond)})
	gs.armOfferSweep()
	return true
}

// armOfferSweep schedules one pass over the retry queue for its head unless a pass is already
// in flight; the pass runs on the router goroutine and re-arms for the next head, so there is
// one timer however many checks are queued.
func (gs *GossipSubRouter) armOfferSweep() {
	ot := gs.offers
	if ot.timer || len(ot.retries) == 0 {
		return
	}
	ot.timer = true
	time.AfterFunc(time.Until(ot.retries[0].due), func() {
		select {
		case gs.p.eval <- gs.sweepOfferRetries:
		case <-gs.p.ctx.Done():
		}
	})
}

// sweepOfferRetries runs every retry check that is due, in order, then re-arms for the next
// one. A check may queue its own successor; that entry is due a window from now, so the pass
// stops at it. Runs on the router goroutine.
func (gs *GossipSubRouter) sweepOfferRetries() {
	ot := gs.offers
	if ot == nil {
		return
	}
	now := time.Now()
	for len(ot.retries) > 0 && !ot.retries[0].due.After(now) {
		r := ot.retries[0]
		ot.retries[0] = offerRetry{}
		ot.retries = ot.retries[1:]
		delete(ot.armed, r.mid)
		gs.retryOfferedPull(r.mid)
	}
	if len(ot.retries) == 0 {
		ot.retries = nil
	}
	ot.timer = false
	gs.armOfferSweep()
}

// clearOffers forgets a delivered id.
func (gs *GossipSubRouter) clearOffers(mid string) {
	ot := gs.offers
	if ot == nil {
		return
	}
	for p := range ot.offers[mid] {
		ot.refundOffer(p)
	}
	delete(ot.offers, mid)
	delete(ot.tried, mid)
	delete(ot.topic, mid)
}

// refundOffer returns one offer to p's budget, dropping the counter at zero so peers that came
// and went do not stay in the map.
func (ot *offerTable) refundOffer(p peer.ID) {
	if ot.byPeer[p] > 1 {
		ot.byPeer[p]--
		return
	}
	delete(ot.byPeer, p)
}

// sweepOffers ages out stale offers; called from the heartbeat. It also drops tried records
// whose id is no longer tracked, so the invariant that tried follows offers holds even if
// an id leaves the table by a path that forgot to clear it.
func (gs *GossipSubRouter) sweepOffers() {
	ot := gs.offers
	if ot == nil {
		return
	}
	for mid, peers := range ot.offers {
		for p, at := range peers {
			if time.Since(at) >= offerTTL {
				delete(peers, p)
				delete(ot.tried[mid], p)
				ot.refundOffer(p)
			}
		}
		if len(peers) == 0 {
			gs.clearOffers(mid)
		}
	}
	for mid := range ot.tried {
		if _, tracked := ot.offers[mid]; !tracked {
			delete(ot.tried, mid)
		}
	}
}

// pickOfferedPeer chooses the announcer to re-ask for mid: untried first, then least recently
// tried, in deterministic peer order. Disconnected, parked, cooling-down and gate-declined
// announcers are skipped, and so is any announcer whose earlier ask for this id is still
// unjudged; waiting reports whether at
// least one announcer was skipped for that reason, since that state resolves on its own clock
// (delivery or the promise deadline) and is worth coming back for.
func (gs *GossipSubRouter) pickOfferedPeer(mid string) (pick peer.ID, found, waiting bool) {
	ot := gs.offers
	offers := ot.offers[mid]
	if len(offers) == 0 {
		return "", false, false
	}
	cands := make([]peer.ID, 0, len(offers))
	for p := range offers {
		if _, connected := gs.p.peers[p]; !connected {
			continue // the announcer left; nothing to send to
		}
		if gs.peerParked(p) || gs.sendRefusedRecently(p) {
			continue
		}
		if gs.promiseOpen(mid, p) {
			waiting = true
			continue
		}
		if !gs.gateAllows(p, ot.topic[mid], mid) {
			continue
		}
		cands = append(cands, p)
	}
	if len(cands) == 0 {
		return "", false, waiting
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i] < cands[j] })
	tried := ot.tried[mid]
	var oldest time.Time
	for _, p := range cands {
		t, was := tried[p]
		if !was {
			return p, true, waiting
		}
		if !found || t.Before(oldest) {
			pick, found, oldest = p, true, t
		}
	}
	return pick, found, waiting
}

// retryOfferedPull runs on the router goroutine one window after an ask was granted. If the
// id is still missing and no ask is in flight, it re-asks the best remembered announcer.
func (gs *GossipSubRouter) retryOfferedPull(mid string) {
	if gs.offers == nil || len(gs.offers.offers[mid]) == 0 {
		return
	}
	if gs.p.seenMessage(mid) {
		gs.clearOffers(mid)
		return
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		// This check may predate the in-flight ask and have suppressed that ask's arm.
		// Keep a successor so an unserved ask still gets retried after its window.
		gs.armOfferRetry(mid)
		return
	}
	pick, found, waiting := gs.pickOfferedPeer(mid)
	if !found {
		if waiting {
			// The only announcers left are still being judged. Come back once, rather
			// than waiting for an event that may never come: a broken promise parks the
			// peer and delivery clears the id, and either way the next check settles it.
			gs.armOfferRetry(mid)
		}
		return
	}
	allowed, attempt := gs.iwantAllowed(mid, gs.offers.topic[mid], pick)
	if !allowed {
		return
	}
	gs.iasked[pick]++
	traced := gs.gossipTracer.AddPromise(pick, []string{mid})
	gs.gateCommitted(pick, []string{mid})
	if !gs.sendRPC(pick, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), true) {
		// Never transmitted: undo the charges and come back once the cooldown has rotated
		// selection past this peer.
		gs.undoIWantDispatch(pick, []string{mid}, map[string]iwantAttempt{mid: attempt}, traced)
		gs.armOfferRetry(mid)
	}
}

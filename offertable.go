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
	armed  map[string]struct{}              // ids with a retry timer in flight; one at a time
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

// noteOfferTried records an ask granted to p for mid and schedules the retry check one window
// later, on the router goroutine.
func (gs *GossipSubRouter) noteOfferTried(mid string, p peer.ID) {
	ot := gs.offers
	if ot == nil {
		return
	}
	if ot.tried[mid] == nil {
		ot.tried[mid] = make(map[peer.ID]time.Time)
	}
	ot.tried[mid][p] = time.Now()
	gs.armOfferRetry(mid)
}

// armOfferRetry schedules the retry check for mid one window out, unless one is already in
// flight: an id owns at most one retry timer, however many asks or skips it goes through, so
// the chain cannot multiply. It reports whether a timer was armed.
func (gs *GossipSubRouter) armOfferRetry(mid string) bool {
	ot := gs.offers
	if ot == nil {
		return false
	}
	if _, inFlight := ot.armed[mid]; inFlight {
		return false
	}
	ot.armed[mid] = struct{}{}
	time.AfterFunc(gs.iwantWindow+5*time.Millisecond, func() {
		select {
		case gs.p.eval <- func() {
			delete(ot.armed, mid)
			gs.retryOfferedPull(mid)
		}:
		case <-gs.p.ctx.Done():
		}
	})
	return true
}

// clearOffers forgets a delivered id.
func (gs *GossipSubRouter) clearOffers(mid string) {
	ot := gs.offers
	if ot == nil {
		return
	}
	for p := range ot.offers[mid] {
		if ot.byPeer[p] > 0 {
			ot.byPeer[p]--
		}
	}
	delete(ot.offers, mid)
	delete(ot.tried, mid)
	delete(ot.topic, mid)
}

// sweepOffers ages out stale offers; called from the heartbeat.
func (gs *GossipSubRouter) sweepOffers() {
	ot := gs.offers
	if ot == nil {
		return
	}
	for mid, peers := range ot.offers {
		for p, at := range peers {
			if time.Since(at) >= offerTTL {
				delete(peers, p)
				if ot.byPeer[p] > 0 {
					ot.byPeer[p]--
				}
			}
		}
		if len(peers) == 0 {
			gs.clearOffers(mid)
		}
	}
}

// pickOfferedPeer chooses the announcer to re-ask for mid: untried first, then least recently
// tried, in deterministic peer order. Parked and gate-declined announcers are skipped, and so
// is any announcer whose earlier ask for this id is still unjudged; waiting reports whether at
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
		if gs.peerParked(p) {
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
	if gs.offers == nil {
		return
	}
	if gs.p.seenMessage(mid) {
		gs.clearOffers(mid)
		return
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		return // an ask is in flight; its own retry timer will fire
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
	if !gs.iwantAllowed(mid, gs.offers.topic[mid], pick) {
		return
	}
	gs.iasked[pick]++
	gs.gossipTracer.AddPromise(pick, []string{mid})
	gs.gateCommitted(pick, []string{mid})
	gs.sendRPC(pick, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{mid}}}, nil, nil, nil), false)
}

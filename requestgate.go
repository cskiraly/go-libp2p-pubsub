package pubsub

import (
	"fmt"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Application request gate.
//
// A receiver in announce-and-pull decides whether to ask, so it can decline what it cannot yet
// authenticate: for a payload bound to a consensus commitment, that means declining an
// announcement whose authorizing block has not arrived. The decision point already exists and
// is already policy-driven; what is missing is a place for the application to answer, since
// only it knows what a message id claims and whether that claim is currently satisfiable.
//
// The gate is deliberately opaque: it sees the peer, the topic and the raw message id and
// nothing else. How an id encodes its claim, which blocks are installed, what per-claim caps
// apply, all of that stays in the application.

// RequestGate is an application veto on requesting announced message ids.
//
// Two methods, because a one-shot boolean cannot both decide and account. Allow runs ahead of
// every request policy, and those policies may still refuse an id the gate approved; a gate
// that charged a budget inside Allow would spend it on ids no IWANT ever carried. So Allow must
// be pure, and Committed reports exactly the ids that were dispatched.
type RequestGate interface {
	// Allow reports whether p's announcement of mid on topic may be requested now. It MUST NOT
	// record state: a false answer has to leave the id exactly as requestable as it was, since
	// an application that declines for want of authority will want the same id once authority
	// arrives.
	Allow(p peer.ID, topic, mid string) bool
	// Committed reports the ids actually dispatched in an IWANT to p. This is the only place an
	// application may charge per-claim budgets or record provenance.
	Committed(p peer.ID, mids []string)
}

// RequestDeferral is the router-side ledger of announcements a gate declined, plus the trigger
// that replays them.
//
// The ledger lives in the router and the trigger belongs to the application. Replaying a
// declined announcement needs the announcer's identity, the per-peer request allowance and
// the outstanding-request ledgers, all router state; knowing when the answer changed needs the
// block, which only the application has. So the router remembers who offered what, and the
// application says when to ask again.
//
// Bounded on three axes, because a declined announcement is attacker-suppliable: total
// entries, announcers per entry, and age. Beyond the mcache's serve window an announcer could
// not serve the message anyway.
type RequestDeferral struct {
	max    int
	replay func()
}

// NewRequestDeferral returns a deferral ledger holding at most max declined ids.
func NewRequestDeferral(max int) *RequestDeferral {
	if max < 0 {
		max = 0
	}
	return &RequestDeferral{max: max}
}

// Replay asks the router to re-offer every deferred announcement to the gate and request
// those now permitted. Safe to call from any goroutine and from a timer; the work runs on the
// router's loop.
//
// Without it, recovery waits for heartbeat gossip, which fires once per heartbeat to a random
// subset of non-mesh peers: it excludes mesh peers on the assumption that they are being
// pushed to, an assumption a receiver-side gate falsifies.
func (d *RequestDeferral) Replay() {
	if d == nil || d.replay == nil {
		return
	}
	d.replay()
}

// WithRequestGate installs a request gate, and optionally the deferral ledger that makes a
// decline recoverable. Returning false from Allow declines the id without consuming any
// request state, so the same id can be requested later when the answer changes.
//
// Declining is not the same as forgetting, and nothing re-delivers a declined announcement on
// its own: heartbeat gossip excludes mesh peers and phase forwarding's immediate IHAVEs are
// one-shot. A nil deferral is therefore a deliberate choice of no retry.
func WithRequestGate(gate RequestGate, deferral *RequestDeferral) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("request gate requires a gossipsub router")
		}
		if gate == nil {
			return fmt.Errorf("request gate requires a non-nil gate")
		}
		gs.requestGate = gate
		if deferral == nil {
			return nil
		}
		gs.deferredMax = deferral.max
		gs.deferred = make(map[string]*deferredEntry)
		deferral.replay = func() {
			select {
			case gs.p.eval <- gs.replayDeferred:
			case <-gs.p.ctx.Done():
			}
		}
		return nil
	}
}

// gateAllows reports whether the gate, if any, permits asking p for mid on topic.
func (gs *GossipSubRouter) gateAllows(p peer.ID, topic, mid string) bool {
	return gs.requestGate == nil || gs.requestGate.Allow(p, topic, mid)
}

// gateCommitted reports a dispatched IWANT to the gate, if any.
func (gs *GossipSubRouter) gateCommitted(p peer.ID, mids []string) {
	if gs.requestGate != nil {
		gs.requestGate.Committed(p, mids)
	}
}

// deferredEntry is one declined announcement: who offered it, on what topic, and when it was
// first declined. `at` is set once and never refreshed, so a repeatedly re-announced id still
// expires.
type deferredEntry struct {
	topic string
	peers []peer.ID
	at    time.Time
}

// maxDeferredAnnouncers caps how many peers are remembered per declined id. Fallback across
// announcers matters (the first may disconnect or refuse to serve) but the tail is worthless.
const maxDeferredAnnouncers = 4

// noteDeclined records that p offered mid on topic and the gate said no.
func (gs *GossipSubRouter) noteDeclined(p peer.ID, topic, mid string) {
	if gs.deferred == nil {
		return
	}
	e, ok := gs.deferred[mid]
	if !ok {
		if len(gs.deferred) >= gs.deferredMax {
			return
		}
		gs.deferred[mid] = &deferredEntry{topic: topic, peers: []peer.ID{p}, at: time.Now()}
		return
	}
	if len(e.peers) >= maxDeferredAnnouncers {
		return
	}
	for _, have := range e.peers {
		if have == p {
			return
		}
	}
	e.peers = append(e.peers, p)
}

// replayDeferred re-offers deferred ids to the gate and requests those now permitted. Runs on
// the router's loop.
//
// It goes through the ordinary selection path rather than around it, so per-peer allowances
// and the outstanding-request discipline apply exactly as they would to a fresh announcement.
// Each id is assigned to ONE announcer, balancing across announcers, before anything is
// requested: the peers that announced during the closed window are exactly the ones that
// would otherwise be asked for the whole group, and a replay that swamped their uplinks was
// measured to be worse than no retry at all.
func (gs *GossipSubRouter) replayDeferred() {
	if gs.deferred == nil || gs.requestGate == nil {
		return
	}
	ttl := time.Duration(gs.params.HistoryLength) * gs.params.HeartbeatInterval
	announcersOf := make(map[string][]peer.ID, len(gs.deferred))
	topicOf := make(map[string]string, len(gs.deferred))
	for mid, e := range gs.deferred {
		// Expired, or it arrived while we waited: either way stop tracking it. "Arrived"
		// means delivered, not seen: a message is marked seen before application
		// validation, so an id the application declined and did not retain is seen while
		// having reached nobody. deliveredMessages is the distinction the library keeps.
		if time.Since(e.at) >= ttl || gs.p.deliveredMessages.Has(mid) {
			delete(gs.deferred, mid)
			continue
		}
		topicOf[mid] = e.topic
		for _, pid := range e.peers {
			if _, connected := gs.p.peers[pid]; !connected {
				continue
			}
			announcersOf[mid] = append(announcersOf[mid], pid)
		}
	}
	ids := make([]string, 0, len(announcersOf))
	for mid := range announcersOf {
		ids = append(ids, mid)
	}
	shuffleStrings(ids)
	load := make(map[peer.ID]int)
	assigned := make(map[peer.ID][]string)
	for _, mid := range ids {
		best, bestLoad := peer.ID(""), -1
		for _, pid := range announcersOf[mid] {
			if l := load[pid]; bestLoad < 0 || l < bestLoad {
				best, bestLoad = pid, l
			}
		}
		load[best]++
		assigned[best] = append(assigned[best], mid)
	}
	for pid, mids := range assigned {
		allowance := gs.params.MaxIHaveLength - gs.iasked[pid]
		if allowance <= 0 {
			continue
		}
		// Shuffle before selection, not after: the policies record state as a side effect
		// of allowing an id, so selecting freely and truncating would leave phantom entries.
		shuffleStrings(mids)
		lst := gs.selectIWants(mids, topicOf, pid, allowance)
		if len(lst) == 0 {
			continue
		}
		gs.iasked[pid] += len(lst)
		gs.gossipTracer.AddPromise(pid, lst)
		gs.gateCommitted(pid, lst)
		gs.sendRPC(pid, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: lst}}, nil, nil, nil), false)
		for _, mid := range lst {
			delete(gs.deferred, mid)
		}
	}
}

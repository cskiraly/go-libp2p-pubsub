package pubsub

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Adversaries against the request rules (hedge-and-adaptivity plan, E7) ---
//
// Extensions of the F1 withholder for the screens the plan names, plus a slow reader. Each is a
// harness-only option on a hash-selected node set; none is a protocol feature.

// WithIWantWithholdingSelect restricts WithIWantWithholding to the ids the predicate accepts;
// every other id is served normally. The selective withholder of the last-piece screen serves
// everything but the piece the victims will need last. Requires WithIWantWithholding first.
func WithIWantWithholdingSelect(pred func(mid string) bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || !gs.withholdServe {
			return fmt.Errorf("selective withholding requires WithIWantWithholding first")
		}
		if pred == nil {
			return fmt.Errorf("selective withholding requires a predicate")
		}
		gs.withholdSelect = pred
		return nil
	}
}

// WithIWantWithholdingPrefix orders the injection in time by count. With fastFirst, the first n
// withholdable ids this node is asked for are served at once and the rest get the injection: a
// fast prefix then a slow or silent tail. Without it the first n get the injection and the rest
// are served at once. The median-poisoning screen's two timed strategies. Requires
// WithIWantWithholding first.
func WithIWantWithholdingPrefix(n int, fastFirst bool) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok || !gs.withholdServe {
			return fmt.Errorf("withholding prefix requires WithIWantWithholding first")
		}
		if n <= 0 {
			return fmt.Errorf("withholding prefix must be positive")
		}
		gs.withholdPrefix = n
		gs.withholdPrefixFast = fastFirst
		return nil
	}
}

// withholdIWantServe applies the injection to a built reply and returns what is served normally:
// the ids the selection exempts and the fast side of a prefix. The rest is swallowed (silent) or
// sent whole after the delay, on pubsub's own goroutine like every router action.
func (gs *GossipSubRouter) withholdIWantServe(p peer.ID, mids []string, msgs []*pb.Message) []*pb.Message {
	var serve, held []*pb.Message
	for i, m := range msgs {
		if gs.withholdSelect != nil && !gs.withholdSelect(mids[i]) {
			serve = append(serve, m)
			continue
		}
		if gs.withholdPrefix > 0 {
			inPrefix := gs.withholdAsked < gs.withholdPrefix
			gs.withholdAsked++
			if inPrefix == gs.withholdPrefixFast {
				serve = append(serve, m)
				continue
			}
		}
		held = append(held, m)
	}
	if len(held) == 0 {
		return serve
	}
	if gs.withheldServes != nil {
		gs.withheldServes.Add(int64(len(held)))
	}
	if gs.withholdDelay > 0 {
		time.AfterFunc(gs.withholdDelay, func() {
			select {
			case gs.p.eval <- func() {
				if _, ok := gs.p.peers[p]; !ok {
					return
				}
				gs.sendRPC(p, rpcWithControl(held, nil, nil, nil, nil, nil), false)
			}:
			case <-gs.p.ctx.Done():
			}
		})
	}
	return serve
}

// WithIHaveHearsay makes this node re-announce every id it hears announced, whether or not it
// holds it: the offer-capture adversary. Its IHAVE reaches a victim before an honest relay has
// the message to announce, so it wins the first ask, and stacked on WithIWantWithholding the ask
// is lost to a move-on. Once per id.
//
// width caps how many peers each id is announced to, mesh peers first and then the rest in
// deterministic peer order; 0 means every peer of the topic. The distinction is the difference
// between an attacker that lies at honest fan-out and one that floods: an honest phase-forwarding
// relay announces to the mesh peers it did not push to plus the heartbeat's lazy subset, so a
// width near the mesh degree is the conformant liar. The unbounded form is only reachable because
// phase forwarding needs gossipsub's per-heartbeat IHAVE limit raised far above its default.
// The counter, when non-nil, records the (id, peer) announcements.
func WithIHaveHearsay(width int, announced *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("hearsay announcing requires a gossipsub router")
		}
		if width < 0 {
			return fmt.Errorf("hearsay width must be non-negative")
		}
		gs.hearsay = true
		gs.hearsayWidth = width
		gs.hearsaid = make(map[string]struct{})
		gs.hearsayCtr = announced
		return nil
	}
}

// hearsayAnnounce forwards the announced ids, first time each, to the peers hearsayTargets
// picks. Router goroutine.
func (gs *GossipSubRouter) hearsayAnnounce(from peer.ID, ctl *pb.ControlMessage) {
	for _, e := range ctl.GetIhave() {
		topic := e.GetTopicID()
		if _, ok := gs.p.topics[topic]; !ok {
			continue
		}
		ids := make([]string, 0, len(e.GetMessageIDs()))
		for _, mid := range e.GetMessageIDs() {
			if _, done := gs.hearsaid[mid]; done {
				continue
			}
			gs.hearsaid[mid] = struct{}{}
			ids = append(ids, mid)
		}
		if len(ids) == 0 {
			continue
		}
		for _, p2 := range gs.hearsayTargets(topic, from) {
			ihave := []*pb.ControlIHave{{TopicID: &topic, MessageIDs: ids}}
			gs.sendRPC(p2, rpcWithControl(nil, ihave, nil, nil, nil, nil), false)
			if gs.hearsayCtr != nil {
				gs.hearsayCtr.Add(int64(len(ids)))
			}
		}
	}
}

// hearsayTargets is the peer set one hearsay announcement reaches: mesh peers of the topic first,
// then its other peers, each in deterministic peer order, truncated to the configured width.
func (gs *GossipSubRouter) hearsayTargets(topic string, from peer.ID) []peer.ID {
	tmap, ok := gs.p.topics[topic]
	if !ok {
		return nil
	}
	mesh := gs.mesh[topic]
	var inMesh, rest []peer.ID
	for p2 := range tmap {
		if p2 == from {
			continue
		}
		if _, m := mesh[p2]; m {
			inMesh = append(inMesh, p2)
		} else {
			rest = append(rest, p2)
		}
	}
	sort.Slice(inMesh, func(i, j int) bool { return inMesh[i] < inMesh[j] })
	sort.Slice(rest, func(i, j int) bool { return rest[i] < rest[j] })
	out := append(inMesh, rest...)
	if gs.hearsayWidth > 0 && len(out) > gs.hearsayWidth {
		out = out[:gs.hearsayWidth]
	}
	return out
}

// WithReadDelay makes this node read its inbound streams slowly: d elapses before each read. The
// slow-reading peer of the queue-poisoning screen: its senders' writers block on the stream's
// flow control, and a node-wide maximum over per-peer queue signals reads that one peer as
// congestion everywhere.
func WithReadDelay(d time.Duration) Option {
	return func(ps *PubSub) error {
		if d <= 0 {
			return fmt.Errorf("read delay must be positive")
		}
		ps.readDelay = d
		return nil
	}
}

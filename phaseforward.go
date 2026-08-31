package pubsub

import (
	"fmt"
	"hash/fnv"
	"sort"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Phase forwarding: a push-pull phase transition on gossipsub's eager-push path.
//
// Stock gossipsub forwards a validated message to every mesh peer that has not sent an
// IDONTWANT for it. Phase forwarding instead pushes it to at most
// max(0, degree - knownHolders) of them and sends the rest an immediate IHAVE, where
// knownHolders is the count of mesh peers that already announced the message via IDONTWANT.
// Early in a message's diffusion nobody is known to hold it and it is pushed degree-wide;
// late, when duplicates concentrate, it is announced instead, and peers that still lack it
// pull with their ordinary IWANT.
//
// This is the PPPT proposal (push-pull phase transition) with the IDONTWANT count standing
// in for the hop counter, so it needs no wire change: receivers already answer IHAVE with an
// immediate IWANT and serve IWANT from mcache. Two operational caveats: the estimator only
// sees mesh peers that have IDONTWANT'd, which requires messages at or above the router's
// IDONTWANT size threshold to be effective; and the per-heartbeat MaxIHaveMessages budget on
// the receiving side must accommodate one IHAVE per forwarded message rather than the
// heartbeat-batched cadence it was sized for.
type phaseForwarding struct {
	match  func(topic string) bool
	degree int

	// Source policy: what the *publisher* of a message does with the mesh peers it does not
	// push to. The default -- announce to all of them immediately -- turns the source into
	// the pull target of first resort: at the moment of publishing it is the only holder, so
	// every announced peer IWANTs from it at once, and its transmit balloons (measured 2.7x
	// variant A's publisher at degree 70). The three alternatives shape when and to whom the
	// source announces, so pulls land on first-hop holders instead.
	srcMode   PhaseSourceMode
	srcDelay  time.Duration
	srcDegree int

	// Meshless mode dissolves the topic mesh's role in forwarding: push candidates are all
	// connected gossipsub-capable topic peers rather than the mesh, ranked per message as
	// usual, and the peers not pushed are announced up to announceWidth (0 = all of them).
	// The mesh still exists for gossipsub's own maintenance; it just stops deciding who gets
	// eager copies. Known v1 limitation: IDONTWANT is only emitted to mesh peers, so the
	// known-holders decay undercounts and the push degree stays near r.
	meshless      bool
	announceWidth int
}

// WithPhaseMeshless widens phase forwarding's candidate set from the topic mesh to every
// connected gossipsub-capable topic peer, announcing the non-pushed candidates up to
// announceWidth per message (0 announces all of them). Requires WithPhaseForwarding.
func WithPhaseMeshless(announceWidth int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("meshless phase requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("meshless phase requires WithPhaseForwarding first")
		}
		if announceWidth < 0 {
			return fmt.Errorf("announce width must be non-negative")
		}
		gs.phaseForward.meshless = true
		gs.phaseForward.announceWidth = announceWidth
		return nil
	}
}

// PhaseSourceMode selects the source-side staggering policy.
type PhaseSourceMode int

const (
	// PhaseSourceImmediate announces every non-pushed mesh peer at publish time. The
	// default, and the mob-the-source baseline.
	PhaseSourceImmediate PhaseSourceMode = iota
	// PhaseSourceDelay pushes now and announces after srcDelay, rechecking IDONTWANT state
	// at fire time so peers that got the message meanwhile are not invited to pull it.
	PhaseSourceDelay
	// PhaseSourceRotate announces each message to only the next srcDegree peers in the same
	// per-message ranking that chose the push targets. Different messages announce to
	// different peers, so each mesh peer initially pulls a disjoint stripe and the rest
	// arrives by cross-pull from relays' announces.
	PhaseSourceRotate
	// PhaseSourceWidePush pushes each message to srcDegree peers and announces to nobody;
	// non-pushed peers learn of the message from relays. Trades the emergent pull mob for a
	// chosen, bounded upload.
	PhaseSourceWidePush
)

// WithPhaseSourcePolicy selects the source-side staggering policy for phase forwarding.
// Must be applied together with WithPhaseForwarding. delay is used by PhaseSourceDelay;
// degree by PhaseSourceRotate (announce width) and PhaseSourceWidePush (push width).
func WithPhaseSourcePolicy(mode PhaseSourceMode, delay time.Duration, degree int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase source policy requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("phase source policy requires WithPhaseForwarding first")
		}
		switch mode {
		case PhaseSourceImmediate:
		case PhaseSourceDelay:
			if delay <= 0 {
				return fmt.Errorf("phase source delay must be positive")
			}
		case PhaseSourceRotate, PhaseSourceWidePush:
			if degree <= 0 {
				return fmt.Errorf("phase source degree must be positive")
			}
		default:
			return fmt.Errorf("unknown phase source mode %d", mode)
		}
		gs.phaseForward.srcMode = mode
		gs.phaseForward.srcDelay = delay
		gs.phaseForward.srcDegree = degree
		return nil
	}
}

// WithPhaseForwarding enables the phase transition on every topic the matcher accepts.
func WithPhaseForwarding(match func(topic string) bool, degree int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("phase forwarding requires a gossipsub router")
		}
		if match == nil || degree < 0 {
			return fmt.Errorf("phase forwarding needs a topic matcher and a non-negative degree")
		}
		// Degree 0 is pull-only: every message is announced (immediate IHAVE) and nothing
		// is pushed unasked, on any node including the source. The gossipsub-native
		// counterpart of an announce-then-pull policy.
		gs.phaseForward = &phaseForwarding{match: match, degree: degree}
		return nil
	}
}

// trim caps the mesh members of tosend at max(0, degree - knownHolders), removing the
// over-budget ones and returning them, rank-ordered, so the caller can announce to them
// instead. Peers outside the mesh set (direct and floodsub peers) are never trimmed.
// fromSelf applies the source policy: it may widen the push (WidePush), and the caller
// narrows or defers the returned announce list per srcMode.
func (pf *phaseForwarding) trim(gs *GossipSubRouter, mid string, csum checksum, gmap map[peer.ID]struct{}, tosend map[peer.ID]struct{}, fromSelf bool) []peer.ID {
	holders := 0
	for p := range gmap {
		if _, ok := gs.unwanted[p][csum]; ok {
			holders++
		}
	}
	degree := pf.degree
	if fromSelf && pf.srcMode == PhaseSourceWidePush {
		degree = pf.srcDegree
	}
	budget := degree - holders
	if budget < 0 {
		budget = 0
	}
	cands := make([]peer.ID, 0, len(tosend))
	for p := range tosend {
		if _, inMesh := gmap[p]; inMesh {
			cands = append(cands, p)
		}
	}
	if budget >= len(cands) {
		return nil
	}
	// Rank per (message, peer) so different messages spread their pushes across different
	// peers, and the choice is stable rather than map-order dependent.
	sort.Slice(cands, func(i, j int) bool {
		ri, rj := phaseRank(mid, cands[i]), phaseRank(mid, cands[j])
		if ri != rj {
			return ri > rj
		}
		return cands[i] > cands[j]
	})
	announce := make([]peer.ID, 0, len(cands)-budget)
	for _, p := range cands[budget:] {
		delete(tosend, p)
		announce = append(announce, p)
	}
	if pf.meshless && pf.announceWidth > 0 && len(announce) > pf.announceWidth {
		// Rank-ordered, so the announce set rotates per message like the push set does.
		announce = announce[:pf.announceWidth]
	}
	if !fromSelf {
		return announce
	}
	switch pf.srcMode {
	case PhaseSourceWidePush:
		// The wide push replaces the announce entirely.
		return nil
	case PhaseSourceRotate:
		// Announce only the next srcDegree in the same per-message ranking the push used;
		// the rank rotates per message, so each mesh peer's initial pulls are a stripe.
		if len(announce) > pf.srcDegree {
			announce = announce[:pf.srcDegree]
		}
		return announce
	case PhaseSourceDelay:
		// Handled by the caller: the announce is scheduled, not yielded.
		return announce
	default:
		return announce
	}
}

// scheduleDeferredIHave announces mid to peers after the source delay, on pubsub's own
// goroutine, rechecking at fire time: a peer that IDONTWANT'd the message meanwhile has it
// and must not be invited to pull, and a peer that disconnected cannot be. This is what
// lets the first-hop holders absorb the pulls -- by the time the deferred announce lands,
// they have announced too, and an IWANT goes to whoever announced first.
func (pf *phaseForwarding) scheduleDeferredIHave(gs *GossipSubRouter, topic, mid string, peers []peer.ID) {
	if len(peers) == 0 {
		return
	}
	time.AfterFunc(pf.srcDelay, func() {
		select {
		case gs.p.eval <- func() {
			csum := computeChecksum(mid)
			for _, pid := range peers {
				if _, ok := gs.unwanted[pid][csum]; ok {
					continue
				}
				if _, ok := gs.p.peers[pid]; !ok {
					continue
				}
				gs.sendRPC(pid, rpcWithControl(nil,
					[]*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{mid}}},
					nil, nil, nil, nil), false)
			}
		}:
		case <-gs.p.ctx.Done():
		}
	})
}

// phaseRank scores one (message, peer) pair. FNV-1a avalanched with the splitmix64
// finalizer: FNV's low bits alone carry almost no mixing, so an unfinalized hash reduced or
// compared over small candidate sets correlates across permuted inputs.
func phaseRank(mid string, p peer.ID) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(mid))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(p))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

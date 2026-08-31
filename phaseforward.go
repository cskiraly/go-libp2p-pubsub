package pubsub

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"
	"sync/atomic"
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
		} else if gs.announcedHolder(mid, p) {
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

// --- F1 withholding injection (measurement-plan section 15) ---

// WithIWantWithholding makes this node answer IWANTs never (delay 0 = silent) or whole
// after a fixed delay. Announcing is untouched -- IHAVE, IDONTWANT and mesh behaviour all
// continue -- which is the adversarial announce-without-serving case; a crashed peer is
// F3's job. The counter, when non-nil, records the ids withheld or delayed, so injected
// exposure is verified rather than assumed.
func WithIWantWithholding(delay time.Duration, withheld *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant withholding requires a gossipsub router")
		}
		if delay < 0 {
			return fmt.Errorf("iwant withholding delay must be zero (silent) or positive (slow)")
		}
		gs.withholdServe = true
		gs.withholdDelay = delay
		gs.withheldServes = withheld
		return nil
	}
}

// withholdIWantServe applies the injection to a built reply: swallow it (silent) or send
// it whole after the delay, on pubsub's own goroutine like every router action.
func (gs *GossipSubRouter) withholdIWantServe(p peer.ID, msgs []*pb.Message) {
	if gs.withheldServes != nil {
		gs.withheldServes.Add(int64(len(msgs)))
	}
	if gs.withholdDelay <= 0 {
		return
	}
	time.AfterFunc(gs.withholdDelay, func() {
		select {
		case gs.p.eval <- func() {
			if _, ok := gs.p.peers[p]; !ok {
				return
			}
			gs.sendRPC(p, rpcWithControl(msgs, nil, nil, nil, nil, nil), false)
		}:
		case <-gs.p.ctx.Done():
		}
	})
}

// --- F2a per-class admission loss (measurement-plan section 15) ---
//
// Models gossipsub's real failure mode -- RPC content shed at queue admission, never
// retransmitted -- one control class at a time, so a run can attribute which mechanism a
// lost class breaks: a lost IDONTWANT costs duplicates, a lost IHAVE/IWANT or data message
// costs liveness. Entries are stripped at doSendRPC, after splitting and piggybacking.
// Decisions are stateless: hash(node seed, class, message id, destination's stable address)
// -- so the schedule is identical across arms that share semantic units, and does not
// depend on arm-specific call order (the section 15 CRN rule).

const (
	lossClassNone uint8 = iota
	lossClassIHave
	lossClassIWant
	lossClassIDontWant
	lossClassData
)

// WithRPCClassLoss drops pct percent of the given class ("ihave", "iwant", "idontwant",
// "data") at queue admission. nodeSeed comes from the harness (fail seed x node index);
// dropped, when non-nil, counts stripped entries so exposure is verified.
func WithRPCClassLoss(class string, pct int, nodeSeed uint64, dropped *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("rpc class loss requires a gossipsub router")
		}
		if pct <= 0 || pct >= 100 {
			return fmt.Errorf("rpc class loss pct must be in (0,100)")
		}
		switch class {
		case "ihave":
			gs.lossClass = lossClassIHave
		case "iwant":
			gs.lossClass = lossClassIWant
		case "idontwant":
			gs.lossClass = lossClassIDontWant
		case "data":
			gs.lossClass = lossClassData
		default:
			return fmt.Errorf("unknown rpc loss class %q", class)
		}
		gs.lossPct = pct
		gs.lossSeed = nodeSeed
		gs.lossDropped = dropped
		return nil
	}
}

// lossDecide is the stateless drop decision for one semantic unit.
func (gs *GossipSubRouter) lossDecide(id string, dst peer.ID) bool {
	h := fnv.New64a()
	_, _ = h.Write([]byte{byte(gs.lossClass)})
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte(gs.stablePeerKey(dst)))
	x := h.Sum64() ^ gs.lossSeed
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return int(x%100) < gs.lossPct
}

// applyClassLoss returns the RPC to send toward dst with the configured class's losses
// applied, or nil when the loss consumed everything the RPC carried.
//
// The input RPC is never mutated. The forwarding path yields one *RPC to every push
// recipient, so an in-place filter would let the first peer's loss decision rewrite what
// every later peer receives -- per-peer loss would silently become correlated loss. When a
// drop occurs, a fresh RPC is built field by field rather than by copying the value:
// pb.RPC embeds protobuf state that carries a mutex.
func (gs *GossipSubRouter) applyClassLoss(rpc *RPC, dst peer.ID) *RPC {
	if gs.lossClass == lossClassNone || rpc == nil {
		return rpc
	}
	dropped := int64(0)
	out := rpc
	switch gs.lossClass {
	case lossClassData:
		if len(rpc.Publish) == 0 {
			return rpc
		}
		kept := make([]*pb.Message, 0, len(rpc.Publish))
		for _, m := range rpc.Publish {
			if gs.lossDecide(gs.p.idGen.ID(&Message{Message: m}), dst) {
				dropped++
				continue
			}
			kept = append(kept, m)
		}
		if dropped == 0 {
			return rpc
		}
		out = &RPC{from: rpc.from}
		out.Subscriptions = rpc.Subscriptions
		out.Control = rpc.Control
		out.Publish = kept
	case lossClassIHave, lossClassIWant, lossClassIDontWant:
		ctl := rpc.GetControl()
		if ctl == nil {
			return rpc
		}
		filterIDs := func(ids []string) ([]string, int64) {
			var n int64
			kept := make([]string, 0, len(ids))
			for _, id := range ids {
				if gs.lossDecide(id, dst) {
					n++
					continue
				}
				kept = append(kept, id)
			}
			return kept, n
		}
		newCtl := &pb.ControlMessage{
			Ihave:     ctl.Ihave,
			Iwant:     ctl.Iwant,
			Graft:     ctl.Graft,
			Prune:     ctl.Prune,
			Idontwant: ctl.Idontwant,
		}
		switch gs.lossClass {
		case lossClassIHave:
			entries := make([]*pb.ControlIHave, len(ctl.Ihave))
			for i, e := range ctl.Ihave {
				kept, n := filterIDs(e.MessageIDs)
				if n == 0 {
					entries[i] = e
					continue
				}
				dropped += n
				entries[i] = &pb.ControlIHave{TopicID: e.TopicID, MessageIDs: kept}
			}
			newCtl.Ihave = entries
		case lossClassIWant:
			entries := make([]*pb.ControlIWant, len(ctl.Iwant))
			for i, e := range ctl.Iwant {
				kept, n := filterIDs(e.MessageIDs)
				if n == 0 {
					entries[i] = e
					continue
				}
				dropped += n
				entries[i] = &pb.ControlIWant{MessageIDs: kept}
			}
			newCtl.Iwant = entries
		case lossClassIDontWant:
			entries := make([]*pb.ControlIDontWant, len(ctl.Idontwant))
			for i, e := range ctl.Idontwant {
				kept, n := filterIDs(e.MessageIDs)
				if n == 0 {
					entries[i] = e
					continue
				}
				dropped += n
				entries[i] = &pb.ControlIDontWant{MessageIDs: kept}
			}
			newCtl.Idontwant = entries
		}
		if dropped == 0 {
			return rpc
		}
		out = &RPC{from: rpc.from}
		out.Subscriptions = rpc.Subscriptions
		out.Publish = rpc.Publish
		out.Control = newCtl
	}
	if gs.lossDropped != nil {
		gs.lossDropped.Add(dropped)
	}
	if len(out.Publish) == 0 && out.GetControl() != nil {
		ctl := out.GetControl()
		empty := true
		for _, e := range ctl.Ihave {
			empty = empty && len(e.MessageIDs) == 0
		}
		for _, e := range ctl.Iwant {
			empty = empty && len(e.MessageIDs) == 0
		}
		for _, e := range ctl.Idontwant {
			empty = empty && len(e.MessageIDs) == 0
		}
		if empty && len(ctl.Graft) == 0 && len(ctl.Prune) == 0 {
			return nil
		}
	}
	return out
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

// --- IWANT discipline (independent of phase forwarding) ---
//
// Stock gossipsub re-requests a message id whenever another peer's IHAVE arrives while an
// earlier IWANT is still in flight: handleIHave only checks seenMessage, which is false until
// the payload lands. On announce-heavy paths that is the dominant duplicate source (measured
// 2.2x received bytes on a pull-only arm). The discipline: at most one outstanding IWANT per
// message id within a window; on expiry the next IHAVE may re-request, so a lost reply costs
// one window rather than liveness.

// WithIWantDiscipline enables one-outstanding-IWANT-per-id with the given expiry window.
// The window must cover a round trip plus the peer's transmission of what was asked -- the
// same sizing rule as any request timeout; sub-RTT values reintroduce the duplicates.
func WithIWantDiscipline(window time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant discipline requires a gossipsub router")
		}
		if window <= 0 {
			return fmt.Errorf("iwant discipline window must be positive")
		}
		gs.iwantWindow = window
		gs.iwantAsked = make(map[string]time.Time)
		return nil
	}
}

// WithIWantHedge lets a second IWANT for the same id go out once `after` has elapsed since
// the first, capping at two outstanding. It is the cheap mitigation for pull-only's
// completion risk under withholding (measurement-plan section 15): a stalled first request
// no longer waits a full window for a fresh announcement to re-open the id. Requires the
// discipline; a later announcer's IHAVE, from a different peer, is what carries the hedge,
// so it naturally lands on a second holder rather than re-asking the silent one. `after`
// should be a round trip or two, well below the window.
func WithIWantHedge(after time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant hedge requires a gossipsub router")
		}
		if after <= 0 {
			return fmt.Errorf("iwant hedge delay must be positive")
		}
		gs.iwantHedge = after
		gs.iwantCount = make(map[string]int)
		return nil
	}
}

// --- Deterministic randomness (common-random-numbers mode for the harness) ---
//
// Gossipsub's mesh draw runs the global math/rand over map-iteration-ordered candidate
// lists, so two runs on the same connectivity graph grow different meshes (Q23: pull-only
// completion spans 0.79-1.37 s across realizations at constant bytes). This mode makes the
// draw a function of the seed: candidate lists are sorted by a run-stable key before a
// Fisher-Yates from a router-local seeded source. Peer IDs are fresh keys every run, so the
// sort key is the peer's first known address -- stable in simnet, where addresses derive
// from the node index. Harness-only; upstream needs none of this.

// WithDeterministicRand replaces the router's shuffle randomness with a seeded source.
func WithDeterministicRand(seed int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("deterministic rand requires a gossipsub router")
		}
		gs.rng = rand.New(rand.NewSource(seed))
		return nil
	}
}

// stablePeerKey orders peers by first known address, falling back to the id.
func (gs *GossipSubRouter) stablePeerKey(p peer.ID) string {
	if addrs := gs.p.host.Peerstore().Addrs(p); len(addrs) > 0 {
		return addrs[0].String()
	}
	return string(p)
}

func (gs *GossipSubRouter) shufflePeers(peers []peer.ID) {
	if gs.rng == nil {
		shufflePeers(peers)
		return
	}
	sort.Slice(peers, func(i, j int) bool {
		return gs.stablePeerKey(peers[i]) < gs.stablePeerKey(peers[j])
	})
	for i := range peers {
		j := gs.rng.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func (gs *GossipSubRouter) shuffleStrings(lst []string) {
	if gs.rng == nil {
		shuffleStrings(lst)
		return
	}
	sort.Strings(lst)
	for i := range lst {
		j := gs.rng.Intn(i + 1)
		lst[i], lst[j] = lst[j], lst[i]
	}
}

func (gs *GossipSubRouter) shufflePeerInfo(peers []*pb.PeerInfo) {
	if gs.rng == nil {
		shufflePeerInfo(peers)
		return
	}
	sort.Slice(peers, func(i, j int) bool {
		return string(peers[i].GetPeerID()) < string(peers[j].GetPeerID())
	})
	for i := range peers {
		j := gs.rng.Intn(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

// WithSendDelay stalls this node's outbound writer per RPC (F2b real queue pressure,
// measurement-plan section 15). Applied to selected nodes by the harness; their rpcQueue
// then overflows naturally through doDropRPC as offered load outpaces the slowed drain.
// Test-only; a production node must never set it.
func WithSendDelay(d time.Duration) Option {
	return func(ps *PubSub) error {
		if d <= 0 {
			return fmt.Errorf("send delay must be positive")
		}
		ps.sendDelay = d
		return nil
	}
}

// --- (N, q, g) request kernel (design-space §10; measurement-plan §15) ---
//
// The inner per-message-ID launch scheduler: keep N *plausible* outstanding requests, each
// declared implausible (and replaceable) at a per-peer quantile F_p⁻¹(q) of that peer's
// response time, clamped to [floor, ceil] with ceil doubling as the hard deadline so a
// withholder times out. Estimation is online and warms across payloads (repeated harness),
// which is the whole reason this cannot be judged single-shot (Q30).
//
// Honest scope, per the external review: this is the *kernel*, not the deployable path. It
// is IHAVE-driven (a replacement is dispatched when the next candidate's IHAVE arrives, not
// from a timer callback — the "otherwise dispatch on IHAVE arrival" fallback used as the
// only trigger); the where-ranking is "a fresh, still-plausible candidate" (LCFS-ish by
// arrival, no reliability/load ranking); node/group admission limits and serve-probability
// are not modelled; sampling is win-biased and post-verification, as in variant B.

// peerQuantile is a Robbins-Monro online estimate of the q-quantile of a peer's response
// time. One float and a count per peer; the step is a fraction of the current estimate.
type peerQuantile struct {
	est time.Duration
	n   int
}

func (pq *peerQuantile) observe(sample time.Duration, q float64) {
	if pq.n == 0 {
		pq.est, pq.n = sample, 1
		return
	}
	step := time.Duration(float64(pq.est) * 0.10)
	if step <= 0 {
		step = time.Millisecond
	}
	if sample <= pq.est {
		pq.est -= time.Duration(float64(step) * (1 - q))
	} else {
		pq.est += time.Duration(float64(step) * q)
	}
	if pq.est < 0 {
		pq.est = 0
	}
	pq.n++
}

type nqgConfig struct {
	n           int
	q           float64
	floor, ceil time.Duration
	minSamples  int
}

// WithRequestNQG enables the (N, q, g) kernel. Distinct from the discipline/hedge modes;
// enable one. dInit seeds the global prior used for cold peers.
func WithRequestNQG(n int, q float64, floor, ceil, dInit time.Duration) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("request-nqg requires a gossipsub router")
		}
		if n < 1 || q <= 0 || q >= 1 || floor <= 0 || ceil < floor || dInit < floor || dInit > ceil {
			return fmt.Errorf("bad request-nqg parameters")
		}
		gs.nqg = &nqgConfig{n: n, q: q, floor: floor, ceil: ceil, minSamples: 8}
		gs.nqgAsked = make(map[string]map[peer.ID]time.Time)
		gs.rttPeer = make(map[peer.ID]*peerQuantile)
		gs.rttGlobal = &peerQuantile{est: dInit, n: 1}
		return nil
	}
}

// nqgTimer is peer p's implausibility timeout: its own quantile once warm, else the global
// prior, clamped to [floor, ceil]. ceil is the hard deadline that fires against silence.
func (gs *GossipSubRouter) nqgTimer(p peer.ID) time.Duration {
	est := gs.rttGlobal.est
	if pq, ok := gs.rttPeer[p]; ok && pq.n >= gs.nqg.minSamples {
		est = pq.est
	}
	if est < gs.nqg.floor {
		est = gs.nqg.floor
	}
	if est > gs.nqg.ceil {
		est = gs.nqg.ceil
	}
	return est
}

// nqgAllowed decides whether to dispatch a request for mid to announcer p: maintain N
// plausible (unexpired) asks, never re-ask a still-plausible peer, drop expired asks (they
// race on, no cancel). Runs on pubsub's goroutine.
func (gs *GossipSubRouter) nqgAllowed(mid string, p peer.ID) bool {
	asks := gs.nqgAsked[mid]
	if asks == nil {
		asks = make(map[peer.ID]time.Time)
		gs.nqgAsked[mid] = asks
	}
	now := time.Now()
	plausible := 0
	for q, at := range asks {
		if now.Sub(at) >= gs.nqgTimer(q) {
			delete(asks, q) // implausible: no longer counts toward N; still on the wire
			continue
		}
		plausible++
	}
	if plausible >= gs.nqg.n {
		return false
	}
	if _, ok := asks[p]; ok {
		return false // already have a plausible request to this peer
	}
	asks[p] = now
	return true
}

// nqgObserve feeds a claim->arrival sample for a served id, if we asked that peer for it.
func (gs *GossipSubRouter) nqgObserve(mid string, from peer.ID) {
	if gs.nqg == nil {
		return
	}
	asks := gs.nqgAsked[mid]
	if asks == nil {
		return
	}
	if at, ok := asks[from]; ok {
		sample := time.Since(at)
		if sample > 0 {
			pq := gs.rttPeer[from]
			if pq == nil {
				pq = &peerQuantile{}
				gs.rttPeer[from] = pq
			}
			pq.observe(sample, gs.nqg.q)
			gs.rttGlobal.observe(sample, gs.nqg.q)
		}
	}
	delete(gs.nqgAsked, mid) // satisfied
}

// clearNqgAsked drops fully-expired id entries at the heartbeat (rttPeer persists — it warms
// across payloads).
func (gs *GossipSubRouter) clearNqgAsked() {
	if gs.nqgAsked == nil {
		return
	}
	now := time.Now()
	for mid, asks := range gs.nqgAsked {
		allOld := true
		for _, at := range asks {
			if now.Sub(at) < gs.nqg.ceil {
				allOld = false
				break
			}
		}
		if allOld {
			delete(gs.nqgAsked, mid)
		}
	}
}

// --- Adaptive hedge: two decoupled controllers (measurement-plan section 15) ---
//
// Job 1 sets the hedge delay d by tracking a tail quantile of arrival latency (when to
// hedge); job 2 gates hedging with an AIMD rate e driven by local queue occupancy (how much
// under load). They are separate because d must RISE under heterogeneous latency while e
// must FALL under congestion -- one knob cannot do both. Both update at the heartbeat.
type adaptiveHedge struct {
	// Job 1: quantile-tracked delay.
	d      time.Duration // current hedge delay
	dMin   time.Duration
	dMax   time.Duration
	target float64       // desired hedge-launch rate (e.g. 0.05 => d tracks p95)
	eta    time.Duration // Robbins-Monro step
	// Job 2: AIMD hedge-firing probability.
	e     float64 // in (0,1]
	aiAdd float64 // additive increase per heartbeat
	aiMul float64 // multiplicative decrease factor (<1)
	qHigh float64 // queue-occupancy fraction that triggers the decrease
	// Per-epoch counters, reset each heartbeat.
	launched int // hedges fired this epoch (job-1 numerator)
	eligible int // ids that reached d this epoch (job-1 denominator)
	// Observability: did Job 2 ever engage, and how deep did the queue get? Scaled x1000
	// into the optional atomics so the harness can log them (min e, max occupancy seen).
	minE   *atomic.Int64
	maxOcc *atomic.Int64
}

// WithAdaptiveHedge turns the fixed hedge delay into the two-controller form. Requires the
// discipline. Sensible defaults; the harness overrides target/bounds by env.
func WithAdaptiveHedge(dInit, dMin, dMax time.Duration, target, qHigh float64, minE, maxOcc *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("adaptive hedge requires a gossipsub router")
		}
		if target <= 0 || target >= 1 || dMin <= 0 || dMax < dMin || dInit < dMin || dInit > dMax || qHigh <= 0 || qHigh >= 1 {
			return fmt.Errorf("bad adaptive hedge parameters")
		}
		gs.adaptHedge = &adaptiveHedge{
			d: dInit, dMin: dMin, dMax: dMax, target: target, eta: dMin / 4,
			e: 1.0, aiAdd: 0.1, aiMul: 0.5, qHigh: qHigh,
			minE: minE, maxOcc: maxOcc,
		}
		if minE != nil {
			minE.Store(1000) // e starts at 1.0
		}
		gs.iwantCount = make(map[string]int)
		return nil
	}
}

// hedgeAllowedAdaptive is the adaptive counterpart of the fixed-delay hedge check. Called
// from iwantAllowed when a request for mid is already outstanding.
func (gs *GossipSubRouter) hedgeAllowedAdaptive(mid string, elapsed time.Duration) bool {
	a := gs.adaptHedge
	if elapsed < a.d {
		return false // job 1: not yet at the delay
	}
	if gs.iwantCount[mid] >= 2 {
		return false // cap at two outstanding
	}
	a.eligible++
	// job 2: fire only with probability e (the AIMD budget). Use the seeded rng if present
	// so the decision is reproducible under SEGMENT_DET_RAND, else the global source.
	var r float64
	if gs.rng != nil {
		r = gs.rng.Float64()
	} else {
		r = rand.Float64()
	}
	if r >= a.e {
		return false
	}
	a.launched++
	gs.iwantCount[mid]++
	return true
}

// updateAdaptiveHedge runs both control loops once, at the heartbeat.
func (gs *GossipSubRouter) updateAdaptiveHedge() {
	a := gs.adaptHedge
	if a == nil {
		return
	}
	// Job 1: Robbins-Monro on the hedge-launch rate. H is the observed rate this epoch; nudge
	// d toward the delay at which H == target. Too many launches => d too small => raise it.
	if a.eligible > 0 {
		h := float64(a.launched) / float64(a.eligible)
		d := a.d + time.Duration((h-a.target)*float64(a.eta)*4)
		if d < a.dMin {
			d = a.dMin
		}
		if d > a.dMax {
			d = a.dMax
		}
		a.d = d
	}
	a.launched, a.eligible = 0, 0
	// Job 2: AIMD on e, signal = max outbound-queue occupancy across this node's peers.
	occ := 0.0
	for _, q := range gs.p.peers {
		l, c := q.Occupancy()
		if c > 0 {
			if f := float64(l) / float64(c); f > occ {
				occ = f
			}
		}
	}
	if occ >= a.qHigh {
		a.e *= a.aiMul // multiplicative decrease: back off fast
	} else {
		a.e += a.aiAdd // additive increase: creep back
		if a.e > 1.0 {
			a.e = 1.0
		}
	}
	// Observability: record how far e fell and how deep the queue ever got.
	if a.minE != nil {
		if v := int64(a.e * 1000); v < a.minE.Load() {
			a.minE.Store(v)
		}
	}
	if a.maxOcc != nil {
		if v := int64(occ * 1000); v > a.maxOcc.Load() {
			a.maxOcc.Store(v)
		}
	}
}

// WithIWantBudget enables the upstream-PR-625 rule instead: a per-message-id IWANT budget
// that the heartbeat resets wholesale and message arrival clears. Same intent as the
// discipline, different retry clock -- the budget re-opens at the next heartbeat *boundary*,
// the discipline's window a fixed interval after the request. Built for a measured
// head-to-head; enable one or the other, not both.
func WithIWantBudget(n int) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("iwant budget requires a gossipsub router")
		}
		if n <= 0 {
			return fmt.Errorf("iwant budget must be positive")
		}
		gs.iwantBudgetMax = n
		gs.iwantBudget = make(map[string]int)
		return nil
	}
}

// iwantBudgetAllowed mirrors PR 625's allowedIWantCount check, consuming one unit.
func (gs *GossipSubRouter) iwantBudgetAllowed(mid string) bool {
	if gs.iwantBudget == nil {
		return true
	}
	n, ok := gs.iwantBudget[mid]
	if !ok {
		n = gs.iwantBudgetMax
	}
	if n <= 0 {
		return false
	}
	gs.iwantBudget[mid] = n - 1
	return true
}

// resetIWantBudget discards all budget state; called from the heartbeat (PR 625 semantics).
func (gs *GossipSubRouter) resetIWantBudget() {
	if len(gs.iwantBudget) > 0 {
		gs.iwantBudget = make(map[string]int)
	}
}

// clearIWantBudgetFor drops the id on message arrival (PR 625's Preprocess delete).
func (gs *GossipSubRouter) clearIWantBudgetFor(mid string) {
	if gs.iwantBudget != nil {
		delete(gs.iwantBudget, mid)
	}
}

// iwantAllowed reports whether an IWANT for mid may be issued now, recording it if so.
// Runs on pubsub's goroutine, like every router map.
func (gs *GossipSubRouter) iwantAllowed(mid string) bool {
	if gs.iwantAsked == nil {
		return true
	}
	if at, ok := gs.iwantAsked[mid]; ok && time.Since(at) < gs.iwantWindow {
		elapsed := time.Since(at)
		// Adaptive hedge (two-controller form) takes precedence over the fixed one.
		if gs.adaptHedge != nil {
			return gs.hedgeAllowedAdaptive(mid, elapsed)
		}
		// Fixed hedge: once the first ask has been outstanding for iwantHedge, permit exactly
		// one more (total two) without extending the window. The elapsed check keeps a burst
		// of near-simultaneous announcers from firing two requests at once.
		if gs.iwantHedge > 0 && elapsed >= gs.iwantHedge && gs.iwantCount[mid] < 2 {
			gs.iwantCount[mid]++
			return true
		}
		return false
	}
	gs.iwantAsked[mid] = time.Now()
	if gs.iwantCount != nil {
		gs.iwantCount[mid] = 1
	}
	return true
}

// clearIWantAsked drops expired outstanding-request records; called from the heartbeat.
func (gs *GossipSubRouter) clearIWantAsked() {
	if gs.iwantAsked == nil {
		return
	}
	for mid, at := range gs.iwantAsked {
		if time.Since(at) >= gs.iwantWindow {
			delete(gs.iwantAsked, mid)
			if gs.iwantCount != nil {
				delete(gs.iwantCount, mid)
			}
		}
	}
}

// --- Announce-fed holder tracking (for phase forwarding) ---
//
// The phase decay's holder estimate is IDONTWANT-fed, and IDONTWANT only reaches mesh peers
// -- which is exactly what broke the first meshless attempt (Q21): off-mesh candidates never
// registered as holders, so the decay never fired. Announces are the wider signal: a peer
// that sent us an IHAVE for a message id holds it. Opt-in, because it changes the measured
// behaviour of the existing phase arms.

// WithPhaseAnnounceHolders feeds received IHAVEs into phase forwarding's holder estimate.
func WithPhaseAnnounceHolders() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("announce holders requires a gossipsub router")
		}
		if gs.phaseForward == nil {
			return fmt.Errorf("announce holders requires WithPhaseForwarding first")
		}
		gs.phaseHolders = make(map[string]*phaseHolderEntry)
		return nil
	}
}

type phaseHolderEntry struct {
	peers map[peer.ID]struct{}
	added time.Time
}

// noteAnnouncedHolder records that p announced mid. Bounded by the heartbeat pruning below
// and by gossipsub's own caps on how many IHAVE ids a peer may advertise per heartbeat.
func (gs *GossipSubRouter) noteAnnouncedHolder(mid string, p peer.ID) {
	if gs.phaseHolders == nil {
		return
	}
	e, ok := gs.phaseHolders[mid]
	if !ok {
		e = &phaseHolderEntry{peers: make(map[peer.ID]struct{}), added: time.Now()}
		gs.phaseHolders[mid] = e
	}
	e.peers[p] = struct{}{}
}

// announcedHolder reports whether p is known (via IHAVE) to hold mid.
func (gs *GossipSubRouter) announcedHolder(mid string, p peer.ID) bool {
	if gs.phaseHolders == nil {
		return false
	}
	e, ok := gs.phaseHolders[mid]
	if !ok {
		return false
	}
	_, held := e.peers[p]
	return held
}

// clearPhaseHolders drops holder entries older than the retention window; called from the
// heartbeat. Six heartbeats matches the mcache history a message stays serveable for.
func (gs *GossipSubRouter) clearPhaseHolders() {
	if gs.phaseHolders == nil {
		return
	}
	cutoff := 6 * gs.params.HeartbeatInterval
	for mid, e := range gs.phaseHolders {
		if time.Since(e.added) >= cutoff {
			delete(gs.phaseHolders, mid)
		}
	}
}

// --- Application request gate ---
//
// A receiver in announce-and-pull decides whether to ask, so it can decline what it cannot yet
// authenticate -- for segmented payloads bound to a consensus commitment, that means declining an
// announcement whose authorizing block has not arrived. The decision point already exists and is
// already policy-driven; what is missing is a place for the *application* to answer, since only it
// knows what a message id claims and whether that claim is currently satisfiable.
//
// Deliberately opaque: the gate sees the peer, the topic and the raw message id and nothing else.
// Every segment-specific notion -- how an id encodes its structural claim, which blocks are
// installed, what aggregate per-claim caps apply -- stays in the application, so the router carries
// one generic predicate rather than a protocol.

// RequestGate is an application veto on requesting announced message ids.
//
// The two methods exist because a one-shot boolean cannot both decide and account. Allow runs ahead
// of every request policy, and those policies may still refuse an id the gate approved; a gate that
// charged a budget inside Allow would spend it on ids no IWANT ever carried -- the same phantom-commit
// bug that made selection and dispatch one set in the first place. So Allow must be pure, and
// Committed reports exactly the ids that were dispatched.
type RequestGate interface {
	// Allow reports whether p's announcement of mid on topic may be requested now. It MUST NOT
	// record state: a false answer has to leave the id exactly as requestable as it was, since an
	// application that declines for want of authority will want the same id once authority arrives.
	Allow(p peer.ID, topic, mid string) bool
	// Committed reports the ids actually dispatched in an IWANT to p. This is the only place an
	// application may charge per-claim budgets or record provenance.
	Committed(p peer.ID, mids []string)
}

// RequestDeferral is the router-side ledger of announcements a gate declined, plus the trigger that
// replays them.
//
// The ledger lives in the router and the trigger belongs to the application, and the split is not
// arbitrary. Replaying a declined announcement needs the announcer's identity, the per-peer request
// allowance, the outstanding-request ledgers and the promise tracker -- all router state. Knowing
// *when* the answer changed needs the block, which only the application has. So the router
// remembers who offered what, and the application says when to ask again.
//
// Bounded on three axes, because a declined announcement is attacker-suppliable: total entries,
// announcers per entry, and age. Beyond the mcache's serve window an announcer could not serve the
// message anyway, so retention past it would be pure memory.
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

// Replay asks the router to re-offer every deferred announcement to the gate and request those now
// permitted. Safe to call from any goroutine and from a timer; the work runs on the router's loop.
//
// This is the event the design turns on. Without it, recovery waits for heartbeat gossip, which
// fires once per heartbeat to a random subset of *non-mesh* peers -- it excludes mesh peers on the
// assumption that they are being pushed to, an assumption a receiver-side gate falsifies. Measured
// at n=30, that made the cost of gating flat in the authority delay and quantized to the heartbeat.
func (d *RequestDeferral) Replay() {
	if d == nil || d.replay == nil {
		return
	}
	d.replay()
}

// WithRequestGate installs a request gate, and optionally the deferral ledger that makes a decline
// recoverable. Returning false from Allow declines the id *without consuming any request state*, so
// the same id can be requested later when the answer changes.
//
// Declining is not the same as forgetting, and nothing re-delivers a declined announcement on its
// own: heartbeat gossip excludes mesh peers and the phase policy's immediate IHAVEs are one-shot.
// Passing a nil deferral is therefore a deliberate choice to measure the no-retry baseline.
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

// deferredEntry is one declined announcement: who offered it, on what topic, and when it was first
// declined. `at` is set once and never refreshed, so a repeatedly re-announced id still expires.
type deferredEntry struct {
	topic string
	peers []peer.ID
	at    time.Time
}

// maxDeferredAnnouncers caps how many peers are remembered per declined id. Fallback across
// announcers matters (the first may disconnect or refuse to serve) but the tail is worthless.
const maxDeferredAnnouncers = 4

// noteDeclined records that p offered mid on topic and the gate said no. Called from the selection
// path, where the peer and topic are both in hand.
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

// replayDeferred re-offers deferred ids to the gate and requests those now permitted. Runs on the
// router's loop.
//
// It goes through the ordinary selection path rather than around it, so per-peer allowances, the
// outstanding-request discipline and the promise tracker all apply exactly as they would to a fresh
// announcement. A replay that bypassed them would be a second, unaccounted request channel.
func (gs *GossipSubRouter) replayDeferred() {
	if gs.deferred == nil || gs.requestGate == nil {
		return
	}
	ttl := time.Duration(gs.params.HistoryLength) * gs.params.HeartbeatInterval
	announcersOf := make(map[string][]peer.ID, len(gs.deferred))
	byPeer := make(map[peer.ID]struct{})
	topicOf := make(map[string]string, len(gs.deferred))
	for mid, e := range gs.deferred {
		// Expired, or it actually arrived while we waited -- either way stop tracking it.
		//
		// "Arrived" means *delivered*, not *seen*. A message is marked seen before application
		// validation, so an id the application declined and did not retain is seen while having
		// reached nobody: testing seenMessage here discarded exactly the ids most in need of a
		// retry, and did so silently. deliveredMessages is the distinction the library already
		// keeps for this reason.
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
			byPeer[pid] = struct{}{}
		}
	}
	// Assign each id to ONE announcer, balancing across announcers, and only then request.
	//
	// The obvious implementation -- walk the peers and ask each for everything it announced -- is
	// strongly source-concentrating: the peers that announced during the closed window are exactly
	// the ones that get asked for the whole group, so the replay swamps a handful of uplinks. That
	// is worse than not retrying at all in a pull-only arm, where every byte moves by IWANT
	// (measured: pull-only completion 2.2s -> 2.7s across three seeds, with duplicate counts and
	// per-node bytes flat, so it was load placement rather than volume). Heartbeat rediscovery
	// happens to draw a fresh random subset each round and is therefore source-diversifying by
	// accident; a replay has to be so on purpose.
	ids := make([]string, 0, len(byPeer))
	for mid := range topicOf {
		if len(announcersOf[mid]) > 0 {
			ids = append(ids, mid)
		}
	}
	gs.shuffleStrings(ids)
	load := make(map[peer.ID]int, len(byPeer))
	assigned := make(map[peer.ID][]string, len(byPeer))
	for _, mid := range ids {
		cands := announcersOf[mid]
		best, bestLoad := peer.ID(""), -1
		for _, pid := range cands {
			if l := load[pid]; bestLoad < 0 || l < bestLoad {
				best, bestLoad = pid, l
			}
		}
		if bestLoad < 0 {
			continue
		}
		load[best]++
		assigned[best] = append(assigned[best], mid)
	}
	for pid, mids := range assigned {
		allowance := gs.params.MaxIHaveLength - gs.iasked[pid]
		if allowance <= 0 {
			continue
		}
		// Shuffle before selection, not after: the policies consulted inside record state as a side
		// effect of allowing an id, so selecting freely and truncating would leave phantom entries.
		gs.shuffleStrings(mids)
		lst := gs.selectIWants(mids, topicOf, pid, allowance)
		if len(lst) == 0 {
			continue
		}
		gs.iasked[pid] += len(lst)
		gs.gossipTracer.AddPromise(pid, lst)
		gs.requestGate.Committed(pid, lst)
		gs.sendRPC(pid, rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: lst}}, nil, nil, nil), false)
		for _, mid := range lst {
			delete(gs.deferred, mid)
		}
	}
}

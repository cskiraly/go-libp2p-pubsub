package pubsub

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// --- Passive request observation (measurement only) ---
//
// The router already holds everything a request-side policy could act on: when each IWANT
// left and to whom, and when and from whom the id then arrived. Nothing recorded it, so the
// service-time distribution behind the discipline's clocks, the duplicates a hedge buys and the
// requests still open at a deadline had never been reported. The ledger records it and changes
// nothing: no decision reads it. Single-payload form, like the tail flag: entries never expire,
// so it is bounded by the ids a node requests over its life. Harness only.

// RequestAsk is one IWANT for an id: the peer asked and when the request was dispatched — after
// the gate and every request policy admitted it, as it entered the peer's outbound queue.
type RequestAsk struct {
	Peer peer.ID
	At   time.Time
	// Dropped: the RPC carrying the ask was refused by the peer's full outbound queue; the
	// control is kept for piggybacking on the next RPC to that peer, so the ask is late, not lost.
	Dropped bool
}

// RequestRecord is one requested id: its asks in dispatch order and how it was resolved.
type RequestRecord struct {
	ID   string
	Asks []RequestAsk
	// Arrived is the first arrival of the id, from From; zero while outstanding. Solicited says
	// whether From had been asked, i.e. whether the arrival answered a request or a push overtook
	// it. Dups counts later arrivals from peers that were asked: the copies a hedge or a retry
	// bought.
	Arrived   time.Time
	From      peer.ID
	Solicited bool
	Dups      int
}

// RequestLedger is a node's request history as of the snapshot. Unasked counts first arrivals
// of ids the node never requested: plain pushes.
type RequestLedger struct {
	Records []RequestRecord
	Unasked int
}

// WithRequestObservation turns the ledger on. It records; it never decides.
func WithRequestObservation() Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return errRequestObservationRouter
		}
		gs.reqLedger = make(map[string]*RequestRecord)
		return nil
	}
}

type requestObservationError struct{}

func (requestObservationError) Error() string {
	return "request observation requires a gossipsub router"
}

var errRequestObservationRouter error = requestObservationError{}

// noteIWantSent records a dispatched IWANT. Router goroutine; called beside the promise tracker
// at every dispatch site, so a request the tracer promises is a request the ledger holds.
func (gs *GossipSubRouter) noteIWantSent(p peer.ID, mids []string) {
	if gs.tailAsks != nil { // the tail hedge's lifetime count per id (tailbounds.go)
		for _, mid := range mids {
			gs.tailAsks[mid]++
		}
	}
	if gs.reqLedger == nil {
		return
	}
	now := time.Now()
	for _, mid := range mids {
		r := gs.reqLedger[mid]
		if r == nil {
			r = &RequestRecord{ID: mid}
			gs.reqLedger[mid] = r
		}
		r.Asks = append(r.Asks, RequestAsk{Peer: p, At: now})
	}
}

// noteIWantDropped marks the latest ask of each mid to p as dropped at the queue. Router
// goroutine, from doDropRPC.
func (gs *GossipSubRouter) noteIWantDropped(p peer.ID, mids []string) {
	if gs.reqLedger == nil {
		return
	}
	for _, mid := range mids {
		r := gs.reqLedger[mid]
		if r == nil {
			continue
		}
		for i := len(r.Asks) - 1; i >= 0; i-- {
			if r.Asks[i].Peer == p {
				r.Asks[i].Dropped = true
				break
			}
		}
	}
}

// noteArrival records the first arrival of mid. Router goroutine, from Preprocess.
func (gs *GossipSubRouter) noteArrival(mid string, from peer.ID) {
	if gs.reqLedger == nil {
		return
	}
	r := gs.reqLedger[mid]
	if r == nil {
		gs.reqUnasked++
		return
	}
	if !r.Arrived.IsZero() {
		return
	}
	r.Arrived = time.Now()
	r.From = from
	for _, a := range r.Asks {
		if a.Peer == from {
			r.Solicited = true
			break
		}
	}
}

// noteDuplicate records a later arrival of an asked id from a peer that was asked for it.
func (gs *GossipSubRouter) noteDuplicate(mid string, from peer.ID) {
	if gs.reqLedger == nil {
		return
	}
	r := gs.reqLedger[mid]
	if r == nil {
		return
	}
	for _, a := range r.Asks {
		if a.Peer == from {
			r.Dups++
			return
		}
	}
}

// RequestLedger snapshots the ledger on the router goroutine. Nil when observation is off or the
// router has stopped.
func (p *PubSub) RequestLedger() *RequestLedger {
	gs, ok := p.rt.(*GossipSubRouter)
	if !ok || gs.reqLedger == nil {
		return nil
	}
	out := make(chan *RequestLedger, 1)
	select {
	case p.eval <- func() {
		l := &RequestLedger{Records: make([]RequestRecord, 0, len(gs.reqLedger)), Unasked: gs.reqUnasked}
		for _, r := range gs.reqLedger {
			c := *r
			c.Asks = append([]RequestAsk(nil), r.Asks...)
			l.Records = append(l.Records, c)
		}
		out <- l
	}:
	case <-p.ctx.Done():
		return nil
	}
	select {
	case l := <-out:
		return l
	case <-p.ctx.Done():
		return nil
	}
}

// OutboundStat is one peer's outbound queue as observed: bytes queued and not yet handed to the
// writer at the snapshot, their peak, and the writer's time blocked in the stream write (under
// synctest, virtual time: the transport's back-pressure, not CPU).
type OutboundStat struct {
	QueuedBytes     int64
	PeakQueuedBytes int64
	WriteBlocked    time.Duration
	Writes          int64
	MaxWrite        time.Duration
}

// OutboundStats snapshots every peer's outbound queue, on the router goroutine (the peer table
// is its). Nil once the router has stopped.
func (p *PubSub) OutboundStats() map[peer.ID]OutboundStat {
	out := make(chan map[peer.ID]OutboundStat, 1)
	select {
	case p.eval <- func() {
		m := make(map[peer.ID]OutboundStat, len(p.peers))
		for pid, q := range p.peers {
			m[pid] = q.outboundStat()
		}
		out <- m
	}:
	case <-p.ctx.Done():
		return nil
	}
	select {
	case m := <-out:
		return m
	case <-p.ctx.Done():
		return nil
	}
}

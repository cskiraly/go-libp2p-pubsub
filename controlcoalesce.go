package pubsub

import (
	"fmt"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// --- Control coalescing at the emission layer (design-space.md) ---
//
// Segmenting a payload multiplies every per-message control path by the number of segments: the
// phase announce emits one IHAVE RPC per (message, peer), and IDONTWANT and IWANT each batch only
// within one call, which under phase forwarding happens once per message because messages arrive
// one per RPC. Measured at 500 nodes, a 32-segment group puts up to 95 control RPCs on one link in
// one heartbeat, against gossipsub's MaxIHaveMessages of 10, which whole-message gossip never
// approaches (max 3). The receiver counts RPCs, so what matters is how many RPCs carry the control,
// not how much control there is.
//
// The fix that covers every class at once is here rather than in any one sender: when a
// control-only RPC is about to be queued for a peer and a control-only RPC is already waiting in
// that peer's queue, fold the new control into the waiting one instead of queuing a second. It is
// free when the link is idle — an empty queue coalesces nothing and the RPC goes out exactly as
// before — and it batches in proportion to how backed up the link is, which is when the limit
// bites. No timer, and no delay added to anything: the merged control leaves no later than the RPC
// it joined, which was already ahead of it.

// WithControlCoalescing folds a control-only RPC into the control-only RPC already queued for that
// peer, when there is one. merged, when non-nil, counts the RPCs that were folded away.
func WithControlCoalescing(merged *atomic.Int64) Option {
	return func(ps *PubSub) error {
		gs, ok := ps.rt.(*GossipSubRouter)
		if !ok {
			return fmt.Errorf("control coalescing requires a gossipsub router")
		}
		gs.coalesceControl = true
		gs.coalesced = merged
		return nil
	}
}

// controlOnly reports whether the RPC carries control and nothing else. A published message must
// never be folded: the data RPCs are yielded as one shared object to every recipient of a message,
// so touching one would edit another peer's send, and their order is the diffusion itself.
func controlOnly(rpc *RPC) bool {
	return rpc != nil && len(rpc.Publish) == 0 && len(rpc.Subscriptions) == 0 && rpc.Control != nil
}

// mergedControl returns a fresh RPC carrying dst's payload untouched and dst's control followed by
// src's. It never mutates either: an RPC already in a queue may be observed by the writer goroutine
// the moment the queue mutex is released, and the control objects are shared with the router's
// pending maps. The clone is shallow on purpose — the published messages are shared pointers that
// nothing mutates, so folding control onto a queued *data* RPC costs one small allocation and not a
// copy of the payload. That case is the one that matters: on a link that keeps up, the RPC already
// waiting is usually a segment push rather than another control RPC.
func mergedControl(dst, src *RPC) *RPC {
	out := &RPC{RPC: pb.RPC{Publish: dst.Publish, Subscriptions: dst.Subscriptions, Control: &pb.ControlMessage{}}, from: dst.from}
	for _, c := range []*pb.ControlMessage{dst.GetControl(), src.GetControl()} {
		if c == nil {
			continue
		}
		out.Control.Ihave = append(out.Control.Ihave, c.GetIhave()...)
		out.Control.Iwant = append(out.Control.Iwant, c.GetIwant()...)
		out.Control.Graft = append(out.Control.Graft, c.GetGraft()...)
		out.Control.Prune = append(out.Control.Prune, c.GetPrune()...)
		out.Control.Idontwant = append(out.Control.Idontwant, c.GetIdontwant()...)
	}
	return out
}

// coalesceInto folds rpc into the newest queued control-only RPC on the same lane, if one is there
// and the result still fits the wire limits. Returns the queued-byte delta and whether it merged.
//
// Lanes are kept apart on purpose: an urgent IDONTWANT folded into a normal-lane RPC would be
// delayed behind whatever the normal lane is draining, which is the opposite of what urgency means.
func (q *rpcQueue) coalesceInto(rpc *RPC, urgent bool, limit, ctlLimit int) (int64, bool) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	if q.closed {
		return 0, false
	}
	var cur *RPC
	var slot *rankedRPC
	if urgent {
		if n := len(q.queue.priority); n > 0 {
			cur = q.queue.priority[n-1]
		}
	} else if n := len(q.queue.normal); n > 0 {
		slot = &q.queue.normal[n-1]
		cur = slot.rpc
	}
	// cur may be a data RPC: the clone keeps its payload by reference. What it may not be is
	// nothing, or an RPC with no room left.
	if cur == nil {
		return 0, false
	}
	out := mergedControl(cur, rpc)
	if out.exceedsSizeLimits(limit, ctlLimit) {
		return 0, false
	}
	out.size = proto.Size(&out.RPC)
	delta := int64(out.size - cur.size)
	if urgent {
		q.queue.priority[len(q.queue.priority)-1] = out
	} else {
		slot.rpc = out
	}
	q.queuedBytes += delta
	if q.queuedBytes > q.peakQueued {
		q.peakQueued = q.queuedBytes
	}
	return delta, true
}

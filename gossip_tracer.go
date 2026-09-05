package pubsub

import (
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// gossipTracer is an internal tracer that tracks IWANT requests in order to penalize
// peers who don't follow up on IWANT requests after an IHAVE advertisement.
// The tracking of promises is probabilistic to avoid using too much memory.
type gossipTracer struct {
	sync.Mutex

	idGen *msgIDGenerator

	followUpTime time.Duration

	// promises for messages by message ID; for each message tracked, we track the promise
	// expiration time for each peer.
	promises map[string]map[peer.ID]time.Time
	// promises for each peer; for each peer, we track the promised message IDs.
	// this index allows us to quickly void promises when a peer is throttled.
	peerPromises map[peer.ID]map[string]struct{}
}

// promiseTrace identifies the promise, if any, added by one AddPromise call.
// AddPromise samples one ID and preserves a promise that already exists, so
// rollback must distinguish a new promise from an older one for the same peer.
type promiseTrace struct {
	mid   string
	added bool
}

func newGossipTracer() *gossipTracer {
	return &gossipTracer{
		idGen:        newMsgIdGenerator(),
		promises:     make(map[string]map[peer.ID]time.Time),
		peerPromises: make(map[peer.ID]map[string]struct{}),
	}
}

func (gt *gossipTracer) Start(gs *GossipSubRouter) {
	if gt == nil {
		return
	}

	gt.idGen = gs.p.idGen
	gt.followUpTime = gs.params.IWantFollowupTime
}

// track a promise to deliver a message from a list of msgIDs we are requesting
func (gt *gossipTracer) AddPromise(p peer.ID, msgIDs []string) promiseTrace {
	if gt == nil {
		return promiseTrace{}
	}

	idx := rand.Intn(len(msgIDs))
	mid := msgIDs[idx]

	gt.Lock()
	defer gt.Unlock()

	promises, ok := gt.promises[mid]
	if !ok {
		promises = make(map[peer.ID]time.Time)
		gt.promises[mid] = promises
	}

	if _, ok = promises[p]; ok {
		return promiseTrace{}
	}

	promises[p] = time.Now().Add(gt.followUpTime)
	peerPromises, ok := gt.peerPromises[p]
	if !ok {
		peerPromises = make(map[string]struct{})
		gt.peerPromises[p] = peerPromises
	}
	peerPromises[mid] = struct{}{}
	return promiseTrace{mid: mid, added: true}
}

// dropPromise forgets the promise added by one AddPromise call. It exists for requests that
// were never transmitted: sendRPC can refuse an RPC at queue admission, and a promise
// recorded for an IWANT that never left this node would score the peer for failing to answer
// a question it was never asked.
func (gt *gossipTracer) dropPromise(p peer.ID, traced promiseTrace) {
	if gt == nil || !traced.added {
		return
	}

	gt.Lock()
	defer gt.Unlock()

	promises, ok := gt.promises[traced.mid]
	if !ok {
		return
	}
	delete(promises, p)
	if len(promises) == 0 {
		delete(gt.promises, traced.mid)
	}
	if peerPromises, ok := gt.peerPromises[p]; ok {
		delete(peerPromises, traced.mid)
		if len(peerPromises) == 0 {
			delete(gt.peerPromises, p)
		}
	}
}

// returns the number of broken promises for each peer who didn't follow up
// on an IWANT request.
func (gt *gossipTracer) GetBrokenPromises() map[peer.ID]int {
	if gt == nil {
		return nil
	}

	gt.Lock()
	defer gt.Unlock()

	var res map[peer.ID]int
	now := time.Now()

	// find broken promises from peers
	for mid, promises := range gt.promises {
		for p, expire := range promises {
			if expire.Before(now) {
				if res == nil {
					res = make(map[peer.ID]int)
				}
				res[p]++

				delete(promises, p)

				peerPromises := gt.peerPromises[p]
				delete(peerPromises, mid)
				if len(peerPromises) == 0 {
					delete(gt.peerPromises, p)
				}
			}
		}

		if len(promises) == 0 {
			delete(gt.promises, mid)
		}
	}

	return res
}

var _ RawTracer = (*gossipTracer)(nil)

func (gt *gossipTracer) fulfillPromise(msg *Message) {
	mid := gt.idGen.ID(msg)

	gt.Lock()
	defer gt.Unlock()

	promises, ok := gt.promises[mid]
	if !ok {
		return
	}
	delete(gt.promises, mid)

	// delete the promise for all peers that promised it, as they have no way to fulfill it.
	for p := range promises {
		peerPromises, ok := gt.peerPromises[p]
		if ok {
			delete(peerPromises, mid)
			if len(peerPromises) == 0 {
				delete(gt.peerPromises, p)
			}
		}
	}
}

func (gt *gossipTracer) DeliverMessage(msg *Message) {
	// someone delivered a message, fulfill promises for it
	gt.fulfillPromise(msg)
}

func (gt *gossipTracer) RejectMessage(msg *Message, reason string) {
	// A message got rejected, so we can fulfill promises and let the score penalty apply
	// from invalid message delivery.
	// We do take exception and apply promise penalty regardless in the following cases, where
	// the peer delivered an obviously invalid message.
	switch reason {
	case RejectMissingSignature:
		return
	case RejectInvalidSignature:
		return
	}

	gt.fulfillPromise(msg)
}

func (gt *gossipTracer) ValidateMessage(msg *Message) {
	// we consider the promise fulfilled as soon as the message begins validation
	// if it was a case of signature issue it would have been rejected immediately
	// without triggering the Validate trace
	gt.fulfillPromise(msg)
}

func (gt *gossipTracer) OnNewOutboundStream(p peer.ID, proto protocol.ID) {}
func (gt *gossipTracer) OnClosedOutboundStream(p peer.ID)                 {}
func (gt *gossipTracer) Join(topic string)                                {}
func (gt *gossipTracer) Leave(topic string)                               {}
func (gt *gossipTracer) Graft(p peer.ID, topic string)                    {}
func (gt *gossipTracer) Prune(p peer.ID, topic string)                    {}
func (gt *gossipTracer) DuplicateMessage(msg *Message)                    {}
func (gt *gossipTracer) RecvRPC(rpc *RPC)                                 {}
func (gt *gossipTracer) SendRPC(rpc *RPC, p peer.ID)                      {}
func (gt *gossipTracer) DropRPC(rpc *RPC, p peer.ID)                      {}
func (gt *gossipTracer) UndeliverableMessage(msg *Message)                {}

func (gt *gossipTracer) ThrottlePeer(p peer.ID) {
	gt.Lock()
	defer gt.Unlock()

	peerPromises, ok := gt.peerPromises[p]
	if !ok {
		return
	}

	for mid := range peerPromises {
		promises := gt.promises[mid]
		delete(promises, p)
		if len(promises) == 0 {
			delete(gt.promises, mid)
		}
	}

	delete(gt.peerPromises, p)
}

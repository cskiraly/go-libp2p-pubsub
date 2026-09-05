package pubsub

import (
	"context"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// popNext returns the next RPC queued to a peer, or fails the test.
func popNext(t *testing.T, q *rpcQueue) *RPC {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rpc, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("nothing queued: %v", err)
	}
	return rpc
}

// TestIHaveReplyIWantIsUrgent: the IWANT that answers an IHAVE overtakes data already queued to
// the same peer, while pending gossip stays on the normal lane behind that data.
func TestIHaveReplyIWantIsUrgent(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, oq := newRefusalRouter(t, full, open)
	gs.extensions = newExtensionsState(PeerExtensions{}, func(peer.ID) {}, func(p peer.ID, out *RPC, urgent bool) { gs.sendRPC(p, out, urgent) })
	gs.p.peerFilter = DefaultPeerFilter
	gs.mesh = map[string]map[peer.ID]struct{}{"t": {open: {}}}
	gs.peerhave = make(map[peer.ID]int)

	// A pushed segment is already waiting for the writer.
	backlog := rpcWithMessages(&pb.Message{Data: []byte("segment")})
	if err := oq.Push(backlog, false); err != nil {
		t.Fatal(err)
	}
	pendingTopic := "pending"
	gs.gossip = map[peer.ID][]*pb.ControlIHave{
		open: {{TopicID: &pendingTopic, MessageIDs: []string{"pending"}}},
	}

	topic := "t"
	gs.HandleRPC(&RPC{from: open, RPC: pb.RPC{Control: &pb.ControlMessage{
		Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"m"}}},
	}}})
	if got := gs.gossip[open]; len(got) != 1 || got[0].GetMessageIDs()[0] != "pending" {
		t.Fatalf("urgent IWANT drained pending gossip: %v", got)
	}
	gs.flush()

	first := popNext(t, oq)
	if got := first.Control.GetIwant(); len(got) != 1 || got[0].GetMessageIDs()[0] != "m" {
		t.Fatalf("first RPC out is %v, want the IWANT for m ahead of the backlog", first)
	}
	if len(first.Publish) != 0 {
		t.Fatal("the urgent IWANT RPC carries data")
	}
	second := popNext(t, oq)
	if second != backlog {
		t.Fatal("the backlog was not the next RPC out")
	}
	third := popNext(t, oq)
	if got := third.Control.GetIhave(); len(got) != 1 || got[0].GetMessageIDs()[0] != "pending" {
		t.Fatalf("third RPC out carries %v, want the pending IHAVE", got)
	}
}

func TestIHaveReplyIWantWinsLastQueueSlot(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, oq := newRefusalRouter(t, full, open)
	oq.maxSize = 2
	gs.extensions = newExtensionsState(PeerExtensions{}, func(peer.ID) {}, func(p peer.ID, out *RPC, urgent bool) { gs.sendRPC(p, out, urgent) })
	gs.p.peerFilter = DefaultPeerFilter
	gs.params = DefaultGossipSubParams()
	gs.mesh = map[string]map[peer.ID]struct{}{"t": {open: {}}}
	gs.peerhave = make(map[peer.ID]int)
	gs.mcache = NewMessageCache(1, 1)
	served := makeTestMessage(1)
	servedID := DefaultMsgIdFn(served)
	gs.mcache.Put(&Message{Message: served})

	backlog := rpcWithMessages(&pb.Message{Data: []byte("backlog")})
	if err := oq.Push(backlog, false); err != nil {
		t.Fatal(err)
	}
	topic := "t"
	gs.HandleRPC(&RPC{from: open, RPC: pb.RPC{Control: &pb.ControlMessage{
		Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"pull"}}},
		Iwant: []*pb.ControlIWant{{MessageIDs: []string{servedID}}},
	}}})

	first := popNext(t, oq)
	if got := first.Control.GetIwant(); len(got) != 1 || got[0].GetMessageIDs()[0] != "pull" {
		t.Fatalf("first RPC out is %v, want the urgent IWANT", first)
	}
	if got := popNext(t, oq); got != backlog {
		t.Fatal("the response took the last slot instead of being dropped")
	}
	if got := oq.queue.Len(); got != 0 {
		t.Fatalf("queue contains %d extra RPCs after the response was dropped", got)
	}
}

func TestSendRPCReportsNormalSendPiggyback(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, oq := newRefusalRouter(t, full, open)
	topic := "t"
	gs.gossip = map[peer.ID][]*pb.ControlIHave{
		open: {{TopicID: &topic, MessageIDs: []string{"m"}}},
	}
	payload := rpcWithMessages(&pb.Message{Data: []byte("segment")})

	if !gs.sendRPC(open, payload, false) {
		t.Fatal("normal send was refused")
	}
	if _, pending := gs.gossip[open]; pending {
		t.Fatal("normal send left gossip pending")
	}
	first := popNext(t, oq)
	if got := first.Control.GetIhave(); len(got) != 1 || got[0].GetMessageIDs()[0] != "m" {
		t.Fatalf("first RPC out carries %v, want the piggybacked IHAVE", got)
	}
	if got := popNext(t, oq); got != payload {
		t.Fatal("payload was not queued after the piggybacked gossip")
	}
}

// TestRetryAndReplayIWantsAreUrgent: the offer-table retry and the deferred replay dispatch the
// same way.
func TestRetryAndReplayIWantsAreUrgent(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")

	t.Run("retry", func(t *testing.T) {
		gs, oq := newRefusalRouter(t, full, open)
		backlog := rpcWithMessages(&pb.Message{Data: []byte("segment")})
		if err := oq.Push(backlog, false); err != nil {
			t.Fatal(err)
		}
		gs.recordOffer("m", "t", open)
		gs.retryOfferedPull("m")
		if got := popNext(t, oq).Control.GetIwant(); len(got) != 1 {
			t.Fatal("the retry's IWANT did not overtake the backlog")
		}
	})

	t.Run("replay", func(t *testing.T) {
		gs, oq := newRefusalRouter(t, full, open)
		gs.requestGate = &recordingGate{allow: func(string, string) bool { return true }}
		gs.deferred = map[string]*deferredEntry{"m": {topic: "t", peers: []peer.ID{open}, at: time.Now()}}
		gs.deferredMax = 8
		backlog := rpcWithMessages(&pb.Message{Data: []byte("segment")})
		if err := oq.Push(backlog, false); err != nil {
			t.Fatal(err)
		}
		gs.replayDeferred()
		if got := popNext(t, oq).Control.GetIwant(); len(got) != 1 {
			t.Fatal("the replay's IWANT did not overtake the backlog")
		}
	})
}

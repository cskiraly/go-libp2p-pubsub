package pubsub

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

func subOpts(topic string, subscribe, requestsPartial, supportsSending bool) *pb.RPC_SubOpts {
	return &pb.RPC_SubOpts{
		Topicid:                proto.String(topic),
		Subscribe:              proto.Bool(subscribe),
		RequestsPartial:        proto.Bool(requestsPartial),
		SupportsSendingPartial: proto.Bool(supportsSending),
	}
}

func newPartialInterestPubSub() *PubSub {
	return &PubSub{
		topics:            make(map[string]map[peer.ID]peerTopicState),
		partialInterest:   make(map[string]map[peer.ID]peerTopicState),
		myPartialInterest: make(map[string]bool),
	}
}

func TestSetPartialInterestFromPeer(t *testing.T) {
	const topic = "t"
	pid := peer.ID("peer-a")

	t.Run("records interest and withdraws it", func(t *testing.T) {
		ps := newPartialInterestPubSub()

		ps.setPartialInterestFromPeer(topic, pid, subOpts(topic, false, true, false))
		state, ok := ps.partialInterestState(topic, pid)
		if !ok || !state.requestsPartial {
			t.Fatalf("expected recorded interest, got %+v ok=%v", state, ok)
		}
		// The pre-existing rule: a peer that requests partials supports them by default.
		if !state.supportsPartial {
			t.Fatal("expected requestsPartial to imply supportsPartial")
		}

		// Both flags clear is an ordinary unsubscribe, which is also how interest is withdrawn.
		ps.setPartialInterestFromPeer(topic, pid, subOpts(topic, false, false, false))
		if _, ok := ps.partialInterestState(topic, pid); ok {
			t.Fatal("expected interest to be withdrawn")
		}
		if len(ps.partialInterest) != 0 {
			t.Fatalf("expected the empty topic map to be dropped, have %d", len(ps.partialInterest))
		}
	})

	t.Run("supports-sending alone is recorded without requesting", func(t *testing.T) {
		ps := newPartialInterestPubSub()

		ps.setPartialInterestFromPeer(topic, pid, subOpts(topic, false, false, true))
		state, ok := ps.partialInterestState(topic, pid)
		if !ok {
			t.Fatal("expected recorded interest")
		}
		if state.requestsPartial {
			t.Fatal("supports-sending must not imply requesting")
		}
		if !state.supportsPartial {
			t.Fatal("expected supportsPartial")
		}
	})
}

// TestPartialInterestIsSupersededBySubscription is the invariant that keeps peerRequestsPartial
// single-valued: a peer is never in both maps, so there is never a question of which one wins.
func TestPartialInterestIsSupersededBySubscription(t *testing.T) {
	const topic = "t"
	pid := peer.ID("peer-a")
	ps := newPartialInterestPubSub()
	ps.setPartialInterestFromPeer(topic, pid, subOpts(topic, false, true, false))

	// The subscribe branch of handleIncomingRPC, reduced to what it does to these two maps.
	ps.topics[topic] = map[peer.ID]peerTopicState{pid: {requestsPartial: true, supportsPartial: true}}
	ps.clearPartialInterest(topic, pid)

	if _, ok := ps.partialInterestState(topic, pid); ok {
		t.Fatal("a subscription must supersede partial-only interest")
	}
}

// TestClearPeerFromTopicsStateClearsPartialInterest covers the leak. The map is keyed by a
// peer-controlled topic string, so a peer that goes away without withdrawing its interest must
// not leave an entry behind.
func TestClearPeerFromTopicsStateClearsPartialInterest(t *testing.T) {
	pid := peer.ID("peer-a")
	other := peer.ID("peer-b")
	ps := newPartialInterestPubSub()
	ps.setPartialInterestFromPeer("only-peer", pid, subOpts("only-peer", false, true, false))
	ps.setPartialInterestFromPeer("shared", pid, subOpts("shared", false, true, false))
	ps.setPartialInterestFromPeer("shared", other, subOpts("shared", false, true, false))

	ps.clearPeerFromTopicsState(pid)

	if _, ok := ps.partialInterest["only-peer"]; ok {
		t.Fatal("expected the topic map with no peers left to be dropped")
	}
	if _, ok := ps.partialInterestState("shared", pid); ok {
		t.Fatal("expected the peer's interest to be cleared")
	}
	if _, ok := ps.partialInterestState("shared", other); !ok {
		t.Fatal("expected the other peer's interest to survive")
	}
}

// TestPartialInterestOverTheWire is the end-to-end contract, and the point of the whole change: a
// host that joined a topic without subscribing to it can be seen as requesting partial messages,
// is reachable through MeshPeers, and is still not a subscriber -- so it is not grafted into the
// mesh and not sent full messages.
func TestPartialInterestOverTheWire(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		const topic = "test-topic"
		hosts := getDefaultHosts(t, 2)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-ctx.Done()
			for _, h := range hosts {
				h.Close()
			}
		}()

		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		newExt := func() *partialmessages.PartialMessagesExtension[peerState] {
			return &partialmessages.PartialMessagesExtension[peerState]{
				Logger:        logger,
				OnEmitGossip:  func(string, []byte, []peer.ID, map[peer.ID]peerState) {},
				OnIncomingRPC: func(peer.ID, map[peer.ID]peerState, *pb.PartialMessagesExtension) error { return nil },
			}
		}

		// Host 0 joins for the partial traffic alone: no Subscribe.
		interested := getGossipsub(ctx, hosts[0], WithPartialMessagesExtension(newExt()))
		interestedTopic, err := interested.Join(topic, RequestPartialMessages())
		if err != nil {
			t.Fatal(err)
		}

		// Host 1 is an ordinary subscriber, and the one whose view we assert on.
		server := getGossipsub(ctx, hosts[1], WithPartialMessagesExtension(newExt()))
		serverTopic, err := server.Join(topic, RequestPartialMessages())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := serverTopic.Subscribe(); err != nil {
			t.Fatal(err)
		}

		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		interestedID := hosts[0].ID()

		// Before the announcement there is nothing to see: joining is purely local.
		if requests, subscriber, reachable := serverViewOf(t, server, topic, interestedID); requests || subscriber || reachable {
			t.Fatalf("before announcing: requests=%v subscriber=%v reachable=%v, want all false",
				requests, subscriber, reachable)
		}

		if err := interestedTopic.SetPartialInterest(context.Background(), true); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)

		requests, subscriber, reachable := serverViewOf(t, server, topic, interestedID)
		if !requests {
			t.Fatal("after announcing: the peer should be seen as requesting partial messages")
		}
		if subscriber {
			t.Fatal("after announcing: the peer must not be seen as a topic subscriber")
		}
		if !reachable {
			t.Fatal("after announcing: the peer should be reachable through MeshPeers")
		}

		if err := interestedTopic.SetPartialInterest(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)

		if requests, _, reachable := serverViewOf(t, server, topic, interestedID); requests || reachable {
			t.Fatalf("after withdrawing: requests=%v reachable=%v, want both false", requests, reachable)
		}
	})
}

// TestPartialInterestSurvivesANewStream covers the hello packet. Interest is announced once when
// set, so without it every reconnection would silently lose it.
func TestPartialInterestSurvivesANewStream(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		const topic = "test-topic"
		hosts := getDefaultHosts(t, 2)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-ctx.Done()
			for _, h := range hosts {
				h.Close()
			}
		}()

		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		newExt := func() *partialmessages.PartialMessagesExtension[peerState] {
			return &partialmessages.PartialMessagesExtension[peerState]{
				Logger:        logger,
				OnEmitGossip:  func(string, []byte, []peer.ID, map[peer.ID]peerState) {},
				OnIncomingRPC: func(peer.ID, map[peer.ID]peerState, *pb.PartialMessagesExtension) error { return nil },
			}
		}

		interested := getGossipsub(ctx, hosts[0], WithPartialMessagesExtension(newExt()))
		interestedTopic, err := interested.Join(topic, RequestPartialMessages())
		if err != nil {
			t.Fatal(err)
		}
		// Announced before there is any peer to announce it to.
		if err := interestedTopic.SetPartialInterest(context.Background(), true); err != nil {
			t.Fatal(err)
		}

		server := getGossipsub(ctx, hosts[1], WithPartialMessagesExtension(newExt()))
		serverTopic, err := server.Join(topic, RequestPartialMessages())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := serverTopic.Subscribe(); err != nil {
			t.Fatal(err)
		}

		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		if requests, subscriber, _ := serverViewOf(t, server, topic, hosts[0].ID()); !requests || subscriber {
			t.Fatalf("hello packet: requests=%v subscriber=%v, want true and false", requests, subscriber)
		}
	})
}

// serverViewOf reads, on the pubsub event loop, the three things that decide whether a peer gets
// partial messages on a topic: whether it is seen as requesting them, whether it is seen as a
// subscriber, and whether the partial-message router would reach it.
func serverViewOf(t *testing.T, ps *PubSub, topic string, pid peer.ID) (requests, subscriber, reachable bool) {
	t.Helper()

	done := make(chan struct{})
	ps.eval <- func() {
		defer close(done)
		gs := ps.rt.(*GossipSubRouter)
		requests = gs.peerRequestsPartial(pid, topic)
		_, subscriber = ps.topics[topic][pid]
		for peer := range (partialMessageRouter{gs}).MeshPeers(topic) {
			if peer == pid {
				reachable = true
			}
		}
	}
	<-done

	return requests, subscriber, reachable
}

// TestSetPartialInterestRefusedOnASubscribedTopic covers the footgun the API has to close. The
// announcement carries subscribe=false, so on a subscribed topic it would tell every peer to drop
// us from the topic's subscriber set -- silently cutting off the messages we subscribed for.
func TestSetPartialInterestRefusedOnASubscribedTopic(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		const topicID = "test-topic"
		hosts := getDefaultHosts(t, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-ctx.Done()
			hosts[0].Close()
		}()

		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		ext := &partialmessages.PartialMessagesExtension[peerState]{
			Logger:        logger,
			OnEmitGossip:  func(string, []byte, []peer.ID, map[peer.ID]peerState) {},
			OnIncomingRPC: func(peer.ID, map[peer.ID]peerState, *pb.PartialMessagesExtension) error { return nil },
		}
		ps := getGossipsub(ctx, hosts[0], WithPartialMessagesExtension(ext))
		topic, err := ps.Join(topicID, RequestPartialMessages())
		if err != nil {
			t.Fatal(err)
		}

		// Unsubscribed: allowed.
		if err := topic.SetPartialInterest(context.Background(), true); err != nil {
			t.Fatalf("expected the announcement to be allowed while unsubscribed: %v", err)
		}

		sub, err := topic.Subscribe()
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Cancel()

		if err := topic.SetPartialInterest(context.Background(), true); err == nil {
			t.Fatal("expected declaring interest in a subscribed topic to be refused")
		}
		// Withdrawal is refused too: it is the same announcement, and the same damage.
		if err := topic.SetPartialInterest(context.Background(), false); err == nil {
			t.Fatal("expected withdrawing interest in a subscribed topic to be refused")
		}
	})
}

// TestSetPartialInterestNeedsAPartialTopicOption: without one there is nothing to announce, and a
// silent no-op would look like a working pull arm that never receives anything.
func TestSetPartialInterestNeedsAPartialTopicOption(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		const topicID = "test-topic"
		hosts := getDefaultHosts(t, 1)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-ctx.Done()
			hosts[0].Close()
		}()

		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		ext := &partialmessages.PartialMessagesExtension[peerState]{
			Logger:        logger,
			OnEmitGossip:  func(string, []byte, []peer.ID, map[peer.ID]peerState) {},
			OnIncomingRPC: func(peer.ID, map[peer.ID]peerState, *pb.PartialMessagesExtension) error { return nil },
		}
		ps := getGossipsub(ctx, hosts[0], WithPartialMessagesExtension(ext))
		topic, err := ps.Join(topicID)
		if err != nil {
			t.Fatal(err)
		}

		if err := topic.SetPartialInterest(context.Background(), true); err == nil {
			t.Fatal("expected a topic joined without a partial-messages option to be refused")
		}
	})
}

package partialmessages

import (
	"iter"
	"log/slog"
	"testing"

	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

func newRegisterTestExt(t *testing.T) *PartialMessagesExtension[peerState] {
	t.Helper()
	ext := &PartialMessagesExtension[peerState]{
		Logger:       slog.Default(),
		OnEmitGossip: func(string, []byte, []peer.ID, map[peer.ID]peerState) {},
		OnIncomingRPC: func(peer.ID, map[peer.ID]peerState, *pubsub_pb.PartialMessagesExtension) error {
			return nil
		},
	}
	router := testRouter{
		sendRPC:   func(peer.ID, *pubsub_pb.PartialMessagesExtension, bool) {},
		meshPeers: func(string) iter.Seq[peer.ID] { return func(func(peer.ID) bool) {} },
	}
	if err := ext.Init(&router); err != nil {
		t.Fatal(err)
	}

	return ext
}

// TestRegisteredStateSurvivesHeartbeatsWithoutPeers pins the deletion-rule change RegisterPartial
// depends on: self-initiated state with no tracked peers lives on its TTL, because a
// registered-only group legitimately has none until heartbeat gossip finds some. Deleting it on
// the first heartbeat -- the old rule -- would kill the advertisement before its first emission.
func TestRegisteredStateSurvivesHeartbeatsWithoutPeers(t *testing.T) {
	const topic = "t"
	group := []byte("g")
	ext := newRegisterTestExt(t)

	if err := ext.RegisterPartial(topic, group); err != nil {
		t.Fatal(err)
	}

	ext.Heartbeat()
	ext.Heartbeat()
	if !hasGroup(ext, topic, group) {
		t.Fatal("registered state was deleted with heartbeats left on its TTL")
	}

	// And the TTL still bounds it: run it out and the state must go.
	for range int(ext.GroupTTLByHeatbeat) + minGroupTTL {
		ext.Heartbeat()
	}
	if hasGroup(ext, topic, group) {
		t.Fatal("registered state outlived its TTL")
	}
}

// TestPeerInitiatedStateStillEndsWhenItsPeersAreGone pins the other half of the rule: a group a
// remote peer started is still dropped once no stream carries it, TTL or not.
func TestPeerInitiatedStateStillEndsWhenItsPeersAreGone(t *testing.T) {
	const topic = "t"
	group := []byte("g")
	from := peer.ID("remote")
	ext := newRegisterTestExt(t)

	rpc := &pubsub_pb.PartialMessagesExtension{TopicID: &[]string{topic}[0], GroupID: group}
	if err := ext.HandleRPC(from, rpc); err != nil {
		t.Fatal(err)
	}
	if !hasGroup(ext, topic, group) {
		t.Fatal("incoming RPC should have created peer-initiated state")
	}

	ext.OnClosedOutboundStream(from)
	ext.Heartbeat()
	if hasGroup(ext, topic, group) {
		t.Fatal("peer-initiated state with no peers left should be dropped at the heartbeat")
	}
}

func hasGroup(ext *PartialMessagesExtension[peerState], topic string, group []byte) bool {
	tState, ok := ext.statePerTopicPerGroup[topic]
	if !ok {
		return false
	}
	_, ok = tState[string(group)]

	return ok
}

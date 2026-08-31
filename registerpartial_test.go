package pubsub

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/partialmessages"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestRegisterPartialIsAnnouncedByHeartbeat is the end-to-end contract of the advertisement-only
// contributor: a host that joined a topic without subscribing, and registered self-initiated
// partial state without publishing, is announced to the topic's subscribers by heartbeat gossip --
// and nothing is sent at registration time.
func TestRegisterPartialIsAnnouncedByHeartbeat(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		const topic = "test-topic"
		groupID := []byte("group-1")
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
		var emissions atomic.Int32
		gossiped := make(chan peer.ID, 8)
		contributorExt := &partialmessages.PartialMessagesExtension[peerState]{
			Logger: logger,
			OnEmitGossip: func(_ string, _ []byte, gossipPeers []peer.ID, _ map[peer.ID]peerState) {
				emissions.Add(1)
				for _, p := range gossipPeers {
					select {
					case gossiped <- p:
					default:
					}
				}
			},
			OnIncomingRPC: func(peer.ID, map[peer.ID]peerState, *pb.PartialMessagesExtension) error { return nil },
		}
		newServerExt := func() *partialmessages.PartialMessagesExtension[peerState] {
			return &partialmessages.PartialMessagesExtension[peerState]{
				Logger:        logger,
				OnEmitGossip:  func(string, []byte, []peer.ID, map[peer.ID]peerState) {},
				OnIncomingRPC: func(peer.ID, map[peer.ID]peerState, *pb.PartialMessagesExtension) error { return nil },
			}
		}

		// The contributor joins for sending only: no Subscribe, no publish.
		contributor := getGossipsub(ctx, hosts[0], WithPartialMessagesExtension(contributorExt))
		if _, err := contributor.Join(topic, SupportsPartialMessages()); err != nil {
			t.Fatal(err)
		}

		// An ordinary subscriber that requests partial messages.
		server := getGossipsub(ctx, hosts[1], WithPartialMessagesExtension(newServerExt()))
		serverTopic, err := server.Join(topic, RequestPartialMessages())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := serverTopic.Subscribe(); err != nil {
			t.Fatal(err)
		}

		connect(t, hosts[0], hosts[1])
		time.Sleep(2 * time.Second)

		if err := RegisterPartial[peerState](contributor, topic, groupID); err != nil {
			t.Fatal(err)
		}
		// Registration itself must contact nobody: the emission count stays zero until a
		// heartbeat runs. The sleep below spans several heartbeats, so the distinction is the
		// counter before and after, not timing luck.
		if got := emissions.Load(); got != 0 {
			t.Fatalf("registration contacted peers immediately: %d emissions", got)
		}

		time.Sleep(3 * time.Second)

		if got := emissions.Load(); got == 0 {
			t.Fatal("heartbeat never announced the registered state")
		}
		select {
		case p := <-gossiped:
			if p != hosts[1].ID() {
				t.Fatalf("announced to %s, want the subscriber %s", p, hosts[1].ID())
			}
		default:
			t.Fatal("no peer was handed to OnEmitGossip")
		}
	})
}

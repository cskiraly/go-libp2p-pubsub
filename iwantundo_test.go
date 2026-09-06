package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/peer"
)

// newRefusalRouter builds a router whose queue to `full` is at capacity, so any send to it is
// refused at admission, and whose queue to `open` accepts. No pubsub loop runs; the timers
// that dispatch arms return on the cancelled context.
func newRefusalRouter(t *testing.T, full, open peer.ID) (*GossipSubRouter, *rpcQueue) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gs := &GossipSubRouter{
		p: &PubSub{
			ctx:                   ctx,
			eval:                  make(chan func()),
			peers:                 make(map[peer.ID]*rpcQueue),
			seenMessages:          timecache.NewTimeCache(time.Minute),
			deliveredMessages:     timecache.NewTimeCache(time.Minute),
			maxMessageSize:        DefaultMaxMessageSize,
			maxControlMessageSize: DefaultMaxMessageSize,
			peerFilter:            DefaultPeerFilter,
		},
		params:      DefaultGossipSubParams(),
		mesh:        map[string]map[peer.ID]struct{}{"t": {}},
		logger:      slog.Default(),
		iasked:      make(map[peer.ID]int),
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		park: &commitmentPark{k: 1, ttl: time.Hour, promise: time.Second,
			promised: make(map[string]map[peer.ID]time.Time),
			breaks:   make(map[peer.ID]breakRecord),
			parked:   make(map[peer.ID]time.Time),
		},
		offers: &offerTable{
			offers: make(map[string]map[peer.ID]time.Time),
			tried:  make(map[string]map[peer.ID]time.Time),
			topic:  make(map[string]string),
			byPeer: make(map[peer.ID]int),
			armed:  make(map[string]struct{}),
		},
	}
	fq := newRpcQueue(1)
	if err := fq.Push(&RPC{}, false); err != nil {
		t.Fatal(err)
	}
	gs.p.peers[full] = fq
	oq := newRpcQueue(8)
	gs.p.peers[open] = oq
	return gs, oq
}

func TestSendRPCReportsAdmission(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, _ := newRefusalRouter(t, full, open)
	iwant := rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{"m"}}}, nil, nil, nil)
	if gs.sendRPC(full, iwant, false) {
		t.Fatal("a send to a full queue was reported as admitted")
	}
	if !gs.sendRPC(open, iwant, false) {
		t.Fatal("a send to an open queue was reported as refused")
	}
	if gs.sendRPC(peer.ID("gone"), iwant, false) {
		t.Fatal("a send to a peer without a queue was reported as admitted")
	}
}

func TestSendRPCSplitAdmission(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	// Each 40-byte id fits in a control frame; two do not. This IWANT needs three slots.
	mids := []string{strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)}
	for _, urgent := range []bool{false, true} {
		for _, free := range []int{2, 3} {
			t.Run(fmt.Sprintf("urgent=%v/free=%d", urgent, free), func(t *testing.T) {
				gs, _ := newRefusalRouter(t, full, open)
				gs.p.maxMessageSize, gs.p.maxControlMessageSize = 64, 48
				q := newRpcQueue(free + 2)
				gs.p.peers[open] = q
				normal, priority := &RPC{}, &RPC{}
				if err := q.Push(normal, false); err != nil {
					t.Fatal(err)
				}
				if err := q.UrgentPush(priority, false); err != nil {
					t.Fatal(err)
				}
				out := rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: mids}}, nil, nil, nil)
				admitted := gs.sendRPC(open, out, urgent)
				if admitted != (free == 3) {
					t.Fatalf("admitted=%v with %d free slots, need three", admitted, free)
				}
				if !admitted {
					if len(q.queue.normal) != 1 || q.queue.normal[0] != normal || len(q.queue.priority) != 1 || q.queue.priority[0] != priority {
						t.Fatal("refusing a split RPC changed the queue")
					}
					return
				}
				if q.queue.Len() != 5 {
					t.Fatalf("queue has %d RPCs, want the backlog plus three fragments", q.queue.Len())
				}
				var got []string
				for q.queue.Len() > 0 {
					rpc := popNext(t, q)
					if rpc == normal || rpc == priority {
						continue
					}
					if rpc.exceedsSizeLimits(64, 48) {
						t.Fatal("an oversized fragment was admitted")
					}
					for _, iw := range rpc.GetControl().GetIwant() {
						got = append(got, iw.MessageIDs...)
					}
				}
				if !slices.Equal(got, mids) {
					t.Fatalf("queued ids %v, want %v", got, mids)
				}
			})
		}
	}

	t.Run("an unsplittable id refuses every fragment", func(t *testing.T) {
		gs, q := newRefusalRouter(t, full, open)
		gs.p.maxMessageSize, gs.p.maxControlMessageSize = 64, 48
		out := rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: []string{"valid", strings.Repeat("x", 128)}}}, nil, nil, nil)
		if gs.sendRPC(open, out, true) || q.queue.Len() != 0 {
			t.Fatal("an oversized id allowed a partially admitted request")
		}
	})

	t.Run("piggybacked control has independent admission", func(t *testing.T) {
		gs, _ := newRefusalRouter(t, full, open)
		gs.p.maxMessageSize, gs.p.maxControlMessageSize = 64, 48
		q := newRpcQueue(3)
		gs.p.peers[open] = q
		topic := "t"
		gs.gossip = map[peer.ID][]*pb.ControlIHave{open: {{TopicID: &topic, MessageIDs: []string{"pending"}}}}
		out := rpcWithControl(nil, nil, []*pb.ControlIWant{{MessageIDs: mids}}, nil, nil, nil)
		if gs.sendRPC(open, out, false) {
			t.Fatal("the split request fit after piggybacked control took a slot")
		}
		if q.queue.Len() != 1 {
			t.Fatalf("queue has %d RPCs, want only the independent piggyback", q.queue.Len())
		}
		rpc := popNext(t, q)
		if got := rpc.GetControl().GetIhave(); len(got) != 1 || got[0].MessageIDs[0] != "pending" || len(rpc.GetControl().GetIwant()) != 0 {
			t.Fatalf("queued %v, want only the pending IHAVE", rpc)
		}
	})
}

// TestRefusedIWantBlamesNobody drives a real refusal through the offer-table retry: the
// request never left, so nothing may remain charged to the peer, and the next selection must
// rotate to another announcer.
func TestRefusedIWantBlamesNobody(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, oq := newRefusalRouter(t, full, open)
	var failed, committed []string
	gs.requestGate = &recordingGate{
		allow:     func(string, string) bool { return true },
		committed: func(mids []string) { committed = append(committed, mids...) },
		failed:    func(mids []string) { failed = append(failed, mids...) },
	}
	gs.recordOffer("m", "t", full)

	gs.retryOfferedPull("m")

	if _, open := gs.park.promised["m"][full]; open {
		t.Fatal("a refused request left a promise open")
	}
	if _, asked := gs.iwantAsked["m"]; asked {
		t.Fatal("a refused request left the discipline window closed")
	}
	if gs.iasked[full] != 0 {
		t.Fatalf("iasked = %d after a refused request, want 0", gs.iasked[full])
	}
	if len(committed) != 1 || len(failed) != 1 || failed[0] != "m" {
		t.Fatalf("gate saw committed=%v failed=%v, want one commit refunded by one failure", committed, failed)
	}
	if !gs.sendRefusedRecently(full) {
		t.Fatal("the refused peer is not cooling down")
	}
	if _, was := gs.offers.tried["m"][full]; !was {
		t.Fatal("the attempt was forgotten, so the refused peer would be the preferred candidate again")
	}
	if _, armed := gs.offers.armed["m"]; !armed {
		t.Fatal("no retry was re-armed after the refusal")
	}

	// Another announcer appears; when the re-armed check comes due it is chosen, the refused
	// one is skipped, and the request goes out and is charged normally.
	gs.recordOffer("m", "t", open)
	fireOfferRetries(gs)
	if _, open := gs.park.promised["m"][open]; !open {
		t.Fatal("the request to the open peer left no promise")
	}
	if gs.iasked[open] != 1 || gs.iasked[full] != 0 {
		t.Fatalf("iasked open=%d full=%d, want 1 and 0", gs.iasked[open], gs.iasked[full])
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rpc, err := oq.Pop(ctx)
	if err != nil {
		t.Fatalf("no IWANT queued to the open peer: %v", err)
	}
	if got := rpc.Control.GetIwant(); len(got) != 1 || len(got[0].GetMessageIDs()) != 1 || got[0].GetMessageIDs()[0] != "m" {
		t.Fatalf("queued RPC carries %v, want one IWANT for m", got)
	}

	// The cooldown lapses with the window.
	gs.iwantWindow = time.Nanosecond
	time.Sleep(time.Millisecond)
	gs.clearIWantAsked()
	if gs.sendRefusedRecently(full) {
		t.Fatal("the cooldown did not lapse")
	}
	if len(gs.iwantRefused) != 0 {
		t.Fatal("the lapsed cooldown was not swept")
	}
}

func TestRefusedIWantPreservesEarlierTracerPromise(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, _ := newRefusalRouter(t, full, open)
	gs.park.k = 2
	gs.gossipTracer = newGossipTracer()
	gs.gossipTracer.followUpTime = time.Hour
	fq := gs.p.peers[full]
	popNext(t, fq) // make room for the first ask
	gs.recordOffer("m", "t", full)

	gs.retryOfferedPull("m")
	popNext(t, fq) // the first ask was transmitted
	if _, promised := gs.gossipTracer.promises["m"][full]; !promised {
		t.Fatal("the first transmitted ask left no tracer promise")
	}

	// The first commitment breaks, but k=2 leaves the peer eligible for another ask
	// while the tracer still scores the earlier one.
	firstAsked := gs.park.promised["m"][full]
	gs.promiseDue("m", full, firstAsked)
	if gs.peerParked(full) {
		t.Fatal("one break parked a peer at k=2")
	}
	if err := fq.Push(&RPC{}, false); err != nil {
		t.Fatal(err)
	}

	gs.retryOfferedPull("m")

	if _, promised := gs.gossipTracer.promises["m"][full]; !promised {
		t.Fatal("the refused second ask erased the first ask's tracer promise")
	}
	if _, promised := gs.gossipTracer.peerPromises[full]["m"]; !promised {
		t.Fatal("the refused second ask erased the first ask's peer index")
	}
}

func TestRefusedIWantPreservesEarlierDisciplineState(t *testing.T) {
	t.Run("unmatched IHAVE cannot refund an open segment ask", func(t *testing.T) {
		full, open := peer.ID("full"), peer.ID("open")
		gs, _ := newRefusalRouter(t, full, open)
		gs.iwantMatch = func(topic string) bool { return topic == "segments" }
		gs.p.peerFilter = DefaultPeerFilter
		gs.mesh = map[string]map[peer.ID]struct{}{"segments": {}, "blocks": {}}
		gs.peerhave = make(map[peer.ID]int)
		gs.extensions = newExtensionsState(PeerExtensions{}, func(peer.ID) {}, func(p peer.ID, out *RPC, urgent bool) { gs.sendRPC(p, out, urgent) })
		fq := gs.p.peers[full]
		backlog := popNext(t, fq)
		gs.recordOffer("m", "segments", full)
		gs.retryOfferedPull("m")
		popNext(t, fq) // the segment ask was transmitted
		at := gs.iwantAsked["m"]
		if at.IsZero() || !gs.park.promised["m"][full].Equal(at) || gs.iwantGranted[full] != 1 {
			t.Fatal("the segment ask did not record its window, promise and cap charge")
		}
		if err := fq.Push(backlog, false); err != nil {
			t.Fatal(err)
		}

		topic := "blocks"
		gs.HandleRPC(&RPC{from: full, RPC: pb.RPC{Control: &pb.ControlMessage{
			Ihave: []*pb.ControlIHave{{TopicID: &topic, MessageIDs: []string{"m"}}},
		}}})

		if !gs.sendRefusedRecently(full) {
			t.Fatal("the unmatched-topic ask did not reach admission refusal")
		}
		if !gs.iwantAsked["m"].Equal(at) || !gs.park.promised["m"][full].Equal(at) {
			t.Fatal("the unmatched-topic refusal erased the earlier window or promise")
		}
		if gs.iwantGranted[full] != 1 || gs.iasked[full] != 1 {
			t.Fatalf("earlier charges changed: discipline=%d iasked=%d, want 1 each", gs.iwantGranted[full], gs.iasked[full])
		}
		if popNext(t, fq) != backlog || fq.queue.Len() != 0 {
			t.Fatal("the refused ask changed the full queue")
		}
		gs.promiseDue("m", full, at)
		if !gs.peerParked(full) {
			t.Fatal("the earlier unserved ask escaped judgment")
		}
	})

	t.Run("undo preserves replacement stamps and refunds its own cap charge", func(t *testing.T) {
		full, open := peer.ID("full"), peer.ID("open")
		gs, _ := newRefusalRouter(t, full, open)
		allowed, attempt := gs.iwantAllowed("m", "t", full)
		if !allowed {
			t.Fatal("the ask was refused")
		}
		next := attempt.at.Add(time.Second)
		gs.iwantAsked["m"] = next
		gs.park.promised["m"][full] = next
		gs.park.promised["m"][open] = next
		gs.iwantGranted[full]++

		gs.undoIWantDispatch(full, []string{"m"}, map[string]iwantAttempt{"m": attempt}, promiseTrace{})
		if !gs.iwantAsked["m"].Equal(next) || !gs.park.promised["m"][full].Equal(next) || !gs.park.promised["m"][open].Equal(next) {
			t.Fatal("undo changed state owned by another attempt")
		}
		if gs.iwantGranted[full] != 1 {
			t.Fatalf("cap count = %d, want only the replacement charge", gs.iwantGranted[full])
		}
	})

	t.Run("a missing window does not lose the cap refund", func(t *testing.T) {
		full, open := peer.ID("full"), peer.ID("open")
		gs, _ := newRefusalRouter(t, full, open)
		allowed, attempt := gs.iwantAllowed("m", "t", full)
		if !allowed {
			t.Fatal("the ask was refused")
		}
		delete(gs.iwantAsked, "m")
		gs.undoIWantDispatch(full, []string{"m"}, map[string]iwantAttempt{"m": attempt}, promiseTrace{})
		if gs.promiseOpen("m", full) || gs.iwantGranted[full] != 0 {
			t.Fatal("undo left its promise or cap charge after the window was cleared")
		}
	})
}

// TestDisconnectedAnnouncerIsSkipped: an announcer that left has no queue, so it is not a
// candidate; nothing is charged and nothing is sent.
func TestDisconnectedAnnouncerIsSkipped(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, _ := newRefusalRouter(t, full, open)
	gone := peer.ID("gone")
	gs.recordOffer("m", "t", gone)
	if _, found, waiting := gs.pickOfferedPeer("m"); found || waiting {
		t.Fatal("a disconnected announcer was selected or waited for")
	}
	gs.retryOfferedPull("m")
	if len(gs.park.promised) != 0 || len(gs.iwantAsked) != 0 {
		t.Fatal("state was recorded for a request to a disconnected peer")
	}
}

// TestReplayDeferredKeepsRefusedEntries: a deferred announcement whose IWANT is refused stays
// deferred for the next replay instead of being lost to our own queue.
func TestReplayDeferredKeepsRefusedEntries(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, _ := newRefusalRouter(t, full, open)
	gs.requestGate = &recordingGate{allow: func(string, string) bool { return true }}
	gs.deferred = map[string]*deferredEntry{
		"m1": {topic: "t", peers: []peer.ID{full}, at: time.Now()},
		"m2": {topic: "t", peers: []peer.ID{open}, at: time.Now()},
	}
	gs.deferredMax = 8
	gs.replayDeferred()
	if _, kept := gs.deferred["m1"]; !kept {
		t.Fatal("the entry whose IWANT was refused was dropped")
	}
	if _, kept := gs.deferred["m2"]; kept {
		t.Fatal("the entry whose IWANT was sent was not cleared")
	}
	if _, asked := gs.iwantAsked["m1"]; asked {
		t.Fatal("the refused replay left the discipline window closed")
	}
}

func TestRetryAndReplayPeerEligibility(t *testing.T) {
	first, second := peer.ID("full"), peer.ID("open")
	for _, path := range []string{"retry", "replay"} {
		t.Run(path, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				setup func(*GossipSubRouter)
				want  peer.ID
			}{
				{"non-mesh announcer on a joined topic", func(*GossipSubRouter) {}, first},
				{"score below gossip threshold", func(gs *GossipSubRouter) {
					gs.gossipThreshold = -1
					gs.score = newPeerScore(&PeerScoreParams{
						AppSpecificScore:  func(peer.ID) float64 { return -2 },
						AppSpecificWeight: 1,
					}, gs.logger)
					gs.score.peerStats[first] = &peerStats{}
				}, second},
				{"no IWANT allowance", func(gs *GossipSubRouter) {
					gs.iasked[first] = gs.params.MaxIHaveLength
				}, second},
				{"no longer joined", func(gs *GossipSubRouter) {
					delete(gs.mesh, "t")
				}, ""},
				{"peer filter declines", func(gs *GossipSubRouter) {
					gs.p.peerFilter = func(p peer.ID, _ string) bool { return p != first }
				}, second},
				{"parked announcer", func(gs *GossipSubRouter) {
					gs.park.parked[first] = time.Now().Add(time.Hour)
				}, second},
				{"refusal cooldown", func(gs *GossipSubRouter) {
					gs.iwantRefused = map[peer.ID]time.Time{first: time.Now()}
				}, second},
				{"disconnected announcer", func(gs *GossipSubRouter) {
					delete(gs.p.peers, first)
				}, second},
			} {
				t.Run(tc.name, func(t *testing.T) {
					gs, sq := newRefusalRouter(t, first, second)
					fq := gs.p.peers[first]
					popNext(t, fq) // both peers can accept; eligibility must choose between them
					gs.recordOffer("m", "t", first)
					gs.recordOffer("m", "t", second)
					gs.requestGate = &recordingGate{allow: func(string, string) bool { return true }}
					gs.deferred = map[string]*deferredEntry{"m": {topic: "t", peers: []peer.ID{first, second}, at: time.Now()}}
					tc.setup(gs)
					if path == "retry" {
						gs.retryOfferedPull("m")
					} else {
						gs.replayDeferred()
					}
					for p, q := range map[peer.ID]*rpcQueue{first: fq, second: sq} {
						if p != tc.want {
							if q.queue.Len() != 0 || gs.promiseOpen("m", p) || gs.iwantGranted[p] != 0 {
								t.Fatalf("unselected peer %q received an ask or a charge", p)
							}
							continue
						}
						if !gs.promiseOpen("m", p) || gs.iwantGranted[p] != 1 {
							t.Fatalf("eligible peer %q was not asked", p)
						}
						rpc := popNext(t, q)
						if iw := rpc.GetControl().GetIwant(); len(iw) != 1 || len(iw[0].MessageIDs) != 1 || iw[0].MessageIDs[0] != "m" {
							t.Fatalf("queued %v, want one IWANT for m", rpc)
						}
					}
					if tc.want == "" && path == "replay" && gs.deferred["m"] == nil {
						t.Fatal("replay lost an entry with no eligible announcers")
					}
				})
			}
		})
	}
}

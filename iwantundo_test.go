package pubsub

import (
	"context"
	"log/slog"
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
		},
		params:      DefaultGossipSubParams(),
		logger:      slog.Default(),
		iasked:      make(map[peer.ID]int),
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		park: &commitmentPark{k: 1, ttl: time.Hour, promise: time.Second,
			promised: make(map[string]map[peer.ID]time.Time),
			breaks:   make(map[peer.ID]int),
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

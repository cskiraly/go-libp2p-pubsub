package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/peer"
)

type recordingGate struct {
	allow     func(topic, mid string) bool
	committed func(mids []string)
	failed    func(mids []string)
}

func (g *recordingGate) Allow(_ peer.ID, topic, mid string) bool { return g.allow(topic, mid) }
func (g *recordingGate) Committed(_ peer.ID, mids []string) {
	if g.committed != nil {
		g.committed(mids)
	}
}
func (g *recordingGate) Failed(_ peer.ID, mids []string) {
	if g.failed != nil {
		g.failed(mids)
	}
}

func newGateRouter(allow func(topic, mid string) bool) *GossipSubRouter {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return &GossipSubRouter{
		p:           &PubSub{ctx: ctx, eval: make(chan func()), peerFilter: DefaultPeerFilter},
		mesh:        map[string]map[peer.ID]struct{}{"t": {}},
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		requestGate: &recordingGate{allow: allow},
		deferred:    make(map[string]*deferredEntry),
		deferredMax: 64,
	}
}

// TestRequestGateDeclineIsRetryable pins the ordering that is the request gate's entire
// correctness argument: the gate runs ahead of every mutating request policy, so a decline
// consumes no request state and the same id can be asked for later.
func TestRequestGateDeclineIsRetryable(t *testing.T) {
	candidates := []string{"a", "b", "c", "d"}
	const allowance = 4

	t.Run("a closed gate declines everything and records nothing", func(t *testing.T) {
		gs := newGateRouter(func(string, string) bool { return false })
		if got, _ := gs.selectIWants(candidates, nil, peer.ID("p"), allowance); len(got) != 0 {
			t.Fatalf("dispatched %d ids through a closed gate", len(got))
		}
		if len(gs.iwantAsked) != 0 {
			t.Fatalf("closed gate recorded %d asks", len(gs.iwantAsked))
		}
		if len(gs.deferred) != len(candidates) {
			t.Fatalf("deferred %d ids, want all %d", len(gs.deferred), len(candidates))
		}
	})

	t.Run("the same router asks once the gate opens", func(t *testing.T) {
		open := false
		gs := newGateRouter(func(string, string) bool { return open })
		gs.selectIWants(candidates, nil, peer.ID("p"), allowance)
		open = true
		got, _ := gs.selectIWants(candidates, nil, peer.ID("p"), allowance)
		if len(got) != len(candidates) {
			t.Fatalf("after opening the gate, dispatched %d of %d ids -- the decline was not retryable",
				len(got), len(candidates))
		}
	})

	t.Run("a declined id does not consume another id's allowance", func(t *testing.T) {
		gs := newGateRouter(func(_, mid string) bool { return mid != "a" && mid != "b" })
		got, _ := gs.selectIWants(candidates, nil, peer.ID("p"), 2)
		if len(got) != 2 || got[0] != "c" || got[1] != "d" {
			t.Fatalf("dispatched %v, want [c d]", got)
		}
	})

	t.Run("the gate sees the topic each id was announced under", func(t *testing.T) {
		seen := map[string]string{}
		gs := newGateRouter(func(topic, mid string) bool {
			seen[mid] = topic
			return true
		})
		topicOf := map[string]string{"a": "t1", "b": "t2", "c": "t1", "d": "t2"}
		gs.selectIWants(candidates, topicOf, peer.ID("p"), allowance)
		for mid, want := range topicOf {
			if seen[mid] != want {
				t.Fatalf("gate saw topic %q for %q, want %q", seen[mid], mid, want)
			}
		}
	})
}

// TestRequestGateCommitsOnlyDispatched pins the other half of the gate contract: an id the
// gate approved but a later policy refused must NOT be reported as committed. An application
// that charged a per-claim budget inside Allow would spend it on requests no IWANT carried.
func TestRequestGateCommitsOnlyDispatched(t *testing.T) {
	var allowed, committed []string
	gs := newGateRouter(func(_, mid string) bool {
		allowed = append(allowed, mid)
		return true
	})
	gs.requestGate.(*recordingGate).committed = func(mids []string) { committed = append(committed, mids...) }
	// The discipline refuses "b": an ask for it is already in flight.
	gs.iwantAsked["b"] = time.Now()

	got, _ := gs.selectIWants([]string{"a", "b", "c"}, nil, peer.ID("p"), 3)

	if len(allowed) != 3 {
		t.Fatalf("gate consulted for %v, want all three candidates", allowed)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("dispatched %v, want [a c]", got)
	}
	// selectIWants does not itself commit; handleIHave does, with the dispatched list.
	gs.gateCommitted(peer.ID("p"), got)
	for _, mid := range committed {
		if mid == "b" {
			t.Fatal("an id refused by a request policy was reported as committed")
		}
	}
	if len(committed) != 2 {
		t.Fatalf("committed %v, want exactly the dispatched pair", committed)
	}
}

func TestRequestDeferralBounds(t *testing.T) {
	gs := newGateRouter(func(string, string) bool { return false })
	gs.deferredMax = 2
	for i := range 5 {
		gs.noteDeclined(peer.ID("p"), "t", fmt.Sprintf("m%d", i))
	}
	if len(gs.deferred) != 2 {
		t.Fatalf("deferred %d ids, want the cap 2", len(gs.deferred))
	}
	for i := range maxDeferredAnnouncers + 3 {
		gs.noteDeclined(peer.ID(fmt.Sprintf("p%d", i)), "t", "m0")
	}
	if n := len(gs.deferred["m0"].peers); n != maxDeferredAnnouncers {
		t.Fatalf("%d announcers remembered, want %d", n, maxDeferredAnnouncers)
	}
	gs.noteDeclined(peer.ID("p0"), "t", "m0")
	if n := len(gs.deferred["m0"].peers); n != maxDeferredAnnouncers {
		t.Fatal("a repeated announcer was recorded twice")
	}
}

// TestRequestDeferralSweep: the heartbeat drops expired and delivered entries while the gate
// stays closed, without waiting for the application to replay.
func TestRequestDeferralSweep(t *testing.T) {
	gs := newGateRouter(func(string, string) bool { return false })
	gs.params = DefaultGossipSubParams()
	gs.p.deliveredMessages = timecache.NewTimeCache(time.Minute)
	for _, mid := range []string{"m1", "m2", "m3"} {
		gs.noteDeclined(peer.ID("p"), "t", mid)
	}
	gs.deferred["m2"].at = time.Now().Add(-time.Hour)
	gs.p.deliveredMessages.Add("m3")

	gs.sweepDeferred()
	if _, ok := gs.deferred["m2"]; ok {
		t.Fatal("an expired entry survived the sweep")
	}
	if _, ok := gs.deferred["m3"]; ok {
		t.Fatal("a delivered entry survived the sweep")
	}
	if _, ok := gs.deferred["m1"]; !ok {
		t.Fatal("a live entry was swept")
	}
}

// TestRequestDeferralReplay pins the replay: once the gate opens, a deferred id is requested
// from a connected announcer through the ordinary selection path, and delivered or expired
// entries are dropped instead.
func TestRequestDeferralReplay(t *testing.T) {
	open := false
	gs := newGateRouter(func(string, string) bool { return open })
	gs.params = DefaultGossipSubParams()
	gs.iasked = make(map[peer.ID]int)
	gs.logger = slog.Default()
	gs.p.peers = make(map[peer.ID]*rpcQueue)
	gs.p.deliveredMessages = timecache.NewTimeCache(time.Minute)
	gs.p.maxMessageSize = DefaultMaxMessageSize
	gs.p.maxControlMessageSize = DefaultMaxMessageSize
	announcer, gone := peer.ID("announcer"), peer.ID("gone")
	q := newRpcQueue(32)
	gs.p.peers[announcer] = q

	gs.selectIWants([]string{"m1", "m2", "m3"}, map[string]string{"m1": "t", "m2": "t", "m3": "t"}, announcer, 3)
	gs.noteDeclined(gone, "t", "m4")
	gs.p.deliveredMessages.Add("m3")
	gs.deferred["m2"].at = time.Now().Add(-time.Hour)

	gs.replayDeferred()
	// m3 was delivered and m2 expired: pruned. m1 is still declined and m4's announcer is
	// not connected: both kept for a later replay.
	if _, ok := gs.deferred["m2"]; ok {
		t.Fatal("an expired entry survived the replay")
	}
	if _, ok := gs.deferred["m3"]; ok {
		t.Fatal("a delivered entry survived the replay")
	}
	if _, ok := gs.deferred["m1"]; !ok {
		t.Fatal("a still-declined entry was dropped")
	}
	if _, ok := gs.deferred["m4"]; !ok {
		t.Fatal("an entry whose announcer is disconnected was dropped")
	}
}

// TestRequestDeferralReplayOpensLater: nothing is dispatched while the gate is closed; once it
// opens, the deferred ids go out in one IWANT to the announcer.
func TestRequestDeferralReplayOpensLater(t *testing.T) {
	open := false
	gs := newGateRouter(func(string, string) bool { return open })
	gs.params = DefaultGossipSubParams()
	gs.iasked = make(map[peer.ID]int)
	gs.logger = slog.Default()
	gs.p.peers = make(map[peer.ID]*rpcQueue)
	gs.p.deliveredMessages = timecache.NewTimeCache(time.Minute)
	gs.p.maxMessageSize = DefaultMaxMessageSize
	gs.p.maxControlMessageSize = DefaultMaxMessageSize
	announcer := peer.ID("announcer")
	q := newRpcQueue(32)
	gs.p.peers[announcer] = q

	topicOf := map[string]string{"m1": "t", "m2": "t"}
	gs.selectIWants([]string{"m1", "m2"}, topicOf, announcer, 2)
	if len(gs.deferred) != 2 {
		t.Fatalf("deferred %d, want 2", len(gs.deferred))
	}

	gs.replayDeferred()
	if len(gs.deferred) != 2 {
		t.Fatalf("a closed gate replay dropped entries: %d left", len(gs.deferred))
	}
	if len(gs.iwantAsked) != 0 {
		t.Fatal("a closed gate replay recorded asks")
	}

	open = true
	gs.replayDeferred()
	if len(gs.deferred) != 0 {
		t.Fatalf("open gate replay left %d entries", len(gs.deferred))
	}
	if len(gs.iwantAsked) != 2 {
		t.Fatalf("open gate replay recorded %d asks, want 2", len(gs.iwantAsked))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rpc, err := q.Pop(ctx)
	if err != nil {
		t.Fatalf("no IWANT was queued to the announcer: %v", err)
	}
	if n := len(rpc.Control.GetIwant()); n != 1 || len(rpc.Control.GetIwant()[0].GetMessageIDs()) != 2 {
		t.Fatalf("queued RPC carries %d IWANT entries, want one with both ids", n)
	}
}

func TestRequestDeferralRetryDecline(t *testing.T) {
	full, open := peer.ID("full"), peer.ID("open")
	gs, q := newRefusalRouter(t, full, open)
	allowed := false
	gs.requestGate = &recordingGate{allow: func(string, string) bool { return allowed }}
	gs.deferred = make(map[string]*deferredEntry)
	gs.deferredMax = 8
	gs.recordOffer("m", "t", open)
	gs.retryOfferedPull("m")
	if e := gs.deferred["m"]; e == nil || e.topic != "t" || len(e.peers) != 1 || e.peers[0] != open {
		t.Fatalf("gate-declined retry left deferral %v, want m on t from open", e)
	}
	if gs.promiseOpen("m", open) || len(gs.iwantAsked) != 0 || gs.iasked[open] != 0 {
		t.Fatal("a gate-declined retry recorded request state")
	}
	allowed = true
	gs.replayDeferred()
	if gs.deferred["m"] != nil || !gs.promiseOpen("m", open) {
		t.Fatal("the deferred retry was not requested when the gate opened")
	}
	if got := popNext(t, q).GetControl().GetIwant(); len(got) != 1 || len(got[0].MessageIDs) != 1 || got[0].MessageIDs[0] != "m" {
		t.Fatalf("replay queued %v, want an IWANT for m", got)
	}
}

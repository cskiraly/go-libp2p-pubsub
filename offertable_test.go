package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-pubsub/timecache"
	"github.com/libp2p/go-libp2p/core/peer"
)

func newOfferRouter() *GossipSubRouter {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Every announcer the tests name is connected: selection skips peers without a send queue.
	peers := make(map[peer.ID]*rpcQueue)
	for _, p := range []string{"announcer-a", "announcer-b", "announcer-c"} {
		peers[peer.ID(p)] = newRpcQueue(8)
	}
	return &GossipSubRouter{
		p:           &PubSub{ctx: ctx, eval: make(chan func()), peers: peers, seenMessages: timecache.NewTimeCache(time.Minute)},
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		offers: &offerTable{
			offers: make(map[string]map[peer.ID]time.Time),
			tried:  make(map[string]map[peer.ID]time.Time),
			topic:  make(map[string]string),
			byPeer: make(map[peer.ID]int),
			armed:  make(map[string]struct{}),
		},
	}
}

func TestOfferTable(t *testing.T) {
	a, b, c := peer.ID("announcer-a"), peer.ID("announcer-b"), peer.ID("announcer-c")

	t.Run("untried announcers are picked first, in peer order", func(t *testing.T) {
		gs := newOfferRouter()
		gs.recordOffer("m", "t", b)
		gs.recordOffer("m", "t", a)
		gs.recordOffer("m", "t", c)
		if p, ok, _ := gs.pickOfferedPeer("m"); !ok || p != a {
			t.Fatalf("picked %q, want the first untried announcer %q", p, a)
		}
		gs.offers.tried["m"] = map[peer.ID]time.Time{a: time.Now()}
		if p, ok, _ := gs.pickOfferedPeer("m"); !ok || p != b {
			t.Fatalf("picked %q, want the next untried announcer %q", p, b)
		}
	})

	t.Run("when every announcer was tried, the least recently tried is picked", func(t *testing.T) {
		gs := newOfferRouter()
		gs.recordOffer("m", "t", a)
		gs.recordOffer("m", "t", b)
		now := time.Now()
		gs.offers.tried["m"] = map[peer.ID]time.Time{a: now, b: now.Add(-time.Second)}
		if p, ok, _ := gs.pickOfferedPeer("m"); !ok || p != b {
			t.Fatalf("picked %q, want the least recently tried %q", p, b)
		}
	})

	t.Run("parked announcers are skipped", func(t *testing.T) {
		gs := newOfferRouter()
		gs.park = &commitmentPark{k: 1, ttl: time.Hour, promise: time.Second,
			promised: make(map[string]map[peer.ID]time.Time),
			breaks:   make(map[peer.ID]int),
			parked:   map[peer.ID]time.Time{a: time.Now().Add(time.Hour)},
		}
		gs.recordOffer("m", "t", a)
		gs.recordOffer("m", "t", b)
		if p, ok, _ := gs.pickOfferedPeer("m"); !ok || p != b {
			t.Fatalf("picked %q, want the unparked announcer %q", p, b)
		}
	})

	t.Run("delivery clears the id and refunds the per-peer budget", func(t *testing.T) {
		gs := newOfferRouter()
		gs.recordOffer("m", "t", a)
		gs.recordOffer("m2", "t", a)
		if gs.offers.byPeer[a] != 2 {
			t.Fatalf("byPeer = %d, want 2", gs.offers.byPeer[a])
		}
		gs.noteDelivered("m")
		if _, still := gs.offers.offers["m"]; still {
			t.Fatal("a delivered id is still offered")
		}
		if gs.offers.byPeer[a] != 1 {
			t.Fatalf("byPeer = %d after delivery, want 1", gs.offers.byPeer[a])
		}
		if _, ok, _ := gs.pickOfferedPeer("m"); ok {
			t.Fatal("a delivered id still has a pick")
		}
	})

	t.Run("a peer beyond its budget is not recorded", func(t *testing.T) {
		gs := newOfferRouter()
		for i := range offerPerPeer + 5 {
			gs.recordOffer(fmt.Sprintf("m%d", i), "t", a)
		}
		if gs.offers.byPeer[a] != offerPerPeer {
			t.Fatalf("byPeer = %d, want the cap %d", gs.offers.byPeer[a], offerPerPeer)
		}
		if len(gs.offers.offers) != offerPerPeer {
			t.Fatalf("%d ids recorded, want %d", len(gs.offers.offers), offerPerPeer)
		}
	})

	t.Run("an id keeps at most offerPerID announcers", func(t *testing.T) {
		gs := newOfferRouter()
		for i := range offerPerID + 3 {
			gs.recordOffer("m", "t", peer.ID(fmt.Sprintf("p%02d", i)))
		}
		if n := len(gs.offers.offers["m"]); n != offerPerID {
			t.Fatalf("%d announcers kept, want %d", n, offerPerID)
		}
	})

	t.Run("offers on undisciplined topics are not recorded", func(t *testing.T) {
		gs := newOfferRouter()
		gs.iwantMatch = func(topic string) bool { return topic == "t" }
		gs.recordOffer("m", "other", a)
		if len(gs.offers.offers) != 0 {
			t.Fatal("an offer on an unmatched topic was recorded")
		}
	})

	t.Run("an announcer with an open promise is skipped and waited for", func(t *testing.T) {
		gs := newOfferRouter()
		gs.park = &commitmentPark{k: 1, ttl: time.Hour, promise: time.Second,
			promised: make(map[string]map[peer.ID]time.Time),
			breaks:   make(map[peer.ID]int),
			parked:   make(map[peer.ID]time.Time),
		}
		gs.recordOffer("m", "t", a)
		gs.recordOffer("m", "t", b)
		if !gs.iwantAllowed("m", "t", a) {
			t.Fatal("the first ask was refused")
		}
		if p, ok, _ := gs.pickOfferedPeer("m"); !ok || p != b {
			t.Fatalf("picked %q, want %q: the peer with the open promise must be skipped", p, b)
		}
		delete(gs.offers.offers["m"], b)
		if _, ok, waiting := gs.pickOfferedPeer("m"); ok || !waiting {
			t.Fatal("a sole announcer with an open promise must yield no pick and waiting=true")
		}
		// Judgment closes the promise: the announcer is parked and no longer waited for.
		gs.promiseDue("m", a, gs.park.promised["m"][a])
		if _, ok, waiting := gs.pickOfferedPeer("m"); ok || waiting {
			t.Fatal("a parked announcer must be neither picked nor waited for")
		}
	})

	t.Run("an id owns one retry timer at a time", func(t *testing.T) {
		gs := newOfferRouter()
		gs.recordOffer("m", "t", a)
		if !gs.armOfferRetry("m") {
			t.Fatal("the first arm was refused")
		}
		if gs.armOfferRetry("m") {
			t.Fatal("a second timer was armed while one was in flight")
		}
		if !gs.armOfferRetry("m2") {
			t.Fatal("another id was refused its own timer")
		}
	})

	t.Run("a repeated offer refreshes without double counting", func(t *testing.T) {
		gs := newOfferRouter()
		gs.recordOffer("m", "t", a)
		gs.recordOffer("m", "t", a)
		if gs.offers.byPeer[a] != 1 || len(gs.offers.offers["m"]) != 1 {
			t.Fatal("a repeated offer was counted twice")
		}
	})

	t.Run("the heartbeat sweep ages offers out", func(t *testing.T) {
		gs := newOfferRouter()
		gs.recordOffer("m", "t", a)
		gs.offers.offers["m"][a] = time.Now().Add(-offerTTL - time.Second)
		gs.sweepOffers()
		if len(gs.offers.offers) != 0 || gs.offers.byPeer[a] != 0 {
			t.Fatal("an expired offer survived the sweep")
		}
	})

	t.Run("a granted ask is recorded as tried", func(t *testing.T) {
		gs := newOfferRouter()
		gs.recordOffer("m", "t", a)
		gs.recordOffer("m", "t", b)
		if !gs.iwantAllowed("m", "t", a) {
			t.Fatal("the first ask was refused")
		}
		if _, was := gs.offers.tried["m"][a]; !was {
			t.Fatal("the granted ask was not recorded as tried")
		}
		if p, ok, _ := gs.pickOfferedPeer("m"); !ok || p != b {
			t.Fatalf("picked %q for the retry, want the untried %q", p, b)
		}
	})
}

// TestDisciplinedPullStackDelivers runs the full pull path -- pull-only phase forwarding, the
// IWANT discipline, the commitment park and the offer table -- on a small honest mesh: every
// subscriber receives every message.
func TestDisciplinedPullStackDelivers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 10
	hosts := getDefaultHosts(t, n)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 0
	psubs := getGossipsubs(ctx, hosts,
		WithGossipSubParams(params),
		WithPhaseForwarding(func(string) bool { return true }, 0),
		WithIWantDiscipline(nil, 200*time.Millisecond),
		WithIHaveCommitmentPark(1, 400*time.Millisecond, 30*time.Second),
		WithOfferTable())

	const topic = "pull-stack"
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}
	connectAll(t, hosts)
	time.Sleep(2 * time.Second)

	for i := range 5 {
		msg := []byte(fmt.Sprintf("pulled message %d", i))
		if err := psubs[i%n].Publish(topic, msg); err != nil {
			t.Fatal(err)
		}
		for j, sub := range subs {
			rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
			got, err := sub.Next(rctx)
			rcancel()
			if err != nil {
				t.Fatalf("subscriber %d did not receive message %d: %v", j, i, err)
			}
			if string(got.Data) != string(msg) {
				t.Fatalf("subscriber %d got %q, want %q", j, got.Data, msg)
			}
		}
	}
	// Nobody withheld, so nobody may be parked.
	for i, ps := range psubs {
		gs := ps.rt.(*GossipSubRouter)
		done := make(chan int)
		ps.eval <- func() { done <- len(gs.park.parked) }
		if parked := <-done; parked != 0 {
			t.Fatalf("node %d parked %d honest peers", i, parked)
		}
	}
}

func TestOfferTableOffIsStock(t *testing.T) {
	gs := &GossipSubRouter{iwantAsked: make(map[string]time.Time), iwantWindow: time.Minute}
	gs.recordOffer("m", "t", peer.ID("a"))
	gs.noteOfferTried("m", peer.ID("a"))
	gs.clearOffers("m")
	gs.sweepOffers()
	gs.retryOfferedPull("m")
}

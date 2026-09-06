package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func newParkRouter(k int, promise, ttl time.Duration) *GossipSubRouter {
	// No pubsub loop: the tests below call promiseDue or sweepPromises directly, which is what
	// the queue's timer does on the router goroutine. A cancelled context lets any timer that
	// does fire return instead of blocking on eval.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gs := &GossipSubRouter{
		p:           &PubSub{ctx: ctx, eval: make(chan func())},
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		park: &commitmentPark{
			k: k, ttl: ttl, promise: promise,
			promised: make(map[string]map[peer.ID]time.Time),
			breaks:   make(map[peer.ID]breakRecord),
			parked:   make(map[peer.ID]time.Time),
		},
	}
	return gs
}

// ask opens a promise the way iwantAllowed does, without the timer.
func (gs *GossipSubRouter) askForTest(mid string, from peer.ID) time.Time {
	now := time.Now()
	gs.iwantAsked[mid] = now
	if gs.park.promised[mid] == nil {
		gs.park.promised[mid] = make(map[peer.ID]time.Time)
	}
	gs.park.promised[mid][from] = now
	return now
}

func TestCommitmentPark(t *testing.T) {
	a, b := peer.ID("announcer-a"), peer.ID("announcer-b")

	t.Run("an unserved ask breaks the promise and parks at k", func(t *testing.T) {
		gs := newParkRouter(1, time.Second, time.Hour)
		at := gs.askForTest("m", a)
		gs.promiseDue("m", a, at)
		if !gs.peerParked(a) {
			t.Fatal("announcer not parked after one break at k=1")
		}
		if gs.peerParked(b) {
			t.Fatal("an uninvolved peer is parked")
		}
		if _, still := gs.iwantAsked["m"]; still {
			t.Fatal("the discipline slot was not refunded for the current ask")
		}
	})

	t.Run("k breaks are needed", func(t *testing.T) {
		gs := newParkRouter(2, time.Second, time.Hour)
		at := gs.askForTest("m1", a)
		gs.promiseDue("m1", a, at)
		if gs.peerParked(a) {
			t.Fatal("parked after one break at k=2")
		}
		at = gs.askForTest("m2", a)
		gs.promiseDue("m2", a, at)
		if !gs.peerParked(a) {
			t.Fatal("not parked after two breaks at k=2")
		}
	})

	t.Run("a stale break does not count toward k", func(t *testing.T) {
		gs := newParkRouter(2, time.Second, time.Minute)
		gs.park.breaks[a] = breakRecord{n: 1, last: time.Now().Add(-time.Minute)}

		at := gs.askForTest("m", a)
		gs.promiseDue("m", a, at)

		if gs.peerParked(a) {
			t.Fatal("a fresh break was combined with a stale break at k=2")
		}
		if got := gs.park.breaks[a].n; got != 1 {
			t.Fatalf("breaks = %d after stale reset, want 1", got)
		}
	})

	t.Run("delivery keeps the promise", func(t *testing.T) {
		gs := newParkRouter(1, time.Second, time.Hour)
		at := gs.askForTest("m", a)
		gs.noteDelivered("m")
		gs.promiseDue("m", a, at)
		if gs.peerParked(a) {
			t.Fatal("a served ask was charged as a break")
		}
		if gs.park.breaks[a].n != 0 {
			t.Fatalf("breaks = %d after delivery, want 0", gs.park.breaks[a].n)
		}
	})

	t.Run("a peer with an open promise is not re-asked and its promise keeps its deadline", func(t *testing.T) {
		gs := newParkRouter(1, time.Second, time.Hour)
		gs.iwantWindow = time.Millisecond
		if allowed, _ := gs.iwantAllowed("m", "t", a); !allowed {
			t.Fatal("the first ask was refused")
		}
		first := gs.park.promised["m"][a]
		time.Sleep(5 * time.Millisecond) // the discipline window has moved on
		if allowed, _ := gs.iwantAllowed("m", "t", a); allowed {
			t.Fatal("a re-ask to a peer with an open promise was allowed")
		}
		if !gs.park.promised["m"][a].Equal(first) {
			t.Fatal("the re-ask re-stamped the promise")
		}
		if allowed, _ := gs.iwantAllowed("m", "t", b); !allowed {
			t.Fatal("moving on to another announcer was refused")
		}
		// The original timer still judges the first ask.
		gs.promiseDue("m", a, first)
		if !gs.peerParked(a) {
			t.Fatal("the withholder escaped judgment")
		}
	})

	t.Run("a moved-on ask is charged but not refunded", func(t *testing.T) {
		gs := newParkRouter(1, time.Second, time.Hour)
		old := gs.askForTest("m", a)
		time.Sleep(time.Millisecond)
		// The discipline moved on and asked b; a's promise is still open.
		gs.askForTest("m", b)
		gs.promiseDue("m", a, old)
		if !gs.peerParked(a) {
			t.Fatal("a's open promise was not charged after the discipline moved on")
		}
		if at, ok := gs.iwantAsked["m"]; !ok || at.Equal(old) {
			t.Fatal("b's current ask was refunded on a's break")
		}
	})

	t.Run("a park expires", func(t *testing.T) {
		gs := newParkRouter(1, time.Second, 20*time.Millisecond)
		at := gs.askForTest("m", a)
		gs.promiseDue("m", a, at)
		if !gs.peerParked(a) {
			t.Fatal("not parked")
		}
		time.Sleep(30 * time.Millisecond)
		if gs.peerParked(a) {
			t.Fatal("still parked after the ttl")
		}
		if _, kept := gs.park.breaks[a]; kept {
			t.Fatal("the expired park left its breaks behind, so the next break would re-park at once")
		}
	})

	t.Run("the heartbeat sweep lifts expired parks and forgets stale breaks", func(t *testing.T) {
		gs := newParkRouter(2, time.Second, time.Minute)
		// a: parked, park expired. b: one break below k, stale. c: one break, fresh.
		c := peer.ID("announcer-c")
		for _, mid := range []string{"m1", "m2"} {
			gs.promiseDue(mid, a, gs.askForTest(mid, a))
		}
		gs.park.parked[a] = time.Now().Add(-time.Second)
		gs.promiseDue("m3", b, gs.askForTest("m3", b))
		gs.park.breaks[b] = breakRecord{n: 1, last: time.Now().Add(-2 * time.Minute)}
		gs.promiseDue("m4", c, gs.askForTest("m4", c))

		gs.sweepPark()
		if _, parked := gs.park.parked[a]; parked {
			t.Fatal("an expired park survived the sweep")
		}
		if _, kept := gs.park.breaks[a]; kept {
			t.Fatal("the lifted park kept its breaks")
		}
		if _, kept := gs.park.breaks[b]; kept {
			t.Fatal("a stale break below k survived the sweep")
		}
		if b := gs.park.breaks[c]; b.n != 1 {
			t.Fatalf("a fresh break was swept: n = %d, want 1", b.n)
		}
	})

	t.Run("asks share one timer and are judged in ask order from the queue", func(t *testing.T) {
		gs := newParkRouter(10, 10*time.Millisecond, time.Hour)
		for _, mid := range []string{"m1", "m2", "m3"} {
			if allowed, _ := gs.iwantAllowed(mid, "t", a); !allowed {
				t.Fatalf("ask for %s refused", mid)
			}
		}
		if n := len(gs.park.due); n != 3 {
			t.Fatalf("%d judgments queued, want 3", n)
		}
		if !gs.park.armed {
			t.Fatal("no timer armed for the head of the queue")
		}
		time.Sleep(15 * time.Millisecond)
		gs.sweepPromises()
		if gs.park.breaks[a].n != 3 {
			t.Fatalf("breaks = %d after the sweep, want 3", gs.park.breaks[a].n)
		}
		if len(gs.park.due) != 0 || gs.park.armed {
			t.Fatal("an empty queue left a timer armed or entries behind")
		}
	})

	t.Run("a sweep judges only what is due and re-arms for the rest", func(t *testing.T) {
		gs := newParkRouter(10, 20*time.Millisecond, time.Hour)
		if allowed, _ := gs.iwantAllowed("m1", "t", a); !allowed {
			t.Fatal("ask refused")
		}
		time.Sleep(25 * time.Millisecond)
		if allowed, _ := gs.iwantAllowed("m2", "t", a); !allowed {
			t.Fatal("ask refused")
		}
		gs.sweepPromises()
		if gs.park.breaks[a].n != 1 {
			t.Fatalf("breaks = %d, want 1: only m1 was due", gs.park.breaks[a].n)
		}
		if len(gs.park.due) != 1 || gs.park.due[0].mid != "m2" || !gs.park.armed {
			t.Fatal("the not-yet-due ask was not kept queued with a timer armed")
		}
	})

	t.Run("a kept promise is skipped at its deadline", func(t *testing.T) {
		gs := newParkRouter(1, 10*time.Millisecond, time.Hour)
		if allowed, _ := gs.iwantAllowed("m", "t", a); !allowed {
			t.Fatal("ask refused")
		}
		gs.noteDelivered("m")
		time.Sleep(15 * time.Millisecond)
		gs.sweepPromises()
		if gs.peerParked(a) || gs.park.breaks[a].n != 0 {
			t.Fatal("a served ask was charged from the queue")
		}
		if len(gs.park.due) != 0 {
			t.Fatal("the queue kept a judged entry")
		}
	})

	t.Run("the discipline refuses a parked announcer's ids only through handleIHave", func(t *testing.T) {
		// peerParked is consulted at the top of handleIHave; iwantAllowed itself is
		// peer-blind, so a parked peer that is asked through another path is still recorded.
		gs := newParkRouter(1, time.Second, time.Hour)
		at := gs.askForTest("m", a)
		gs.promiseDue("m", a, at)
		if allowed, _ := gs.iwantAllowed("m2", "t", a); !allowed {
			t.Fatal("iwantAllowed is expected to stay peer-blind")
		}
	})
}

func TestCommitmentParkOffIsStock(t *testing.T) {
	gs := &GossipSubRouter{iwantAsked: make(map[string]time.Time), iwantWindow: time.Minute}
	if gs.peerParked(peer.ID("a")) {
		t.Fatal("parked with the park off")
	}
	gs.noteDelivered("m")
	gs.promiseDue("m", peer.ID("a"), time.Now())
	gs.notePromiseBreak("m", peer.ID("a"))
}

package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func newParkRouter(k int, promise, ttl time.Duration) *GossipSubRouter {
	// No pubsub loop: the tests below call promiseDue directly, which is what notePromise's
	// timer does on the router goroutine. A cancelled context lets any timer that does fire
	// return instead of blocking on eval.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gs := &GossipSubRouter{
		p:           &PubSub{ctx: ctx, eval: make(chan func())},
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		park: &commitmentPark{
			k: k, ttl: ttl, promise: promise,
			promised: make(map[string]map[peer.ID]time.Time),
			breaks:   make(map[peer.ID]int),
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

	t.Run("delivery keeps the promise", func(t *testing.T) {
		gs := newParkRouter(1, time.Second, time.Hour)
		at := gs.askForTest("m", a)
		gs.noteDelivered("m")
		gs.promiseDue("m", a, at)
		if gs.peerParked(a) {
			t.Fatal("a served ask was charged as a break")
		}
		if gs.park.breaks[a] != 0 {
			t.Fatalf("breaks = %d after delivery, want 0", gs.park.breaks[a])
		}
	})

	t.Run("a re-made ask supersedes the old promise", func(t *testing.T) {
		gs := newParkRouter(1, time.Second, time.Hour)
		old := gs.askForTest("m", a)
		time.Sleep(time.Millisecond)
		gs.askForTest("m", a)
		gs.promiseDue("m", a, old)
		if gs.peerParked(a) {
			t.Fatal("a superseded promise was charged")
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
	})

	t.Run("the discipline refuses a parked announcer's ids only through handleIHave", func(t *testing.T) {
		// peerParked is consulted at the top of handleIHave; iwantAllowed itself is
		// peer-blind, so a parked peer that is asked through another path is still recorded.
		gs := newParkRouter(1, time.Second, time.Hour)
		at := gs.askForTest("m", a)
		gs.promiseDue("m", a, at)
		if !gs.iwantAllowed("m2", "t", a) {
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

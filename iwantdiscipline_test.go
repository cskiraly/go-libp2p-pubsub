package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestIWantDisciplineOneOutstanding pins the discipline: a second announcer inside the window
// is not asked, and the id opens again once the window has passed.
func TestIWantDisciplineOneOutstanding(t *testing.T) {
	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: 50 * time.Millisecond,
	}
	if !gs.iwantAllowed("m", "t", peer.ID("a")) {
		t.Fatal("the first ask was refused")
	}
	if gs.iwantAllowed("m", "t", peer.ID("b")) {
		t.Fatal("a second ask inside the window was allowed")
	}
	if !gs.iwantAllowed("other", "t", peer.ID("b")) {
		t.Fatal("an unrelated id was refused")
	}
	time.Sleep(60 * time.Millisecond)
	if !gs.iwantAllowed("m", "t", peer.ID("b")) {
		t.Fatal("the id did not reopen after the window")
	}
	// "other" was asked before the sleep and has expired; "m" was just re-asked and is live.
	gs.clearIWantAsked()
	if len(gs.iwantAsked) != 1 {
		t.Fatalf("heartbeat sweep left %d entries, want the one live ask", len(gs.iwantAsked))
	}
	time.Sleep(60 * time.Millisecond)
	gs.clearIWantAsked()
	if len(gs.iwantAsked) != 0 {
		t.Fatalf("heartbeat sweep kept expired entries: %d left", len(gs.iwantAsked))
	}
}

// TestIWantDisciplineOffIsStock is the no-option baseline: without the discipline every ask
// is allowed, exactly as before.
func TestIWantDisciplineOffIsStock(t *testing.T) {
	gs := &GossipSubRouter{}
	for range 3 {
		if !gs.iwantAllowed("m", "t", peer.ID("a")) {
			t.Fatal("an ask was refused with the discipline off")
		}
	}
	gs.clearIWantAsked()
}

// TestIWantDisciplineScopedToTopics pins the matcher: announcements on other topics keep the
// stock behaviour and record nothing.
func TestIWantDisciplineScopedToTopics(t *testing.T) {
	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		iwantMatch:  func(topic string) bool { return topic == "segments" },
	}
	for range 3 {
		if !gs.iwantAllowed("m", "blocks", peer.ID("a")) {
			t.Fatal("an ask on an unmatched topic was refused")
		}
	}
	if len(gs.iwantAsked) != 0 {
		t.Fatal("an unmatched topic recorded an ask")
	}
	if !gs.iwantAllowed("m", "segments", peer.ID("a")) || gs.iwantAllowed("m", "segments", peer.ID("b")) {
		t.Fatal("the matched topic is not disciplined")
	}
}

// TestSelectIWantsRecordsOnlyDispatched pins the select/commit seam.
//
// The discipline records state as a side effect of allowing an id, so selecting freely and
// truncating afterwards would leave phantom entries: ids marked as requested that no IWANT
// ever carried, suppressed until their window expired. The invariant is that the ids
// recorded as asked are exactly the ids dispatched.
func TestSelectIWantsRecordsOnlyDispatched(t *testing.T) {
	const (
		announced = 20
		allowance = 5
	)
	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
	}
	candidates := make([]string, announced)
	for i := range candidates {
		candidates[i] = string(rune('a' + i))
	}

	got := gs.selectIWants(candidates, nil, peer.ID("peer-1"), allowance)

	if len(got) != allowance {
		t.Fatalf("dispatched %d ids, want the full allowance %d", len(got), allowance)
	}
	if recorded := len(gs.iwantAsked); recorded != len(got) {
		t.Fatalf("recorded %d asks for %d dispatched ids: %d phantom entries",
			recorded, len(got), recorded-len(got))
	}
	dispatched := make(map[string]bool, len(got))
	for _, mid := range got {
		dispatched[mid] = true
	}
	for mid := range gs.iwantAsked {
		if !dispatched[mid] {
			t.Fatalf("id %q recorded as asked but never dispatched", mid)
		}
	}
}

// TestSelectIWantsZeroAllowance asks for nothing and must record nothing.
func TestSelectIWantsZeroAllowance(t *testing.T) {
	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
	}
	if got := gs.selectIWants([]string{"a", "b"}, nil, peer.ID("p"), 0); len(got) != 0 {
		t.Fatalf("dispatched %d ids at zero allowance", len(got))
	}
	if len(gs.iwantAsked) != 0 {
		t.Fatalf("recorded %d asks at zero allowance", len(gs.iwantAsked))
	}
}

// TestIWantDisciplineDelivers runs a pull-only mesh (phase degree 0) with the discipline on:
// every subscriber still receives every message, so one ask per id is enough for liveness.
func TestIWantDisciplineDelivers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 10
	hosts := getDefaultHosts(t, n)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 0
	psubs := getGossipsubs(ctx, hosts,
		WithGossipSubParams(params),
		WithPhaseForwarding(func(string) bool { return true }, 0),
		WithIWantDiscipline(nil, time.Second))

	const topic = "discipline"
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
		msg := []byte(fmt.Sprintf("disciplined message %d", i))
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
			if !bytes.Equal(got.Data, msg) {
				t.Fatalf("subscriber %d got %q, want %q", j, got.Data, msg)
			}
		}
	}
}

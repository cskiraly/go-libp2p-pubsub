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
	if allowed, _ := gs.iwantAllowed("m", "t", peer.ID("a")); !allowed {
		t.Fatal("the first ask was refused")
	}
	if allowed, _ := gs.iwantAllowed("m", "t", peer.ID("b")); allowed {
		t.Fatal("a second ask inside the window was allowed")
	}
	if allowed, _ := gs.iwantAllowed("other", "t", peer.ID("b")); !allowed {
		t.Fatal("an unrelated id was refused")
	}
	time.Sleep(60 * time.Millisecond)
	if allowed, _ := gs.iwantAllowed("m", "t", peer.ID("b")); !allowed {
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
		if allowed, attempt := gs.iwantAllowed("m", "t", peer.ID("a")); !allowed || attempt != (iwantAttempt{}) {
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
		if allowed, attempt := gs.iwantAllowed("m", "blocks", peer.ID("a")); !allowed || attempt != (iwantAttempt{}) {
			t.Fatal("an ask on an unmatched topic was refused")
		}
	}
	if len(gs.iwantAsked) != 0 {
		t.Fatal("an unmatched topic recorded an ask")
	}
	first, _ := gs.iwantAllowed("m", "segments", peer.ID("a"))
	second, _ := gs.iwantAllowed("m", "segments", peer.ID("b"))
	if !first || second {
		t.Fatal("the matched topic is not disciplined")
	}
}

// TestIWantDisciplineAskCap: a peer's disciplined asks are capped per heartbeat; other peers
// and undisciplined topics are unaffected, a refused dispatch refunds the count, and the
// heartbeat resets it.
func TestIWantDisciplineAskCap(t *testing.T) {
	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantWindow: time.Minute,
		iwantMatch:  func(topic string) bool { return topic == "t" },
	}
	a, b := peer.ID("a"), peer.ID("b")
	var firstAttempt iwantAttempt
	for i := range disciplinedAsksPerHeartbeat {
		allowed, attempt := gs.iwantAllowed(fmt.Sprintf("m%d", i), "t", a)
		if !allowed {
			t.Fatalf("ask %d was refused below the cap", i)
		}
		if i == 0 {
			firstAttempt = attempt
		}
	}
	if allowed, _ := gs.iwantAllowed("over", "t", a); allowed {
		t.Fatal("an ask beyond the cap was allowed")
	}
	if allowed, _ := gs.iwantAllowed("over", "t", b); !allowed {
		t.Fatal("another peer's ask was refused by a's cap")
	}
	if allowed, _ := gs.iwantAllowed("free", "blocks", a); !allowed {
		t.Fatal("an undisciplined topic was capped")
	}
	if gs.iwantGranted[a] != disciplinedAsksPerHeartbeat {
		t.Fatalf("granted = %d, want exactly the cap", gs.iwantGranted[a])
	}

	// A dispatch refused at admission refunds the ask, and only for the disciplined id.
	gs.undoIWantDispatch(a, []string{"m0", "free"}, map[string]iwantAttempt{"m0": firstAttempt}, promiseTrace{})
	if gs.iwantGranted[a] != disciplinedAsksPerHeartbeat-1 {
		t.Fatalf("granted = %d after one refund, want %d", gs.iwantGranted[a], disciplinedAsksPerHeartbeat-1)
	}
	if allowed, _ := gs.iwantAllowed("m0", "t", a); !allowed {
		t.Fatal("the refund did not reopen the cap")
	}

	gs.clearIWantAsked()
	if len(gs.iwantGranted) != 0 {
		t.Fatal("the heartbeat did not reset the ask counts")
	}
	if allowed, _ := gs.iwantAllowed("next", "t", a); !allowed {
		t.Fatal("the cap did not reopen with the heartbeat")
	}
}

func TestIWantDisciplinePeerEligibilityIsReadOnly(t *testing.T) {
	gs := newOfferRouter()
	p := peer.ID("announcer-a")
	gs.park = newParkRouter(1, time.Second, time.Hour).park
	expired := time.Now().Add(-time.Second)
	gs.park.parked[p] = expired
	gs.park.breaks[p] = breakRecord{n: 1, last: expired.Add(-time.Hour)}
	if gs.iwantPeerEligibility(p, "t") != iwantEligible || gs.iwantPeerEligibility(p, "t") != iwantEligible {
		t.Fatal("an expired park still blocks selection")
	}
	if !gs.park.parked[p].Equal(expired) || gs.park.breaks[p].n != 1 {
		t.Fatal("eligibility mutated the park ledger")
	}
	if len(gs.iwantAsked) != 0 || len(gs.park.promised) != 0 || len(gs.iwantGranted) != 0 {
		t.Fatal("eligibility recorded request state")
	}
	gs.park.parked[p] = time.Now().Add(time.Hour)
	gs.iwantMatch = func(topic string) bool { return topic == "segments" }
	if gs.iwantPeerEligibility(p, "t") != iwantEligible {
		t.Fatal("a park blocks an undisciplined topic")
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

	got, _ := gs.selectIWants(candidates, nil, peer.ID("peer-1"), allowance)

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
	if got, _ := gs.selectIWants([]string{"a", "b"}, nil, peer.ID("p"), 0); len(got) != 0 {
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

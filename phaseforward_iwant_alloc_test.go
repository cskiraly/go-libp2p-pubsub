package pubsub

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// TestSelectIWantsRecordsOnlyDispatched pins the select/commit seam.
//
// Each request policy records state as a side effect of allowing an id, so selecting freely and
// truncating afterwards would leave phantom entries: ids marked as requested that no IWANT ever
// carried, suppressed until their window expired -- and nondeterministically, since the old
// truncation followed a shuffle. The invariant is that the ids recorded as asked are exactly the
// ids dispatched.
func TestSelectIWantsRecordsOnlyDispatched(t *testing.T) {
	const (
		announced = 20
		allowance = 5
	)

	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantCount:  make(map[string]int),
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

// TestSelectIWantsBudgetNotSpentBeyondAllowance is the same invariant for the PR-625-style
// budget: an id whose budget was decremented must have been asked for.
func TestSelectIWantsBudgetNotSpentBeyondAllowance(t *testing.T) {
	const allowance = 3
	gs := &GossipSubRouter{
		iwantBudget:    make(map[string]int),
		iwantBudgetMax: 1,
	}
	candidates := []string{"a", "b", "c", "d", "e", "f", "g"}

	got := gs.selectIWants(candidates, nil, peer.ID("peer-1"), allowance)

	if len(got) != allowance {
		t.Fatalf("dispatched %d, want %d", len(got), allowance)
	}
	spent := 0
	for _, n := range gs.iwantBudget {
		if n < gs.iwantBudgetMax {
			spent++
		}
	}
	if spent != len(got) {
		t.Fatalf("budget spent on %d ids but only %d dispatched", spent, len(got))
	}
}

// TestSelectIWantsZeroAllowance asks for nothing and must record nothing.
func TestSelectIWantsZeroAllowance(t *testing.T) {
	gs := &GossipSubRouter{
		iwantAsked:  make(map[string]time.Time),
		iwantCount:  make(map[string]int),
		iwantWindow: time.Minute,
	}
	if got := gs.selectIWants([]string{"a", "b"}, nil, peer.ID("p"), 0); len(got) != 0 {
		t.Fatalf("dispatched %d ids at zero allowance", len(got))
	}
	if len(gs.iwantAsked) != 0 {
		t.Fatalf("recorded %d asks at zero allowance", len(gs.iwantAsked))
	}
}

// TestRequestGateDeclineIsRetryable pins the ordering that is the request gate's entire
// correctness argument: the gate runs ahead of every mutating request policy, so a decline
// consumes no request state and the same id can be asked for later.
//
// Placed after any policy instead, each decline would burn the id -- recorded as asked, budget
// spent, window opened -- and the block-authority case, which declines precisely so it can come
// back once the block lands, would never get a second chance.
func TestRequestGateDeclineIsRetryable(t *testing.T) {
	candidates := []string{"a", "b", "c", "d"}
	const allowance = 4

	newRouter := func(fn func(topic, mid string) bool) *GossipSubRouter {
		gate := gateFunc(fn)
		return &GossipSubRouter{
			iwantAsked:     make(map[string]time.Time),
			iwantCount:     make(map[string]int),
			iwantWindow:    time.Minute,
			iwantBudget:    make(map[string]int),
			iwantBudgetMax: 1,
			requestGate:    gate,
		}
	}

	t.Run("a closed gate declines everything and records nothing", func(t *testing.T) {
		gs := newRouter(func(string, string) bool { return false })
		if got := gs.selectIWants(candidates, nil, peer.ID("p"), allowance); len(got) != 0 {
			t.Fatalf("dispatched %d ids through a closed gate", len(got))
		}
		if len(gs.iwantAsked) != 0 {
			t.Fatalf("closed gate recorded %d asks", len(gs.iwantAsked))
		}
		if len(gs.iwantBudget) != 0 {
			t.Fatalf("closed gate spent budget on %d ids", len(gs.iwantBudget))
		}
	})

	t.Run("the same router asks once the gate opens", func(t *testing.T) {
		open := false
		gs := newRouter(func(string, string) bool { return open })
		gs.selectIWants(candidates, nil, peer.ID("p"), allowance)
		open = true
		got := gs.selectIWants(candidates, nil, peer.ID("p"), allowance)
		if len(got) != len(candidates) {
			t.Fatalf("after opening the gate, dispatched %d of %d ids -- the decline was not retryable",
				len(got), len(candidates))
		}
	})

	t.Run("a declined id does not consume another id's allowance", func(t *testing.T) {
		gs := newRouter(func(_, mid string) bool { return mid != "a" && mid != "b" })
		got := gs.selectIWants(candidates, nil, peer.ID("p"), 2)
		if len(got) != 2 || got[0] != "c" || got[1] != "d" {
			t.Fatalf("dispatched %v, want [c d]", got)
		}
	})

	t.Run("the gate sees the topic each id was announced under", func(t *testing.T) {
		seen := map[string]string{}
		gs := newRouter(func(topic, mid string) bool {
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

// TestRequestGateCommitsOnlyDispatched pins the other half of the gate contract: an id the gate
// approved but a later policy refused must NOT be reported as committed.
//
// This is the phantom-commit bug one layer up. An application that charges a per-claim budget
// inside its predicate would spend it on requests no IWANT ever carried; with a cap of two content
// variants per structural claim, two refused variants would permanently exclude the honest third.
// So the router reports exactly the dispatched set, and the application charges from that.
func TestRequestGateCommitsOnlyDispatched(t *testing.T) {
	var allowed, committed []string
	gate := &recordingGate{
		allow: func(mid string) bool {
			allowed = append(allowed, mid)
			return true
		},
		committed: func(mids []string) { committed = append(committed, mids...) },
	}

	// iwantBudgetMax 1 with the budget pre-spent on "b" makes a downstream policy refuse an id the
	// gate approved.
	gs := &GossipSubRouter{
		iwantBudget:    map[string]int{"b": 0},
		iwantBudgetMax: 1,
		requestGate:    gate,
	}
	got := gs.selectIWants([]string{"a", "b", "c"}, nil, peer.ID("p"), 3)

	if len(allowed) != 3 {
		t.Fatalf("gate consulted for %v, want all three candidates", allowed)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("dispatched %v, want [a c]", got)
	}
	// selectIWants does not itself commit -- handleIHave does, with the dispatched list -- so the
	// invariant to assert is that the two sets are not the same, and that committing the dispatched
	// list never mentions the refused id.
	gate.committed(got)
	for _, mid := range committed {
		if mid == "b" {
			t.Fatal("an id refused by a request policy was reported as committed")
		}
	}
	if len(committed) != 2 {
		t.Fatalf("committed %v, want exactly the dispatched pair", committed)
	}
}

type recordingGate struct {
	allow     func(mid string) bool
	committed func(mids []string)
}

func (g *recordingGate) Allow(_ peer.ID, _, mid string) bool { return g.allow(mid) }
func (g *recordingGate) Committed(_ peer.ID, mids []string)  { g.committed(mids) }

// gateFunc adapts a bare predicate to RequestGate for tests that only exercise Allow.
type gateFunc func(topic, mid string) bool

func (f gateFunc) Allow(_ peer.ID, topic, mid string) bool { return f(topic, mid) }
func (gateFunc) Committed(peer.ID, []string)               {}

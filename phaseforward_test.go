package pubsub

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

// TestPhaseForwardingTrim pins the push budget: max(0, degree - knownHolders) mesh peers are
// pushed, the rest are returned for announcing, and non-mesh peers are never trimmed.
func TestPhaseForwardingTrim(t *testing.T) {
	mesh := func(n int) map[peer.ID]struct{} {
		m := make(map[peer.ID]struct{}, n)
		for i := range n {
			m[peer.ID(fmt.Sprintf("mesh-%02d", i))] = struct{}{}
		}
		return m
	}
	direct := peer.ID("direct-peer")
	const mid = "m1"
	csum := computeChecksum(mid)

	newRouter := func(holders ...peer.ID) *GossipSubRouter {
		gs := &GossipSubRouter{unwanted: make(map[peer.ID]map[checksum]int)}
		for _, p := range holders {
			gs.unwanted[p] = map[checksum]int{csum: 3}
		}
		return gs
	}
	tosend := func(gmap map[peer.ID]struct{}, extra ...peer.ID) map[peer.ID]struct{} {
		ts := make(map[peer.ID]struct{}, len(gmap)+len(extra))
		for p := range gmap {
			ts[p] = struct{}{}
		}
		for _, p := range extra {
			ts[p] = struct{}{}
		}
		return ts
	}

	t.Run("pushes degree peers and announces the rest", func(t *testing.T) {
		pf := &phaseForwarding{degree: 3}
		gmap := mesh(8)
		ts := tosend(gmap, direct)
		announce := pf.trim(newRouter(), nil, mid, csum, gmap, ts)
		if len(announce) != 5 {
			t.Fatalf("announced %d peers, want 5", len(announce))
		}
		if len(ts) != 4 {
			t.Fatalf("pushing to %d peers, want 3 mesh + 1 direct", len(ts))
		}
		if _, ok := ts[direct]; !ok {
			t.Fatal("the direct peer was trimmed; only mesh members are subject to the budget")
		}
	})

	t.Run("known holders reduce the budget", func(t *testing.T) {
		pf := &phaseForwarding{degree: 3}
		gmap := mesh(8)
		// Two mesh peers already IDONTWANT'd; rpcs would not have them in tosend.
		gs := newRouter(peer.ID("mesh-00"), peer.ID("mesh-01"))
		ts := tosend(gmap)
		delete(ts, peer.ID("mesh-00"))
		delete(ts, peer.ID("mesh-01"))
		announce := pf.trim(gs, nil, mid, csum, gmap, ts)
		if len(ts) != 1 {
			t.Fatalf("pushing to %d peers, want degree 3 minus 2 holders = 1", len(ts))
		}
		if len(announce) != 5 {
			t.Fatalf("announced %d peers, want 5", len(announce))
		}
	})

	t.Run("holders beyond the degree leave a zero budget", func(t *testing.T) {
		pf := &phaseForwarding{degree: 1}
		gmap := mesh(4)
		gs := newRouter(peer.ID("mesh-00"), peer.ID("mesh-01"))
		ts := tosend(gmap)
		delete(ts, peer.ID("mesh-00"))
		delete(ts, peer.ID("mesh-01"))
		announce := pf.trim(gs, nil, mid, csum, gmap, ts)
		if len(ts) != 0 || len(announce) != 2 {
			t.Fatalf("push %d / announce %d, want 0 / 2", len(ts), len(announce))
		}
	})

	t.Run("nothing to trim when the budget covers the mesh", func(t *testing.T) {
		pf := &phaseForwarding{degree: 8}
		gmap := mesh(4)
		ts := tosend(gmap)
		if announce := pf.trim(newRouter(), nil, mid, csum, gmap, ts); announce != nil {
			t.Fatalf("announced %d peers with a budget covering the mesh", len(announce))
		}
	})

	t.Run("the degree may follow the message", func(t *testing.T) {
		// One byte of body per pushed peer: a 3-byte message is pushed to 3 of 8, a 0-byte
		// one to none, and a function that says less than nothing is floored at zero.
		pf := &phaseForwarding{degreeOf: func(m *Message) int { return len(m.GetData()) - 1 }}
		gmap := mesh(8)
		for _, tc := range []struct {
			body      int
			wantPush  int
			wantAnnce int
		}{
			{body: 4, wantPush: 3, wantAnnce: 5},
			{body: 1, wantPush: 0, wantAnnce: 8},
			{body: 0, wantPush: 0, wantAnnce: 8},
		} {
			msg := &Message{Message: &pb.Message{Data: make([]byte, tc.body)}}
			ts := tosend(gmap)
			announce := pf.trim(newRouter(), msg, mid, csum, gmap, ts)
			if len(ts) != tc.wantPush || len(announce) != tc.wantAnnce {
				t.Fatalf("body %d: push %d / announce %d, want %d / %d", tc.body, len(ts), len(announce), tc.wantPush, tc.wantAnnce)
			}
		}
	})

	t.Run("the pushed subset is stable and varies by message", func(t *testing.T) {
		pf := &phaseForwarding{degree: 2}
		gmap := mesh(8)
		pushed := func(id string) map[peer.ID]struct{} {
			ts := tosend(gmap)
			pf.trim(newRouter(), nil, id, computeChecksum(id), gmap, ts)
			return ts
		}
		a, b := pushed("m1"), pushed("m1")
		for p := range a {
			if _, ok := b[p]; !ok {
				t.Fatal("the pushed subset for one message differs between two calls")
			}
		}
		same := true
		for id := 2; id < 20 && same; id++ {
			c := pushed(fmt.Sprintf("m%d", id))
			for p := range c {
				if _, ok := a[p]; !ok {
					same = false
				}
			}
		}
		if same {
			t.Fatal("19 different messages all pushed to the same peers; the rank is not per message")
		}
	})
}

// TestPhaseForwardingDelivers runs a small mesh under phase degree 0 (pull-only): every
// message is announced, nothing is pushed unasked, and every subscriber still receives it
// through the IHAVE/IWANT path.
func TestPhaseForwardingDelivers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 10
	hosts := getDefaultHosts(t, n)
	params := DefaultGossipSubParams()
	params.IDontWantMessageThreshold = 0
	psubs := getGossipsubs(ctx, hosts,
		WithGossipSubParams(params),
		WithPhaseForwarding(func(string) bool { return true }, 0))

	const topic = "phase"
	var subs []*Subscription
	for _, ps := range psubs {
		sub, err := ps.Subscribe(topic)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}
	connectAll(t, hosts)
	// let the meshes form
	time.Sleep(2 * time.Second)

	for i := range 5 {
		msg := []byte(fmt.Sprintf("phase message %d", i))
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

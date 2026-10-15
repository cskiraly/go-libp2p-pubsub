package pubsub

import (
	"context"
	"testing"
	"time"
)

func TestUplinkGateHoldsLowerClass(t *testing.T) {
	g := newUplinkGate(time.Second)
	high, low := newRpcQueue(10), newRpcQueue(10)
	high.gate, low.gate = g, g
	if _, err := high.PushClass(&RPC{}, defaultClass, false, false); err != nil {
		t.Fatal(err)
	}
	released := make(chan time.Duration)
	start := time.Now()
	go func() {
		g.wait(context.Background(), defaultClass+2, low.classCounts)
		released <- time.Since(start)
	}()
	select {
	case <-released:
		t.Fatal("lower class passed while a higher class was pending for another peer")
	case <-time.After(50 * time.Millisecond):
	}
	e, err := high.pop(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	g.done(e.class) // the high-class write finished
	if d := <-released; d > 500*time.Millisecond {
		t.Fatalf("released after %v, want right after the high-class write", d)
	}
}

func TestUplinkGateIgnoresOwnQueueAndTimesOut(t *testing.T) {
	g := newUplinkGate(30 * time.Millisecond)
	q, other := newRpcQueue(10), newRpcQueue(10)
	q.gate, other.gate = g, g
	// Higher class in the caller's own queue: no wait.
	q.PushClass(&RPC{}, defaultClass, false, false)
	start := time.Now()
	g.wait(context.Background(), defaultClass+2, q.classCounts)
	if d := time.Since(start); d > 20*time.Millisecond {
		t.Fatalf("waited %v on its own queue", d)
	}
	// Higher class for another peer that never drains: bounded by maxWait.
	other.PushClass(&RPC{}, defaultClass, false, false)
	start = time.Now()
	g.wait(context.Background(), defaultClass+2, q.classCounts)
	if d := time.Since(start); d < 25*time.Millisecond || d > 300*time.Millisecond {
		t.Fatalf("waited %v, want about maxWait", d)
	}
	// Closing a queue releases its count.
	other.Close()
	q.Close()
	for c, n := range g.pending {
		if n != 0 {
			t.Fatalf("class %d pending %d after close", c, n)
		}
	}
}

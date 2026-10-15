package pubsub

import (
	"context"
	"sync"
	"time"
)

// uplinkGate extends the per-peer priority classes to the node's whole uplink:
// a peer's writer holds back an RPC while a higher class (other than urgent)
// is pending for any other peer, so the peers' writers stop competing for the
// uplink with lower-class data. It counts the RPCs queued or being written,
// by class, over all peers. maxWait bounds the hold, so a slow peer with a
// higher-class backlog cannot starve the others.
type uplinkGate struct {
	mu      sync.Mutex
	pending [numQueueClasses]int
	changed chan struct{} // closed and replaced whenever a count drops
	maxWait time.Duration
}

func newUplinkGate(maxWait time.Duration) *uplinkGate {
	return &uplinkGate{changed: make(chan struct{}), maxWait: maxWait}
}

func (g *uplinkGate) add(class int) {
	g.mu.Lock()
	g.pending[class]++
	g.mu.Unlock()
}

func (g *uplinkGate) done(class int) {
	g.mu.Lock()
	g.pending[class]--
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()
}

// wait blocks while an RPC of a class above class (urgent excluded) is
// pending for another peer, up to maxWait. own reports the RPCs per class
// still in the caller's own queue, which only the caller can send; it is
// called without g.mu held (push takes the queue lock, then g.mu).
func (g *uplinkGate) wait(ctx context.Context, class int, own func() [numQueueClasses]int) {
	if class <= defaultClass {
		return
	}
	var timeout <-chan time.Time
	for {
		mine := own()
		g.mu.Lock()
		blocked := false
		for c := urgentClass + 1; c < class; c++ {
			if g.pending[c]-mine[c] > 0 {
				blocked = true
				break
			}
		}
		ch := g.changed
		g.mu.Unlock()
		if !blocked {
			return
		}
		if timeout == nil {
			t := time.NewTimer(g.maxWait)
			defer t.Stop()
			timeout = t.C
		}
		select {
		case <-ch:
		case <-timeout:
			return
		case <-ctx.Done():
			return
		}
	}
}

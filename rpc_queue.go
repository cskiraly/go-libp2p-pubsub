package pubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrQueueCancelled    = errors.New("rpc queue operation cancelled")
	ErrQueueClosed       = errors.New("rpc queue closed")
	ErrQueueFull         = errors.New("rpc queue full")
	ErrQueuePushOnClosed = errors.New("push on closed rpc queue")
)

// rankedRPC is a normal-lane entry with its drain rank. Rank 0 is the neutral default, so a
// queue where nothing sets ranks drains in exact FIFO order (first minimal wins the scan).
type rankedRPC struct {
	rpc  *RPC
	rank int
}

type priorityQueue struct {
	normal   []rankedRPC
	priority []*RPC
}

func (q *priorityQueue) Len() int {
	return len(q.normal) + len(q.priority)
}

func (q *priorityQueue) NormalPush(rpc *RPC) {
	q.RankedPush(rpc, 0)
}

func (q *priorityQueue) RankedPush(rpc *RPC, rank int) {
	q.normal = append(q.normal, rankedRPC{rpc: rpc, rank: rank})
}

func (q *priorityQueue) PriorityPush(rpc *RPC) {
	q.priority = append(q.priority, rpc)
}

func (q *priorityQueue) Pop() *RPC {
	var rpc *RPC

	if len(q.priority) > 0 {
		rpc = q.priority[0]
		q.priority[0] = nil
		q.priority = q.priority[1:]
	} else if len(q.normal) > 0 {
		// Lowest rank first; FIFO within a rank (the scan keeps the first minimum). With
		// every rank 0 this is plain FIFO, so an unranked queue is byte-identical to the
		// old behaviour, and reordering can only ever happen where messages were already
		// waiting — the uncontended path is untouched by construction. Linear scan: the
		// queue is bounded (default 600) and normally holds a handful.
		best := 0
		for i := 1; i < len(q.normal); i++ {
			if q.normal[i].rank < q.normal[best].rank {
				best = i
			}
		}
		rpc = q.normal[best].rpc
		q.normal[best] = rankedRPC{}
		q.normal = append(q.normal[:best], q.normal[best+1:]...)
	}

	return rpc
}

type rpcQueue struct {
	dataAvailable  sync.Cond
	spaceAvailable sync.Cond
	// Mutex used to access queue
	queueMu sync.Mutex
	queue   priorityQueue

	closed  bool
	maxSize int

	// Passive outbound observation (measurement only): bytes pushed and not yet popped by the
	// writer and their peak, under queueMu; the writer's time blocked in the stream write and
	// its write count, written by the writer goroutine alone.
	queuedBytes  int64
	peakQueued   int64
	writeBlocked atomic.Int64 // nanoseconds
	writes       atomic.Int64
	maxWrite     atomic.Int64 // nanoseconds
}

func newRpcQueue(maxSize int) *rpcQueue {
	q := &rpcQueue{maxSize: maxSize}
	q.dataAvailable.L = &q.queueMu
	q.spaceAvailable.L = &q.queueMu
	return q
}

func (q *rpcQueue) Push(rpc *RPC, block bool) error {
	return q.push(rpc, false, block, 0)
}

// RankedPush enqueues on the normal lane with a drain rank; lower ranks drain first.
func (q *rpcQueue) RankedPush(rpc *RPC, rank int, block bool) error {
	return q.push(rpc, false, block, rank)
}

func (q *rpcQueue) UrgentPush(rpc *RPC, block bool) error {
	return q.push(rpc, true, block, 0)
}

func (q *rpcQueue) push(rpc *RPC, urgent bool, block bool, rank int) error {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		panic(ErrQueuePushOnClosed)
	}

	for q.queue.Len() == q.maxSize {
		if block {
			q.spaceAvailable.Wait()
			// It can receive a signal because the queue is closed.
			if q.closed {
				panic(ErrQueuePushOnClosed)
			}
		} else {
			return ErrQueueFull
		}
	}
	if urgent {
		q.queue.PriorityPush(rpc)
	} else {
		q.queue.RankedPush(rpc, rank)
	}
	q.queuedBytes += int64(rpc.size)
	if q.queuedBytes > q.peakQueued {
		q.peakQueued = q.queuedBytes
	}

	q.dataAvailable.Signal()
	return nil
}

// Note that, when the queue is empty and there are two blocked Pop calls, it
// doesn't mean that the first Pop will get the item from the next Push. The
// second Pop will probably get it instead.
func (q *rpcQueue) Pop(ctx context.Context) (*RPC, error) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	unregisterAfterFunc := context.AfterFunc(ctx, func() {
		// Wake up all the waiting routines. The only routine that correponds
		// to this Pop call will return from the function. Note that this can
		// be expensive, if there are too many waiting routines.
		q.dataAvailable.Broadcast()
	})
	defer unregisterAfterFunc()

	for q.queue.Len() == 0 {
		select {
		case <-ctx.Done():
			return nil, ErrQueueCancelled
		default:
		}
		q.dataAvailable.Wait()
		// It can receive a signal because the queue is closed.
		if q.closed {
			return nil, ErrQueueClosed
		}
	}
	rpc := q.queue.Pop()
	q.queuedBytes -= int64(rpc.size)
	q.spaceAvailable.Signal()
	return rpc, nil
}

func (q *rpcQueue) Close() {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	q.closed = true
	q.dataAvailable.Broadcast()
	q.spaceAvailable.Broadcast()
}

// noteWrite accounts one stream write by the writer goroutine: d is the time it blocked.
func (q *rpcQueue) noteWrite(d time.Duration) {
	q.writes.Add(1)
	if d <= 0 {
		return
	}
	q.writeBlocked.Add(int64(d))
	if int64(d) > q.maxWrite.Load() {
		q.maxWrite.Store(int64(d))
	}
}

// outboundStat reads the queue's observation counters.
func (q *rpcQueue) outboundStat() OutboundStat {
	q.queueMu.Lock()
	queued, peak := q.queuedBytes, q.peakQueued
	q.queueMu.Unlock()
	return OutboundStat{QueuedBytes: queued, PeakQueuedBytes: peak, WriteBlocked: time.Duration(q.writeBlocked.Load()),
		Writes: q.writes.Load(), MaxWrite: time.Duration(q.maxWrite.Load())}
}

// Occupancy returns the current queue length and its capacity, for the adaptive hedge's
// local congestion signal (measurement-plan section 15, Job 2).
func (q *rpcQueue) Occupancy() (length, capacity int) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	return q.queue.Len(), q.maxSize
}

package pubsub

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrQueueCancelled    = errors.New("rpc queue operation cancelled")
	ErrQueueClosed       = errors.New("rpc queue closed")
	ErrQueueFull         = errors.New("rpc queue full")
	ErrQueuePushOnClosed = errors.New("push on closed rpc queue")
)

const (
	// urgentClass is the queue class of urgent RPCs (e.g. IDONTWANT); it is
	// always served first.
	urgentClass = 0
	// defaultClass is the queue class of every other RPC when no priority
	// function is configured.
	defaultClass = 1
	// MaxTopicPriority is the largest value a TopicPriorityFn may return;
	// larger values are clamped.
	MaxTopicPriority = 14
	numQueueClasses  = defaultClass + MaxTopicPriority + 1
)

type queuedRPC struct {
	rpc   *RPC
	class int
	at    time.Time
}

// priorityQueue is a strict priority queue of FIFO classes; class 0 is
// served first.
type priorityQueue struct {
	classes [numQueueClasses][]queuedRPC
	len     int
}

func (q *priorityQueue) Len() int {
	return q.len
}

func (q *priorityQueue) push(e queuedRPC) {
	q.classes[e.class] = append(q.classes[e.class], e)
	q.len++
}

func (q *priorityQueue) Pop() (queuedRPC, bool) {
	for c := range q.classes {
		if len(q.classes[c]) > 0 {
			e := q.classes[c][0]
			q.classes[c][0] = queuedRPC{}
			q.classes[c] = q.classes[c][1:]
			q.len--
			return e, true
		}
	}
	return queuedRPC{}, false
}

// evictBelow removes the newest RPC of the lowest-priority non-empty class
// that is strictly below class, if there is one.
func (q *priorityQueue) evictBelow(class int) (queuedRPC, bool) {
	for c := len(q.classes) - 1; c > class; c-- {
		if n := len(q.classes[c]); n > 0 {
			e := q.classes[c][n-1]
			q.classes[c][n-1] = queuedRPC{}
			q.classes[c] = q.classes[c][:n-1]
			q.len--
			return e, true
		}
	}
	return queuedRPC{}, false
}

type rpcQueue struct {
	dataAvailable  sync.Cond
	spaceAvailable sync.Cond
	// Mutex used to access queue
	queueMu sync.Mutex
	queue   priorityQueue

	closed  bool
	maxSize int
	// gate, when set, counts this queue's RPCs in the node-wide uplink gate.
	gate *uplinkGate
}

func newRpcQueue(maxSize int) *rpcQueue {
	q := &rpcQueue{maxSize: maxSize}
	q.dataAvailable.L = &q.queueMu
	q.spaceAvailable.L = &q.queueMu
	return q
}

func (q *rpcQueue) Push(rpc *RPC, block bool) error {
	_, err := q.push(rpc, defaultClass, false, block)
	return err
}

func (q *rpcQueue) UrgentPush(rpc *RPC, block bool) error {
	_, err := q.push(rpc, urgentClass, false, block)
	return err
}

// PushClass enqueues rpc in the given class. When the queue is full and evict
// is set, the newest RPC of the lowest class below class is dropped to make
// room and returned; otherwise a full queue fails with ErrQueueFull (or blocks).
func (q *rpcQueue) PushClass(rpc *RPC, class int, evict bool, block bool) (*RPC, error) {
	return q.push(rpc, class, evict, block)
}

func (q *rpcQueue) push(rpc *RPC, class int, evict bool, block bool) (*RPC, error) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		panic(ErrQueuePushOnClosed)
	}

	var evicted *RPC
	for q.queue.Len() >= q.maxSize {
		if evict {
			if e, ok := q.queue.evictBelow(class); ok {
				evicted = e.rpc
				if q.gate != nil {
					q.gate.done(e.class)
				}
				continue
			}
		}
		if block {
			q.spaceAvailable.Wait()
			// It can receive a signal because the queue is closed.
			if q.closed {
				panic(ErrQueuePushOnClosed)
			}
		} else {
			return nil, ErrQueueFull
		}
	}
	q.queue.push(queuedRPC{rpc: rpc, class: class, at: time.Now()})
	if q.gate != nil {
		q.gate.add(class)
	}

	q.dataAvailable.Signal()
	return evicted, nil
}

// Note that, when the queue is empty and there are two blocked Pop calls, it
// doesn't mean that the first Pop will get the item from the next Push. The
// second Pop will probably get it instead.
func (q *rpcQueue) Pop(ctx context.Context) (*RPC, error) {
	e, err := q.pop(ctx)
	return e.rpc, err
}

func (q *rpcQueue) pop(ctx context.Context) (queuedRPC, error) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.closed {
		return queuedRPC{}, ErrQueueClosed
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
			return queuedRPC{}, ErrQueueCancelled
		default:
		}
		q.dataAvailable.Wait()
		// It can receive a signal because the queue is closed.
		if q.closed {
			return queuedRPC{}, ErrQueueClosed
		}
	}
	e, _ := q.queue.Pop()
	q.spaceAvailable.Signal()
	return e, nil
}

// classCounts returns the number of queued RPCs per class.
func (q *rpcQueue) classCounts() (n [numQueueClasses]int) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	for c := range q.queue.classes {
		n[c] = len(q.queue.classes[c])
	}
	return n
}

func (q *rpcQueue) Close() {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if q.gate != nil && !q.closed {
		for c := range q.queue.classes {
			for range q.queue.classes[c] {
				q.gate.done(c)
			}
		}
	}
	q.closed = true
	q.dataAvailable.Broadcast()
	q.spaceAvailable.Broadcast()
}

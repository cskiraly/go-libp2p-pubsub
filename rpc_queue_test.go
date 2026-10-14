package pubsub

import (
	"context"
	"testing"
	"time"
)

func TestNewRpcQueue(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)
		if q.maxSize != maxSize {
			t.Fatalf("rpc queue has wrong max size, expected %d but got %d", maxSize, q.maxSize)
		}
		if q.dataAvailable.L != &q.queueMu {
			t.Fatalf("the dataAvailable field of rpc queue has an incorrect mutex")
		}
		if q.spaceAvailable.L != &q.queueMu {
			t.Fatalf("the spaceAvailable field of rpc queue has an incorrect mutex")
		}
	})
}

func TestRpcQueueUrgentPush(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)

		rpc1 := &RPC{}
		rpc2 := &RPC{}
		rpc3 := &RPC{}
		rpc4 := &RPC{}
		q.Push(rpc1, true)
		q.UrgentPush(rpc2, true)
		q.Push(rpc3, true)
		q.UrgentPush(rpc4, true)
		pop1, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pop2, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pop3, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pop4, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if pop1 != rpc2 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
		if pop2 != rpc4 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
		if pop3 != rpc1 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
		if pop4 != rpc3 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
	})
}

func TestRpcQueuePushThenPop(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)

		rpc1 := &RPC{}
		rpc2 := &RPC{}
		q.Push(rpc1, true)
		q.Push(rpc2, true)
		pop1, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pop2, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if pop1 != rpc1 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
		if pop2 != rpc2 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
	})
}

func TestRpcQueuePopThenPush(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)

		rpc1 := &RPC{}
		rpc2 := &RPC{}
		go func() {
			// Wait to make sure the main goroutine is blocked.
			time.Sleep(1 * time.Millisecond)
			q.Push(rpc1, true)
			q.Push(rpc2, true)
		}()
		pop1, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		pop2, err := q.Pop(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if pop1 != rpc1 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
		if pop2 != rpc2 {
			t.Fatalf("get wrong item from rpc queue Pop")
		}
	})
}

func TestRpcQueueBlockPushWhenFull(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 1
		q := newRpcQueue(maxSize)

		finished := make(chan struct{})
		q.Push(&RPC{}, true)
		go func() {
			defer func() { recover() }() // recover from close
			q.Push(&RPC{}, true)
			finished <- struct{}{}
		}()
		// Wait to make sure the goroutine is blocked.
		time.Sleep(1 * time.Millisecond)
		select {
		case <-finished:
			t.Fatalf("blocking rpc queue Push is not blocked when it is full")
		default:
		}
		// Unblock the goroutine so synctest can exit cleanly
		q.Close()
	})
}

func TestRpcQueueNonblockPushWhenFull(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 1
		q := newRpcQueue(maxSize)

		q.Push(&RPC{}, true)
		err := q.Push(&RPC{}, false)
		if err != ErrQueueFull {
			t.Fatalf("non-blocking rpc queue Push returns wrong error when it is full")
		}
	})
}

func TestRpcQueuePushAfterClose(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)
		q.Close()

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("rpc queue Push does not panick after closed")
			}
		}()
		q.Push(&RPC{}, true)
	})
}

func TestRpcQueuePopAfterClose(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)
		q.Close()
		_, err := q.Pop(context.Background())
		if err != ErrQueueClosed {
			t.Fatalf("rpc queue Pop returns wrong error after closed")
		}
	})
}

func TestRpcQueueCloseWhilePush(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 1
		q := newRpcQueue(maxSize)
		q.Push(&RPC{}, true)

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("rpc queue Push does not panick when it's closed on the fly")
			}
		}()

		go func() {
			// Wait to make sure the main goroutine is blocked.
			time.Sleep(1 * time.Millisecond)
			q.Close()
		}()
		q.Push(&RPC{}, true)
	})
}

func TestRpcQueueCloseWhilePop(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)
		go func() {
			// Wait to make sure the main goroutine is blocked.
			time.Sleep(1 * time.Millisecond)
			q.Close()
		}()
		_, err := q.Pop(context.Background())
		if err != ErrQueueClosed {
			t.Fatalf("rpc queue Pop returns wrong error when it's closed on the fly")
		}
	})
}

func TestRpcQueuePushWhenFullThenPop(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 1
		q := newRpcQueue(maxSize)

		q.Push(&RPC{}, true)
		go func() {
			// Wait to make sure the main goroutine is blocked.
			time.Sleep(1 * time.Millisecond)
			q.Pop(context.Background())
		}()
		q.Push(&RPC{}, true)
	})
}

func TestRpcQueueCancelPop(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		maxSize := 32
		q := newRpcQueue(maxSize)
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			// Wait to make sure the main goroutine is blocked.
			time.Sleep(1 * time.Millisecond)
			cancel()
		}()
		_, err := q.Pop(ctx)
		if err != ErrQueueCancelled {
			t.Fatalf("rpc queue Pop returns wrong error when it's cancelled")
		}
	})
}

func TestRpcQueuePushClassOrder(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		q := newRpcQueue(32)
		low1, high, low2, urgent, mid := &RPC{}, &RPC{}, &RPC{}, &RPC{}, &RPC{}
		q.PushClass(low1, defaultClass+5, true, false)
		q.PushClass(high, defaultClass, true, false)
		q.PushClass(low2, defaultClass+5, true, false)
		q.UrgentPush(urgent, false)
		q.PushClass(mid, defaultClass+2, true, false)
		for i, want := range []*RPC{urgent, high, mid, low1, low2} {
			got, err := q.Pop(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("pop %d: got the wrong rpc", i)
			}
		}
	})
}

func TestRpcQueuePushClassEvicts(t *testing.T) {
	synctestTest(t, func(t *testing.T) {
		q := newRpcQueue(3)
		lowOld, lowNew, mid := &RPC{}, &RPC{}, &RPC{}
		q.PushClass(lowOld, defaultClass+3, true, false)
		q.PushClass(mid, defaultClass+1, true, false)
		q.PushClass(lowNew, defaultClass+3, true, false)

		// A full queue evicts the newest RPC of the lowest lower class.
		high := &RPC{}
		evicted, err := q.PushClass(high, defaultClass, true, false)
		if err != nil {
			t.Fatal(err)
		}
		if evicted != lowNew {
			t.Fatal("expected the newest low priority rpc to be evicted")
		}

		// Nothing below its own class: the RPC is refused, as without eviction.
		evicted, err = q.PushClass(&RPC{}, defaultClass+3, true, false)
		if err != ErrQueueFull || evicted != nil {
			t.Fatalf("expected ErrQueueFull and no eviction, got %v, %v", err, evicted)
		}
		// Same class is not evicted either.
		if _, err = q.PushClass(&RPC{}, defaultClass+1, false, false); err != ErrQueueFull {
			t.Fatalf("expected ErrQueueFull without eviction, got %v", err)
		}

		for i, want := range []*RPC{high, mid, lowOld} {
			got, err := q.Pop(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("pop %d: got the wrong rpc", i)
			}
		}
	})
}

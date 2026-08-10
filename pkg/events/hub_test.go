// Copyright (C) 2026 The OpenEverest Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package events

import (
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

// TestHub_SlowSubscriberDropDoesNotPanic reproduces issue #2726. A subscriber
// that never drains its channel must be dropped without triggering
// "close of closed channel" when broadcast repeatedly hits the full buffer
// and the cancel callback is invoked afterwards.
func TestHub_SlowSubscriberDropDoesNotPanic(t *testing.T) {
	t.Parallel()

	h := NewHub(zaptest.NewLogger(t).Sugar(), nil)
	_, cancel := h.Subscribe(nil, nil)

	// Overflow the buffer enough times that multiple broadcast goroutines
	// try to close the same subscriber concurrently.
	for i := 0; i < defaultBufferSize+32; i++ {
		h.Publish(Event{Type: InstanceCreated, Namespace: "default"})
	}

	// Wait for the async cleanup goroutines spawned by broadcast to run
	// so the ensuing cancel() collides with them on close.
	waitForSubscriberRemoved(t, h)

	// Must not panic even though broadcast already closed the channel.
	cancel()
}

// TestHub_CancelAndSlowDropRace deterministically exercises the race
// between the cancel path and broadcast's slow-subscriber cleanup path.
// Repeated iterations under `-race` make it reliably trigger the bug on
// unpatched code.
func TestHub_CancelAndSlowDropRace(t *testing.T) {
	t.Parallel()

	for i := 0; i < 200; i++ {
		h := NewHub(zaptest.NewLogger(t).Sugar(), nil)
		_, cancel := h.Subscribe(nil, nil)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			for j := 0; j < defaultBufferSize+8; j++ {
				h.Publish(Event{Type: InstanceCreated, Namespace: "default"})
			}
		}()

		go func() {
			defer wg.Done()
			cancel()
		}()

		wg.Wait()
		// A second cancel must also be safe (idempotent shutdown).
		cancel()
	}
}

// TestHub_CancelIsIdempotent verifies the returned cancel function can be
// invoked more than once without panicking, matching the safety promise of
// the sync.Once-guarded shutdown.
func TestHub_CancelIsIdempotent(t *testing.T) {
	t.Parallel()

	h := NewHub(zaptest.NewLogger(t).Sugar(), nil)
	_, cancel := h.Subscribe(nil, nil)

	cancel()
	cancel()
}

// TestHub_DeliversMatchingEvents is a sanity check that the fix does not
// alter normal event delivery semantics.
func TestHub_DeliversMatchingEvents(t *testing.T) {
	t.Parallel()

	h := NewHub(zaptest.NewLogger(t).Sugar(), nil)
	ch, cancel := h.Subscribe([]Type{InstanceCreated}, []string{"default"})
	defer cancel()

	h.Publish(Event{Type: InstanceCreated, Namespace: "default"})
	h.Publish(Event{Type: InstanceDeleted, Namespace: "default"})   // filtered out by type
	h.Publish(Event{Type: InstanceCreated, Namespace: "elsewhere"}) // filtered out by namespace

	select {
	case evt := <-ch:
		if evt.Type != InstanceCreated || evt.Namespace != "default" {
			t.Fatalf("unexpected event delivered: %+v", evt)
		}
	case <-time.After(time.Second):
		t.Fatal("expected event was not delivered")
	}

	select {
	case evt := <-ch:
		t.Fatalf("unexpected extra event: %+v", evt)
	case <-time.After(50 * time.Millisecond):
	}
}

// waitForSubscriberRemoved polls until the hub reports no subscribers,
// deterministically synchronising with broadcast's asynchronous cleanup
// goroutine instead of relying on a fixed sleep.
func waitForSubscriberRemoved(t *testing.T, h *Hub) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.RLock()
		n := len(h.subscribers)
		h.mu.RUnlock()
		if n == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("subscriber was not removed by slow-subscriber cleanup within timeout")
}

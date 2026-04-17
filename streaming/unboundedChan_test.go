package streaming

import (
	"sync"
	"testing"
)

func TestUnboundedChanCloseIsIdempotent(t *testing.T) {
	q := NewUnboundedChan(1)

	if !q.Close() {
		t.Fatal("first close should succeed")
	}
	if q.Close() {
		t.Fatal("second close should be ignored")
	}
	if !q.IsClosed() {
		t.Fatal("queue should be marked as closed")
	}
}

func TestUnboundedChanCloseConcurrent(t *testing.T) {
	q := NewUnboundedChan(1)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				q.Close()
			}
		}()
	}

	wg.Wait()
	if !q.IsClosed() {
		t.Fatal("queue should be marked as closed")
	}
}

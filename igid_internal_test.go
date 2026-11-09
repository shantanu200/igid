package igid

import (
	"testing"
	"time"
)

func TestMonotonicClockTracksWallClock(t *testing.T) {
	clock := monotonicClock()

	before := time.Now().UnixMilli()
	got := clock()
	after := time.Now().UnixMilli()
	// Allow 1ms either side for rounding between the two clocks.
	if got < before-1 || got > after+1 {
		t.Errorf("clock() = %d, want within [%d, %d]", got, before-1, after+1)
	}
}

func TestMonotonicClockNeverDecreases(t *testing.T) {
	clock := monotonicClock()
	prev := clock()
	deadline := time.Now().Add(20 * time.Millisecond)
	for time.Now().Before(deadline) {
		now := clock()
		if now < prev {
			t.Fatalf("clock went from %d to %d", prev, now)
		}
		prev = now
	}
}

func TestNewUsesMonotonicClockByDefault(t *testing.T) {
	n, err := New(0)
	if err != nil {
		t.Fatal(err)
	}
	// A wall-clock step can't be simulated in a test, so this only checks
	// that the default clock is set and advances with real time.
	a := n.nowFn()
	time.Sleep(5 * time.Millisecond)
	b := n.nowFn()
	if d := b - a; d < 5 || d > 1000 {
		t.Errorf("default clock advanced %dms over a 5ms sleep", d)
	}
}

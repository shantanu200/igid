package igid_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync"
	"testing"
	"time"

	"github.com/shantanu200/igid"
)

// maxElapsedMs mirrors the unexported limit: 41 timestamp bits minus the sign bit.
const maxElapsedMs = 1<<(63-igid.ShardBits-igid.SeqBits) - 1

var (
	testEpochMs = int64(1_700_000_000_000)
	testEpoch   = time.UnixMilli(testEpochMs)
)

// fakeClock is a manual clock. Reads first return any scripted values in
// order; after that each read returns the current value and then advances
// it by step, so a non-zero step lets ID's wait loops finish.
type fakeClock struct {
	mu     sync.Mutex
	ms     int64
	step   int64
	script []int64
	reads  int
}

func newFakeClock(ms int64) *fakeClock { return &fakeClock{ms: ms} }

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	if len(c.script) > 0 {
		t := c.script[0]
		c.script = c.script[1:]
		c.ms = t
		return time.UnixMilli(t)
	}
	t := c.ms
	c.ms += c.step
	return time.UnixMilli(t)
}

// then makes the next reads return these values, in order.
func (c *fakeClock) then(ms ...int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.script = append(c.script, ms...)
}

func (c *fakeClock) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

func (c *fakeClock) set(ms int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ms = ms
}

func (c *fakeClock) setStep(step int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.step = step
}

func newTestNode(t *testing.T, shard int64, clk *fakeClock, opts ...igid.Option) *igid.Node {
	t.Helper()
	opts = append([]igid.Option{igid.WithEpoch(testEpoch), igid.WithClock(clk.now)}, opts...)
	n, err := igid.New(shard, opts...)
	if err != nil {
		t.Fatalf("New(%d): %v", shard, err)
	}
	return n
}

func mustID(t *testing.T, n *igid.Node) igid.ID {
	t.Helper()
	id, err := n.ID()
	if err != nil {
		t.Fatalf("ID: %v", err)
	}
	return id
}

func TestNewValidation(t *testing.T) {
	clk := newFakeClock(testEpochMs + 1000)

	tests := []struct {
		name  string
		shard int64
		opts  []igid.Option
		want  error
	}{
		{"shard zero", 0, nil, nil},
		{"shard max", igid.MaxShard, nil, nil},
		{"shard negative", -1, nil, igid.ErrInvalidShard},
		{"shard too large", igid.MaxShard + 1, nil, igid.ErrInvalidShard},
		{"epoch equals now", 0, []igid.Option{igid.WithEpoch(time.UnixMilli(testEpochMs + 1000))}, nil},
		{"epoch in future", 0, []igid.Option{igid.WithEpoch(time.UnixMilli(testEpochMs + 1001))}, igid.ErrInvalidEpoch},
		{"elapsed at limit", 0, []igid.Option{igid.WithEpoch(time.UnixMilli(testEpochMs + 1000 - maxElapsedMs))}, nil},
		{"elapsed past limit", 0, []igid.Option{igid.WithEpoch(time.UnixMilli(testEpochMs + 1000 - maxElapsedMs - 1))}, igid.ErrTimeOverflow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := append([]igid.Option{igid.WithEpoch(testEpoch), igid.WithClock(clk.now)}, tt.opts...)
			n, err := igid.New(tt.shard, opts...)
			if !errors.Is(err, tt.want) {
				t.Fatalf("New error = %v, want %v", err, tt.want)
			}
			if tt.want == nil && n == nil {
				t.Fatal("New returned nil Node with nil error")
			}
			if tt.want != nil && n != nil {
				t.Fatal("New returned a Node along with an error")
			}
		})
	}
}

func TestNewDefaults(t *testing.T) {
	n, err := igid.New(7)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.Shard(); got != 7 {
		t.Errorf("Shard() = %d, want 7", got)
	}
	if got := n.Epoch(); !got.Equal(igid.DefaultEpoch) {
		t.Errorf("Epoch() = %v, want %v", got, igid.DefaultEpoch)
	}
	if loc := n.Epoch().Location(); loc != time.UTC {
		t.Errorf("Epoch() location = %v, want UTC", loc)
	}

	id := mustID(t, n)
	p := igid.Decode(id)
	if d := time.Since(p.Time); d < 0 || d > time.Minute {
		t.Errorf("decoded time %v is not close to now", p.Time)
	}
	if p.Shard != 7 {
		t.Errorf("decoded shard = %d, want 7", p.Shard)
	}
}

func TestIDBitLayout(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 5, clk)

	// Move to a fresh millisecond so the sequence starts at 0.
	clk.set(testEpochMs + 1000)
	id := mustID(t, n)

	// 1000<<23 | 5<<10 | 0, written out so the shift widths are checked too.
	const want igid.ID = 8388613120
	if id != want {
		t.Fatalf("ID = %d, want %d", id, want)
	}

	p := n.Decode(id)
	if !p.Time.Equal(time.UnixMilli(testEpochMs+1000)) || p.Shard != 5 || p.Seq != 0 {
		t.Errorf("Decode = %+v, want time=%v shard=5 seq=0", p, time.UnixMilli(testEpochMs+1000).UTC())
	}
}

func TestMaxFieldsFillInt64(t *testing.T) {
	id := igid.ID(int64(maxElapsedMs)<<(igid.ShardBits+igid.SeqBits) | igid.MaxShard<<igid.SeqBits | igid.MaxSeq)
	if id != math.MaxInt64 {
		t.Fatalf("all fields at max = %d, want MaxInt64", id)
	}
	p := igid.DecodeWithEpoch(id, testEpoch)
	if got := p.Time.UnixMilli() - testEpochMs; got != maxElapsedMs {
		t.Errorf("elapsed = %d, want %d", got, int64(maxElapsedMs))
	}
	if p.Shard != igid.MaxShard || p.Seq != igid.MaxSeq {
		t.Errorf("shard/seq = %d/%d, want %d/%d", p.Shard, p.Seq, igid.MaxShard, igid.MaxSeq)
	}
}

func TestSequenceWithinMillisecond(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 3, clk)
	clk.set(testEpochMs + 50)

	var prev igid.ID
	for i := range 10 {
		id := mustID(t, n)
		p := n.Decode(id)
		if p.Seq != int64(i) {
			t.Fatalf("ID %d: seq = %d, want %d", i, p.Seq, i)
		}
		if p.Time.UnixMilli() != testEpochMs+50 {
			t.Fatalf("ID %d: time = %d, want %d", i, p.Time.UnixMilli(), testEpochMs+50)
		}
		if i > 0 && id <= prev {
			t.Fatalf("ID %d = %d, not greater than previous %d", i, id, prev)
		}
		prev = id
	}

	// A new millisecond resets the sequence.
	clk.set(testEpochMs + 51)
	if p := n.Decode(mustID(t, n)); p.Seq != 0 || p.Time.UnixMilli() != testEpochMs+51 {
		t.Errorf("after new ms: %+v, want seq 0 at %d", p, testEpochMs+51)
	}
}

func TestSequenceExhaustionWaitsForNextMillisecond(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 0, clk)
	clk.set(testEpochMs + 100)

	var prev igid.ID
	for i := range igid.MaxSeq + 1 {
		id := mustID(t, n)
		if p := n.Decode(id); p.Seq != int64(i) || p.Time.UnixMilli() != testEpochMs+100 {
			t.Fatalf("ID %d: %+v, want seq %d at %d", i, p, i, testEpochMs+100)
		}
		prev = id
	}

	// The clock is frozen, so let it tick on each read or ID would spin forever.
	clk.setStep(1)
	id := mustID(t, n)
	p := n.Decode(id)
	if p.Seq != 0 {
		t.Errorf("seq after exhaustion = %d, want 0", p.Seq)
	}
	if p.Time.UnixMilli() <= testEpochMs+100 {
		t.Errorf("time after exhaustion = %d, want > %d", p.Time.UnixMilli(), testEpochMs+100)
	}
	if id <= prev {
		t.Errorf("ID after exhaustion %d not greater than %d", id, prev)
	}
}

func TestClockBackwardsWithinDriftIsWaitedOut(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 0, clk) // default drift is 5ms
	clk.set(testEpochMs + 110)
	first := mustID(t, n)

	clk.set(testEpochMs + 107) // 3ms back
	clk.setStep(1)
	id := mustID(t, n)

	p := n.Decode(id)
	if p.Time.UnixMilli() != testEpochMs+110 || p.Seq != 1 {
		t.Errorf("Decode = %+v, want seq 1 at %d", p, testEpochMs+110)
	}
	if id <= first {
		t.Errorf("ID %d not greater than %d", id, first)
	}
}

func TestClockBackwardsBeyondDrift(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 0, clk)
	clk.set(testEpochMs + 110)
	first := mustID(t, n)

	clk.set(testEpochMs + 110 - igid.DefaultMaxDrift.Milliseconds() - 1)
	if _, err := n.ID(); !errors.Is(err, igid.ErrClockBackwards) {
		t.Fatalf("ID error = %v, want ErrClockBackwards", err)
	}

	// A failed call must not disturb state: once the clock recovers,
	// IDs continue from where they left off.
	clk.set(testEpochMs + 110)
	id := mustID(t, n)
	if p := n.Decode(id); p.Seq != 1 {
		t.Errorf("seq after recovery = %d, want 1", p.Seq)
	}
	if id <= first {
		t.Errorf("ID %d not greater than %d", id, first)
	}
}

func TestWithMaxDrift(t *testing.T) {
	t.Run("zero rejects any backward step", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 10)
		n := newTestNode(t, 0, clk, igid.WithMaxDrift(0))
		clk.set(testEpochMs + 9)
		if _, err := n.ID(); !errors.Is(err, igid.ErrClockBackwards) {
			t.Fatalf("ID error = %v, want ErrClockBackwards", err)
		}
	})

	t.Run("larger drift is waited out", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 100)
		n := newTestNode(t, 0, clk, igid.WithMaxDrift(50*time.Millisecond))
		clk.then(testEpochMs+60, testEpochMs+100) // 40ms back, then caught up
		if _, err := n.ID(); err != nil {
			t.Fatalf("ID error = %v, want nil", err)
		}
	})

	t.Run("sub-millisecond drift rounds up", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 100)
		n := newTestNode(t, 0, clk, igid.WithMaxDrift(900*time.Microsecond))

		clk.then(testEpochMs+99, testEpochMs+100) // 1ms back: allowed
		if _, err := n.ID(); err != nil {
			t.Fatalf("1ms back: ID error = %v, want nil", err)
		}

		clk.then(testEpochMs + 98) // 2ms back: not allowed
		if _, err := n.ID(); !errors.Is(err, igid.ErrClockBackwards) {
			t.Fatalf("2ms back: ID error = %v, want ErrClockBackwards", err)
		}
	})

	t.Run("negative is rejected", func(t *testing.T) {
		n, err := igid.New(0, igid.WithMaxDrift(-time.Millisecond))
		if !errors.Is(err, igid.ErrInvalidOption) {
			t.Fatalf("New error = %v, want ErrInvalidOption", err)
		}
		if n != nil {
			t.Fatal("New returned a Node along with an error")
		}
	})
}

func TestWaitSleepsInsteadOfSpinning(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 0, clk)
	clk.set(testEpochMs + 100)
	for range igid.MaxSeq + 1 {
		mustID(t, n)
	}

	// The clock stays on the used-up millisecond for 20ms of real time.
	go func() {
		time.Sleep(20 * time.Millisecond)
		clk.set(testEpochMs + 101)
	}()

	before := clk.readCount()
	id := mustID(t, n)
	if p := n.Decode(id); p.Time.UnixMilli() != testEpochMs+101 || p.Seq != 0 {
		t.Errorf("Decode = %+v, want seq 0 at %d", p, testEpochMs+101)
	}

	// Polling every 100µs gives roughly 200 reads over 20ms; a busy loop
	// would read the clock millions of times.
	if reads := clk.readCount() - before; reads > 2000 {
		t.Errorf("clock read %d times while waiting, want sleeping, not spinning", reads)
	}
}

func TestClockBackwardsDuringSequenceWait(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 0, clk)
	clk.set(testEpochMs + 110)
	for range igid.MaxSeq + 1 {
		mustID(t, n)
	}

	// The sequence is used up, and while ID waits for the next millisecond
	// the clock steps back 10ms and stays there. ID must give up rather
	// than block until the clock catches up.
	clk.then(testEpochMs+110, testEpochMs+100)
	done := make(chan error, 1)
	go func() {
		_, err := n.ID()
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, igid.ErrClockBackwards) {
			t.Fatalf("ID error = %v, want ErrClockBackwards", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ID blocked instead of returning ErrClockBackwards")
	}

	// A failed wait must not consume a sequence slot or move lastMs.
	clk.set(testEpochMs + 111)
	if p := n.Decode(mustID(t, n)); p.Seq != 0 || p.Time.UnixMilli() != testEpochMs+111 {
		t.Errorf("after recovery: %+v, want seq 0 at %d", p, testEpochMs+111)
	}
}

func TestZeroValueNode(t *testing.T) {
	var zero igid.Node
	if _, err := zero.ID(); !errors.Is(err, igid.ErrUninitialized) {
		t.Errorf("zero Node: ID error = %v, want ErrUninitialized", err)
	}

	var nilNode *igid.Node
	if _, err := nilNode.ID(); !errors.Is(err, igid.ErrUninitialized) {
		t.Errorf("nil *Node: ID error = %v, want ErrUninitialized", err)
	}
}

func TestTimeOverflow(t *testing.T) {
	now := testEpochMs + 1000
	clk := newFakeClock(now)
	n := newTestNode(t, igid.MaxShard, clk, igid.WithEpoch(time.UnixMilli(now-maxElapsedMs)))

	id := mustID(t, n)
	if id <= 0 {
		t.Fatalf("last valid ID = %d, want positive", id)
	}

	clk.set(now + 1)
	if _, err := n.ID(); !errors.Is(err, igid.ErrTimeOverflow) {
		t.Fatalf("ID error = %v, want ErrTimeOverflow", err)
	}
}

func TestIDsSortByTimeAcrossShards(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	high := newTestNode(t, igid.MaxShard, clk)
	low := newTestNode(t, 0, clk)

	clk.set(testEpochMs + 10)
	earlier := mustID(t, high)
	clk.set(testEpochMs + 11)
	later := mustID(t, low)

	if later <= earlier {
		t.Errorf("later ID %d (shard 0) not greater than earlier ID %d (shard %d)", later, earlier, igid.MaxShard)
	}
}

func TestIDStrictlyIncreasing(t *testing.T) {
	n, err := igid.New(1)
	if err != nil {
		t.Fatal(err)
	}
	count := 100_000
	if testing.Short() {
		count = 5_000
	}
	prev := mustID(t, n)
	for i := range count {
		id := mustID(t, n)
		if id <= prev {
			t.Fatalf("ID %d: %d not greater than previous %d", i, id, prev)
		}
		prev = id
	}
}

func TestIDConcurrentUnique(t *testing.T) {
	n, err := igid.New(1)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 8
	perWorker := 10_000
	if testing.Short() {
		perWorker = 1_000
	}

	results := make([][]igid.ID, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			ids := make([]igid.ID, 0, perWorker)
			for range perWorker {
				id, err := n.ID()
				if err != nil {
					t.Errorf("ID: %v", err)
					return
				}
				ids = append(ids, id)
			}
			results[w] = ids
		})
	}
	wg.Wait()

	seen := make(map[igid.ID]struct{}, workers*perWorker)
	for w, ids := range results {
		for i, id := range ids {
			if i > 0 && id <= ids[i-1] {
				t.Fatalf("worker %d: ID %d = %d not greater than %d", w, i, id, ids[i-1])
			}
			if _, dup := seen[id]; dup {
				t.Fatalf("duplicate ID %d", id)
			}
			seen[id] = struct{}{}
		}
	}
}

func TestDecodeEpochs(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 9, clk)
	clk.set(testEpochMs + 2500)
	id := mustID(t, n)

	want := time.UnixMilli(testEpochMs + 2500).UTC()
	if got := n.Decode(id).Time; !got.Equal(want) {
		t.Errorf("Node.Decode time = %v, want %v", got, want)
	}
	if got := igid.DecodeWithEpoch(id, testEpoch).Time; !got.Equal(want) {
		t.Errorf("DecodeWithEpoch time = %v, want %v", got, want)
	}
	if got := n.Decode(id).Time.Location(); got != time.UTC {
		t.Errorf("decoded location = %v, want UTC", got)
	}

	// Decode assumes DefaultEpoch, so a custom-epoch ID decodes to a
	// time shifted by the difference between the epochs.
	shift := testEpoch.Sub(igid.DefaultEpoch)
	if got := igid.Decode(id).Time; !got.Equal(want.Add(-shift)) {
		t.Errorf("Decode time = %v, want %v", got, want.Add(-shift))
	}
}

func TestDecodeNegativeID(t *testing.T) {
	// Negative IDs are never issued; Decode must not panic on one, and
	// its fields stay in their ranges even though they mean nothing.
	for _, v := range []int64{-1, math.MinInt64} {
		p := igid.Decode(igid.ID(v))
		if p.Shard < 0 || p.Shard > igid.MaxShard || p.Seq < 0 || p.Seq > igid.MaxSeq {
			t.Errorf("Decode(%d) = %+v, field out of range", v, p)
		}
		if !p.Time.Before(igid.DefaultEpoch) {
			t.Errorf("Decode(%d) time = %v, want before epoch", v, p.Time)
		}
	}
}

func TestIDNeverZero(t *testing.T) {
	// The tightest case: shard 0 and an epoch equal to the current time,
	// so the first ID has zero elapsed time and zero shard bits.
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 0, clk)
	for i := range igid.MaxSeq {
		if id := mustID(t, n); id == 0 {
			t.Fatalf("ID %d = 0", i)
		}
	}
}

// makeID packs fields the same way ID does in the default layout.
func makeID(elapsedMs, shard, seq int64) igid.ID {
	return igid.ID(elapsedMs<<(igid.ShardBits+igid.SeqBits) | shard<<igid.SeqBits | seq)
}

// layoutBits is the number of bits shard ID and sequence share.
const layoutBits = igid.ShardBits + igid.SeqBits

// seqBitsFor returns the sequence width for a power-of-two shard count.
func seqBitsFor(shards int64) int64 {
	return layoutBits - int64(bits.TrailingZeros64(uint64(shards)))
}

func TestWithShardsValidation(t *testing.T) {
	for _, n := range []int64{1, 2, 1024, igid.DefaultShards, 65536, 1 << layoutBits} {
		node, err := igid.New(0, igid.WithShards(n))
		if err != nil {
			t.Errorf("WithShards(%d): New error = %v", n, err)
			continue
		}
		if got := node.Shards(); got != n {
			t.Errorf("WithShards(%d): Shards() = %d", n, got)
		}
	}
	for _, n := range []int64{0, -1, -1024, 3, 1000, 8191, 1<<layoutBits + 1, 1 << (layoutBits + 1)} {
		if _, err := igid.New(0, igid.WithShards(n)); !errors.Is(err, igid.ErrInvalidOption) {
			t.Errorf("WithShards(%d): New error = %v, want ErrInvalidOption", n, err)
		}
	}
}

func TestDefaultShards(t *testing.T) {
	n, err := igid.New(0)
	if err != nil {
		t.Fatal(err)
	}
	if igid.DefaultShards != 8192 {
		t.Fatalf("DefaultShards = %d, want 8192", igid.DefaultShards)
	}
	if got := n.Shards(); got != 8192 {
		t.Errorf("Shards() = %d, want 8192", got)
	}
}

func TestWithShardsLimitsShardID(t *testing.T) {
	if _, err := igid.New(1023, igid.WithShards(1024)); err != nil {
		t.Errorf("shard 1023 of 1024: %v", err)
	}
	if _, err := igid.New(1024, igid.WithShards(1024)); !errors.Is(err, igid.ErrInvalidShard) {
		t.Errorf("shard 1024 of 1024: error = %v, want ErrInvalidShard", err)
	}
	// More shards than the default allows shard IDs past MaxShard.
	if _, err := igid.New(igid.MaxShard+1, igid.WithShards(2*igid.DefaultShards)); err != nil {
		t.Errorf("shard %d of %d: %v", igid.MaxShard+1, 2*igid.DefaultShards, err)
	}
	if _, err := igid.New(1, igid.WithShards(1)); !errors.Is(err, igid.ErrInvalidShard) {
		t.Errorf("shard 1 of 1: error = %v, want ErrInvalidShard", err)
	}
}

func TestWithShardsBitLayout(t *testing.T) {
	clk := newFakeClock(testEpochMs)
	n := newTestNode(t, 5, clk, igid.WithShards(1024))
	clk.set(testEpochMs + 1000)
	id := mustID(t, n)

	// 1024 shards = 10 shard bits, 13 sequence bits: 1000<<23 | 5<<13 | 0.
	const want igid.ID = 8388648960
	if id != want {
		t.Fatalf("ID = %d, want %d", id, want)
	}
	if p := n.Decode(id); p.Shard != 5 || p.Seq != 0 || p.Time.UnixMilli() != testEpochMs+1000 {
		t.Errorf("Decode = %+v, want shard 5 seq 0 at %d", p, testEpochMs+1000)
	}

	// The package-level Decode assumes the default layout, so it misreads
	// the shard of a custom-layout ID. The time still decodes correctly.
	if p := igid.DecodeWithEpoch(id, testEpoch); p.Shard == 5 || p.Time.UnixMilli() != testEpochMs+1000 {
		t.Errorf("DecodeWithEpoch = %+v, want wrong shard but right time", p)
	}
}

func TestWithShardsSequenceCapacity(t *testing.T) {
	// Small shard counts are left out: one shard holds 2^23 IDs per ms,
	// too many to step through here.
	for _, shards := range []int64{1024, igid.DefaultShards, 65536, 1 << (layoutBits - 1)} {
		t.Run(fmt.Sprint(shards), func(t *testing.T) {
			perMs := int64(1) << seqBitsFor(shards)
			shard := shards - 1 // highest shard, so shard bits are all set
			clk := newFakeClock(testEpochMs)
			n := newTestNode(t, shard, clk, igid.WithShards(shards))
			clk.set(testEpochMs + 100)

			var prev igid.ID
			for i := range perMs {
				id := mustID(t, n)
				p := n.Decode(id)
				if p.Seq != i || p.Shard != shard || p.Time.UnixMilli() != testEpochMs+100 {
					t.Fatalf("ID %d: %+v, want shard %d seq %d at %d", i, p, shard, i, testEpochMs+100)
				}
				if i > 0 && id <= prev {
					t.Fatalf("ID %d = %d not greater than %d", i, id, prev)
				}
				prev = id
			}

			clk.then(testEpochMs+100, testEpochMs+101)
			id := mustID(t, n)
			if p := n.Decode(id); p.Seq != 0 || p.Time.UnixMilli() != testEpochMs+101 {
				t.Errorf("after %d IDs: %+v, want seq 0 at %d", perMs, p, testEpochMs+101)
			}
			if id <= prev {
				t.Errorf("ID %d not greater than %d", id, prev)
			}
		})
	}
}

func TestWithShardsMaxShardsOneIDPerMs(t *testing.T) {
	// 2^23 shards leaves no sequence bits: one ID per millisecond.
	clk := newFakeClock(testEpochMs + 100)
	n := newTestNode(t, 1<<layoutBits-1, clk, igid.WithShards(1<<layoutBits))

	// The slot for the millisecond New ran in counts as used.
	clk.then(testEpochMs+100, testEpochMs+101)
	first := mustID(t, n)
	clk.then(testEpochMs+101, testEpochMs+102)
	second := mustID(t, n)

	for i, tc := range []struct {
		id igid.ID
		ms int64
	}{{first, testEpochMs + 101}, {second, testEpochMs + 102}} {
		p := n.Decode(tc.id)
		if p.Seq != 0 || p.Shard != 1<<layoutBits-1 || p.Time.UnixMilli() != tc.ms {
			t.Errorf("ID %d: %+v, want seq 0, max shard, at %d", i, p, tc.ms)
		}
	}
	if second <= first {
		t.Errorf("second ID %d not greater than first %d", second, first)
	}
}

func TestWithShardsNeverZero(t *testing.T) {
	for _, shards := range []int64{1, 1 << layoutBits} {
		clk := newFakeClock(testEpochMs)
		n := newTestNode(t, 0, clk, igid.WithShards(shards))
		clk.then(testEpochMs, testEpochMs+1) // lets the 2^23 case move on
		if id := mustID(t, n); id == 0 {
			t.Errorf("WithShards(%d): first ID = 0", shards)
		}
	}
}

func TestWithShardsLastID(t *testing.T) {
	// 1024 shards: 13 sequence bits, so seq 5000 is only valid here.
	clk := newFakeClock(testEpochMs + 100)
	last := igid.ID(100<<layoutBits | 3<<13 | 5000)
	n := newTestNode(t, 3, clk, igid.WithShards(1024), igid.WithLastID(last))
	id := mustID(t, n)
	if p := n.Decode(id); p.Seq != 5001 || p.Shard != 3 {
		t.Errorf("Decode = %+v, want shard 3 seq 5001", p)
	}
	if id <= last {
		t.Errorf("ID %d not greater than last %d", id, last)
	}
}

func TestWithShardsConcurrentUnique(t *testing.T) {
	// Two shards of a 1024-shard layout, running at once, never collide.
	const perNode = 20_000
	var wg sync.WaitGroup
	results := make([][]igid.ID, 2)
	for i := range results {
		n, err := igid.New(int64(i), igid.WithShards(1024))
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			for range perNode {
				id, err := n.ID()
				if err != nil {
					t.Errorf("ID: %v", err)
					return
				}
				results[i] = append(results[i], id)
			}
		})
	}
	wg.Wait()

	seen := make(map[igid.ID]struct{}, 2*perNode)
	for _, ids := range results {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				t.Fatalf("duplicate ID %d", id)
			}
			seen[id] = struct{}{}
		}
	}
}

func TestWithLastID(t *testing.T) {
	t.Run("behind the clock has no effect", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 100)
		n := newTestNode(t, 2, clk, igid.WithLastID(makeID(50, 2, 9)))
		clk.set(testEpochMs + 101)
		if p := n.Decode(mustID(t, n)); p.Time.UnixMilli() != testEpochMs+101 || p.Seq != 0 {
			t.Errorf("Decode = %+v, want seq 0 at %d", p, testEpochMs+101)
		}
	})

	t.Run("same millisecond continues the sequence", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 100)
		last := makeID(100, 2, 9)
		n := newTestNode(t, 2, clk, igid.WithLastID(last))
		id := mustID(t, n)
		if p := n.Decode(id); p.Time.UnixMilli() != testEpochMs+100 || p.Seq != 10 {
			t.Errorf("Decode = %+v, want seq 10 at %d", p, testEpochMs+100)
		}
		if id <= last {
			t.Errorf("ID %d not greater than last %d", id, last)
		}
	})

	t.Run("same millisecond with sequence used up waits", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 100)
		n := newTestNode(t, 2, clk, igid.WithLastID(makeID(100, 2, igid.MaxSeq)))
		clk.then(testEpochMs+100, testEpochMs+101)
		if p := n.Decode(mustID(t, n)); p.Time.UnixMilli() != testEpochMs+101 || p.Seq != 0 {
			t.Errorf("Decode = %+v, want seq 0 at %d", p, testEpochMs+101)
		}
	})

	t.Run("ahead within drift waits for the clock", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 100)
		last := makeID(103, 2, 7)
		n := newTestNode(t, 2, clk, igid.WithLastID(last))
		clk.then(testEpochMs+100, testEpochMs+103)
		id := mustID(t, n)
		if p := n.Decode(id); p.Time.UnixMilli() != testEpochMs+103 || p.Seq != 8 {
			t.Errorf("Decode = %+v, want seq 8 at %d", p, testEpochMs+103)
		}
		if id <= last {
			t.Errorf("ID %d not greater than last %d", id, last)
		}
	})

	t.Run("ahead beyond drift fails", func(t *testing.T) {
		clk := newFakeClock(testEpochMs + 100)
		last := makeID(100+igid.DefaultMaxDrift.Milliseconds()+1, 2, 0)
		_, err := igid.New(2, igid.WithEpoch(testEpoch), igid.WithClock(clk.now), igid.WithLastID(last))
		if !errors.Is(err, igid.ErrClockBackwards) {
			t.Fatalf("New error = %v, want ErrClockBackwards", err)
		}
	})

	t.Run("uses the final epoch and drift whatever the option order", func(t *testing.T) {
		// 20ms ahead: only allowed because of the WithMaxDrift given after it,
		// and only 20ms because WithEpoch, also given after it, is applied.
		clk := newFakeClock(testEpochMs + 100)
		n, err := igid.New(2,
			igid.WithLastID(makeID(120, 2, 0)),
			igid.WithClock(clk.now),
			igid.WithMaxDrift(25*time.Millisecond),
			igid.WithEpoch(testEpoch),
		)
		if err != nil {
			t.Fatalf("New error = %v", err)
		}
		clk.then(testEpochMs+100, testEpochMs+120)
		if p := n.Decode(mustID(t, n)); p.Time.UnixMilli() != testEpochMs+120 || p.Seq != 1 {
			t.Errorf("Decode = %+v, want seq 1 at %d", p, testEpochMs+120)
		}
	})

	t.Run("negative is rejected", func(t *testing.T) {
		if _, err := igid.New(2, igid.WithLastID(-1)); !errors.Is(err, igid.ErrInvalidOption) {
			t.Fatalf("New error = %v, want ErrInvalidOption", err)
		}
	})
}

func TestParse(t *testing.T) {
	tests := []struct {
		in      string
		want    igid.ID
		wantErr bool
	}{
		{"0", 0, false},
		{"8388613120", 8388613120, false},
		{"9223372036854775807", math.MaxInt64, false},
		{"", 0, true},
		{"abc", 0, true},
		{"-1", 0, true},
		{"1.5", 0, true},
		{" 1", 0, true},
		{"0x10", 0, true},
		{"9223372036854775808", 0, true},
		{"+5", 0, true},
		{"-0", 0, true},
		{"+0", 0, true},
		{"00", 0, true},
		{"0005", 0, true},
		{"1_000", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := igid.Parse(tt.in)
			if tt.wantErr {
				if !errors.Is(err, igid.ErrInvalidID) {
					t.Fatalf("Parse(%q) error = %v, want ErrInvalidID", tt.in, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("Parse(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestStringAndInt64(t *testing.T) {
	id := igid.ID(8388613120)
	if got := id.String(); got != "8388613120" {
		t.Errorf("String() = %q", got)
	}
	if got := id.Int64(); got != 8388613120 {
		t.Errorf("Int64() = %d", got)
	}
	if got := fmt.Sprint(id); got != "8388613120" {
		t.Errorf("fmt.Sprint = %q", got)
	}
}

func TestJSONMarshal(t *testing.T) {
	type record struct {
		ID   igid.ID  `json:"id"`
		Ref  *igid.ID `json:"ref,omitempty"`
		Name string   `json:"name"`
	}
	ref := igid.ID(42)
	b, err := json.Marshal(record{ID: math.MaxInt64, Ref: &ref, Name: "x"})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"id":"9223372036854775807","ref":"42","name":"x"}`
	if string(b) != want {
		t.Errorf("Marshal = %s, want %s", b, want)
	}

	var got record
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != math.MaxInt64 || got.Ref == nil || *got.Ref != 42 {
		t.Errorf("round trip = %+v", got)
	}
}

func TestJSONUnmarshal(t *testing.T) {
	tests := []struct {
		in      string
		want    igid.ID
		wantErr bool
	}{
		{`"123"`, 123, false},
		{`123`, 123, false},
		{`"9223372036854775807"`, math.MaxInt64, false},
		{`9223372036854775807`, math.MaxInt64, false},
		{`""`, 0, true},
		{`"abc"`, 0, true},
		{`"-1"`, 0, true},
		{`-1`, 0, true},
		{`1.5`, 0, true},
		{`1e3`, 0, true},
		{`true`, 0, true},
		{`"9223372036854775808"`, 0, true},
		{`"+5"`, 0, true},
		{`"007"`, 0, true},
		{`"1"`, 1, false},
		{`"12"`, 12, false},
		{`"12"`, 12, false},
		{`"-1"`, 0, true}, // "-1"
		{`"00"`, 0, true}, // "00"
		{`" "`, 0, true},  // " "
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			var id igid.ID
			err := json.Unmarshal([]byte(tt.in), &id)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Unmarshal(%s) = %d, want error", tt.in, id)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unmarshal(%s) error = %v", tt.in, err)
			}
			if id != tt.want {
				t.Errorf("Unmarshal(%s) = %d, want %d", tt.in, id, tt.want)
			}
		})
	}
}

func TestJSONNullLeavesIDUnchanged(t *testing.T) {
	id := igid.ID(77)
	if err := json.Unmarshal([]byte(`null`), &id); err != nil {
		t.Fatal(err)
	}
	if id != 77 {
		t.Errorf("after null, ID = %d, want 77", id)
	}
}

func TestJSONMapKeys(t *testing.T) {
	in := map[igid.ID]string{1: "a", 8388613120: "b"}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"1":"a","8388613120":"b"}`
	if string(b) != want {
		t.Errorf("Marshal = %s, want %s", b, want)
	}

	var out map[igid.ID]string
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[1] != "a" || out[8388613120] != "b" {
		t.Errorf("round trip = %v", out)
	}

	if err := json.Unmarshal([]byte(`{"007":"x"}`), &out); err == nil {
		t.Error(`Unmarshal({"007":"x"}) succeeded, want error for non-canonical key`)
	}
}

func TestUnmarshalJSONInvalidEscape(t *testing.T) {
	// encoding/json rejects bad escapes before calling UnmarshalJSON, so
	// call it directly to cover that branch.
	var id igid.ID
	if err := id.UnmarshalJSON([]byte(`"\x31"`)); !errors.Is(err, igid.ErrInvalidID) {
		t.Errorf("UnmarshalJSON(bad escape) error = %v, want ErrInvalidID", err)
	}
}

func TestText(t *testing.T) {
	id := igid.ID(8388613120)
	b, err := id.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "8388613120" {
		t.Errorf("MarshalText = %q", b)
	}

	var got igid.ID
	if err := got.UnmarshalText(b); err != nil {
		t.Fatal(err)
	}
	if got != id {
		t.Errorf("UnmarshalText = %d, want %d", got, id)
	}

	got = 5
	if err := got.UnmarshalText([]byte("nope")); !errors.Is(err, igid.ErrInvalidID) {
		t.Errorf("UnmarshalText error = %v, want ErrInvalidID", err)
	}
	if got != 5 {
		t.Errorf("failed UnmarshalText changed ID to %d", got)
	}
}

func FuzzParseRoundTrip(f *testing.F) {
	for _, s := range []string{"0", "1", "8388613120", "9223372036854775807", "-1", "abc", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		id, err := igid.Parse(s)
		if err != nil {
			return
		}
		if id < 0 {
			t.Fatalf("Parse(%q) = %d, negative", s, id)
		}
		// Only the canonical form parses, so it must match String exactly.
		if got := id.String(); got != s {
			t.Fatalf("Parse(%q) = %d, whose String is %q", s, id, got)
		}

		b, err := json.Marshal(id)
		if err != nil {
			t.Fatal(err)
		}
		var fromJSON igid.ID
		if err := json.Unmarshal(b, &fromJSON); err != nil || fromJSON != id {
			t.Fatalf("JSON round trip of %d = %d, %v", id, fromJSON, err)
		}
	})
}

func FuzzDecodeRoundTrip(f *testing.F) {
	for _, v := range []int64{0, 1, 8388613120, math.MaxInt64} {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, v int64) {
		if v < 0 {
			return
		}
		p := igid.DecodeWithEpoch(igid.ID(v), testEpoch)
		if p.Shard < 0 || p.Shard > igid.MaxShard || p.Seq < 0 || p.Seq > igid.MaxSeq {
			t.Fatalf("Decode(%d) = %+v, field out of range", v, p)
		}
		elapsed := p.Time.UnixMilli() - testEpochMs
		re := elapsed<<(igid.ShardBits+igid.SeqBits) | p.Shard<<igid.SeqBits | p.Seq
		if re != v {
			t.Fatalf("re-encoding Decode(%d) = %d", v, re)
		}
	})
}

func FuzzDecodeRoundTripLayouts(f *testing.F) {
	for _, v := range []int64{0, 1, 8388648960, math.MaxInt64} {
		for _, shardBits := range []uint8{0, 10, 13, 23} {
			f.Add(v, shardBits)
		}
	}
	clk := newFakeClock(testEpochMs)
	f.Fuzz(func(t *testing.T, v int64, shardBits uint8) {
		if v < 0 {
			return
		}
		shardBits %= layoutBits + 1
		n, err := igid.New(0, igid.WithEpoch(testEpoch), igid.WithClock(clk.now), igid.WithShards(1<<shardBits))
		if err != nil {
			t.Fatal(err)
		}
		p := n.Decode(igid.ID(v))
		seqBits := layoutBits - int64(shardBits)
		if p.Shard < 0 || p.Shard >= n.Shards() || p.Seq < 0 || p.Seq >= 1<<seqBits {
			t.Fatalf("Decode(%d) with %d shards = %+v, field out of range", v, n.Shards(), p)
		}
		elapsed := p.Time.UnixMilli() - testEpochMs
		if re := elapsed<<layoutBits | p.Shard<<seqBits | p.Seq; re != v {
			t.Fatalf("re-encoding Decode(%d) with %d shards = %d", v, n.Shards(), re)
		}
	})
}

func BenchmarkID(b *testing.B) {
	n, err := igid.New(1)
	if err != nil {
		b.Fatal(err)
	}
	for b.Loop() {
		if _, err := n.ID(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIDParallel(b *testing.B) {
	n, err := igid.New(1)
	if err != nil {
		b.Fatal(err)
	}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := n.ID(); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func ExampleDecode() {
	p := igid.Decode(8388613120)
	fmt.Println(p.Time.Format(time.RFC3339Nano))
	fmt.Println("shard", p.Shard, "seq", p.Seq)
	// Output:
	// 2011-08-24T21:07:02.721Z
	// shard 5 seq 0
}

func ExampleID_MarshalJSON() {
	b, _ := json.Marshal(struct {
		ID igid.ID `json:"id"`
	}{ID: 8388613120})
	fmt.Println(string(b))
	// Output:
	// {"id":"8388613120"}
}

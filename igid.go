// Package igid generates 64-bit, time-sortable IDs using Instagram's
// sharded ID layout:
//
//	| 41 bits: ms since epoch | 13 bits: shard ID | 10 bits: sequence |
//
// By default there are 8,192 shards, and each can issue up to 1,024 IDs
// per millisecond. The shard ID and the sequence share 23 bits, and
// [WithShards] moves the split: for example, 1,024 shards that can each
// issue 8,192 IDs per millisecond.
//
// IDs are stored in a signed int64, so the top timestamp bit
// doubles as the sign bit: IDs stay positive for about 34.8 years after the
// epoch. Use [WithEpoch] to pick a recent epoch and get the most time.
//
// IDs are larger than 2^53, so JavaScript cannot hold them exactly as
// numbers. The [ID] type marshals to a JSON string for this reason.
package igid

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// ShardBits is the number of bits used for the shard ID in the default
	// layout. WithShards changes it per Node.
	ShardBits = 13

	// SeqBits is the number of bits used for the per-millisecond sequence
	// in the default layout. WithShards changes it per Node.
	SeqBits = 10

	// MaxShard is the largest valid shard ID in the default layout (8191).
	MaxShard = 1<<ShardBits - 1

	// MaxSeq is the largest sequence value within one millisecond in the
	// default layout (1023).
	MaxSeq = 1<<SeqBits - 1

	// DefaultShards is the number of shards in the default layout (8192).
	DefaultShards = 1 << ShardBits

	// layoutBits is the number of bits the shard ID and sequence share.
	// WithShards moves the split between them, but the total stays fixed
	// so the timestamp keeps its bits.
	layoutBits = ShardBits + SeqBits

	// timeShift is how far the timestamp is shifted left.
	timeShift = layoutBits

	// maxElapsedMs is the largest elapsed time that keeps the ID positive.
	// 64 - 23 = 41 bits, minus the sign bit = 40.
	maxElapsedMs = 1<<(63-timeShift) - 1

	// pollInterval is how often ID rechecks the clock while waiting for
	// the next millisecond.
	pollInterval = 100 * time.Microsecond
)

var (
	// DefaultEpoch is Instagram's original epoch (2011-08-24 21:07:01.721 UTC).
	// IDs made with it stop fitting in June 2046.
	DefaultEpoch = time.UnixMilli(1314220021721).UTC()

	// DefaultMaxDrift is how far the clock may go backwards before ID
	// returns ErrClockBackwards. Smaller drifts are waited out. The default
	// clock never goes backwards, so this only matters with WithClock.
	DefaultMaxDrift = 5 * time.Millisecond
)

var (
	// ErrInvalidShard is returned by New when the shard ID is negative or
	// not below the number of shards (see WithShards).
	ErrInvalidShard = errors.New("igid: shard ID out of range")

	// ErrInvalidEpoch is returned by New when the epoch is in the future.
	ErrInvalidEpoch = errors.New("igid: epoch is in the future")

	// ErrInvalidOption is returned by New when an option is given a value
	// it cannot use, such as a negative drift.
	ErrInvalidOption = errors.New("igid: invalid option")

	// ErrClockBackwards is returned by ID when the clock moved backwards
	// by more than the allowed drift, and by New when the ID passed to
	// WithLastID is further ahead of the clock than the allowed drift.
	ErrClockBackwards = errors.New("igid: clock moved backwards")

	// ErrTimeOverflow is returned when the time since the epoch no longer
	// fits in the ID. Use a newer epoch.
	ErrTimeOverflow = errors.New("igid: time since epoch no longer fits in an ID")

	// ErrUninitialized is returned by ID when the Node was not made by New,
	// such as a zero-value Node or a nil *Node.
	ErrUninitialized = errors.New("igid: Node not created with New")

	// ErrInvalidID is returned when parsing a string that is not a valid ID.
	ErrInvalidID = errors.New("igid: invalid ID")
)

// Option configures a Node. Pass options to New.
type Option func(*Node)

// WithEpoch sets the custom epoch that timestamps are counted from.
// Every node that shares an ID space must use the same epoch, and you must
// never change it once IDs are stored, or decoded times will be wrong and
// new IDs may collide with old ones.
func WithEpoch(t time.Time) Option {
	return func(n *Node) { n.epochMs = t.UnixMilli() }
}

// WithMaxDrift sets how far the clock may move backwards before ID returns
// ErrClockBackwards. Drifts up to this amount are waited out. Zero means
// any backward step is an error. The drift is counted in whole
// milliseconds, so a value that is not a whole number of milliseconds is
// rounded up. A negative value makes New return ErrInvalidOption.
func WithMaxDrift(d time.Duration) Option {
	return func(n *Node) {
		if d < 0 {
			n.optErr = fmt.Errorf("%w: WithMaxDrift(%v): drift must not be negative", ErrInvalidOption, d)
			return
		}
		ms := d.Milliseconds()
		if d%time.Millisecond != 0 {
			ms++
		}
		n.maxDrift = ms
	}
}

// WithShards sets how many shards share the ID space. n must be a power of
// two from 1 to 8,388,608 (2^23). The default is DefaultShards (8,192).
//
// The shard ID and the sequence share 23 bits, so shards times IDs per
// millisecond per shard is always 8,388,608:
//
//	shards     IDs per ms per shard
//	1,024      8,192
//	8,192      1,024 (default)
//	65,536     128
//
// The timestamp is not affected, so neither is the 34.8-year lifetime.
//
// Every Node sharing an ID space must use the same value, and it must not
// change once IDs are stored. Old IDs would decode to the wrong shard and
// sequence, and Nodes with different values running at the same time, as
// in a rolling deploy, can issue the same ID. To decode IDs made with a
// non-default value, use Node.Decode on a Node made with the same options.
func WithShards(n int64) Option {
	return func(nd *Node) {
		if n < 1 || n > 1<<layoutBits || n&(n-1) != 0 {
			nd.optErr = fmt.Errorf("%w: WithShards(%d): want a power of two from 1 to %d",
				ErrInvalidOption, n, 1<<layoutBits)
			return
		}
		nd.layout = newLayout(int64(bits.TrailingZeros64(uint64(n))))
	}
}

// layout is how the 23 bits below the timestamp are split between the
// shard ID (high part) and the sequence (low part).
type layout struct {
	seqBits  int64
	maxShard int64
	maxSeq   int64
}

func newLayout(shardBits int64) layout {
	seqBits := layoutBits - shardBits
	return layout{seqBits: seqBits, maxShard: 1<<shardBits - 1, maxSeq: 1<<seqBits - 1}
}

var defaultLayout = newLayout(ShardBits)

// WithLastID tells New the largest ID this shard has already issued, such
// as the result of SELECT max(id) for the shard. The Node then only issues
// IDs that come after it, which protects against duplicates when a process
// restarts after the wall clock was stepped back.
//
// If id is ahead of the clock by up to the allowed drift (see
// WithMaxDrift), the first call to ID waits for the clock to catch up.
// If it is further ahead, New returns ErrClockBackwards. A negative id
// makes New return ErrInvalidOption. id is decoded with the Node's epoch,
// whatever order the options are given in.
func WithLastID(id ID) Option {
	return func(n *Node) {
		if id < 0 {
			n.optErr = fmt.Errorf("%w: WithLastID(%d): ID must not be negative", ErrInvalidOption, id)
			return
		}
		n.lastID, n.hasLastID = id, true
	}
}

// WithClock replaces the clock the Node reads. It is meant for tests.
// The clock must keep moving forward eventually, or ID will block forever
// when a millisecond's sequence is used up.
func WithClock(now func() time.Time) Option {
	return func(n *Node) {
		if now != nil {
			n.nowFn = func() int64 { return now().UnixMilli() }
		}
	}
}

// monotonicClock returns a clock in Unix milliseconds that is anchored to
// the wall clock once and then advanced by Go's monotonic clock, so wall
// clock steps (NTP corrections, manual changes) never move it backwards.
//
// On systems where the monotonic clock pauses during suspend, timestamps
// fall behind wall time by the time spent suspended. IDs stay unique and
// increasing; only the decoded times lag.
func monotonicClock() func() int64 {
	start := time.Now()
	startNs := start.UnixNano()
	return func() int64 {
		return (startNs + int64(time.Since(start))) / int64(time.Millisecond)
	}
}

// Node generates IDs for one shard. It is safe for concurrent use.
// Create Nodes with New. A zero-value Node cannot issue IDs; its ID method
// returns ErrUninitialized.
//
// A Node only remembers what it issued while it is running, so uniqueness
// depends on how shard IDs are assigned:
//
//   - Only one running Node may use a shard ID at a time. Two Nodes with
//     the same shard ID will produce duplicate IDs. Watch for rolling
//     deploys and failover, where the old and new process overlap; give
//     them different shard IDs or stop the old one first.
//   - A restarted process starts from the current wall clock. If the wall
//     clock was stepped back while the old process ran, the new one can
//     reissue IDs the old one already issued, for as long as the step.
//     Pass the shard's highest stored ID to WithLastID to rule this out.
type Node struct {
	layout
	mu        sync.Mutex
	shardID   int64
	epochMs   int64
	maxDrift  int64
	nowFn     func() int64
	lastMs    int64
	seq       int64
	lastID    ID    // set by WithLastID
	hasLastID bool  // whether WithLastID was given
	optErr    error // first invalid option seen by New
}

// New returns a Node for the given shard ID, which must be below the
// number of shards (0..MaxShard by default, or as set by WithShards).
// It returns ErrInvalidShard, ErrInvalidEpoch, ErrInvalidOption or
// ErrTimeOverflow if the configuration cannot produce valid IDs, and
// ErrClockBackwards if the WithLastID ID is too far ahead of the clock.
func New(shardID int64, opts ...Option) (*Node, error) {
	n := &Node{
		layout:   defaultLayout,
		epochMs:  DefaultEpoch.UnixMilli(),
		maxDrift: DefaultMaxDrift.Milliseconds(),
	}
	for _, opt := range opts {
		opt(n)
		if n.optErr != nil {
			return nil, n.optErr
		}
	}

	// Checked after the options, since WithShards sets the limit.
	if shardID < 0 || shardID > n.maxShard {
		return nil, fmt.Errorf("%w: got %d, want 0..%d", ErrInvalidShard, shardID, n.maxShard)
	}
	n.shardID = shardID

	if n.nowFn == nil {
		n.nowFn = monotonicClock()
	}

	now := n.nowFn()
	if now < n.epochMs {
		return nil, fmt.Errorf("%w: epoch %s", ErrInvalidEpoch, time.UnixMilli(n.epochMs).UTC())
	}
	if now-n.epochMs > maxElapsedMs {
		return nil, ErrTimeOverflow
	}

	// Starting with seq 0 in the current millisecond means the first ID in
	// it gets seq 1. That costs one slot, but it is why ID never returns 0,
	// even with shard 0 and an epoch of right now. Keep it that way.
	n.lastMs, n.seq = now, 0

	if n.hasLastID {
		lastMs := int64(n.lastID)>>timeShift + n.epochMs
		if lastMs >= now {
			if ahead := lastMs - now; ahead > n.maxDrift {
				return nil, fmt.Errorf("%w: last ID is %dms ahead of the clock", ErrClockBackwards, ahead)
			}
			// ID treats this like a small backward clock step and waits.
			n.lastMs, n.seq = lastMs, int64(n.lastID)&n.maxSeq
		}
	}
	return n, nil
}

// Shard returns the Node's shard ID.
func (n *Node) Shard() int64 { return n.shardID }

// Shards returns the number of shards in the Node's layout: DefaultShards,
// or the value given to WithShards.
func (n *Node) Shards() int64 { return n.maxShard + 1 }

// Epoch returns the Node's epoch.
func (n *Node) Epoch() time.Time { return time.UnixMilli(n.epochMs).UTC() }

// ID returns a new unique ID. IDs from one Node always increase.
//
// If a millisecond's sequence is used up (1,024 IDs in the default layout,
// see WithShards), ID sleeps until the next millisecond. If the clock moves backwards by up to the
// allowed drift, ID sleeps until it catches up; beyond that it returns
// ErrClockBackwards. Other callers of the same Node wait while it sleeps.
func (n *Node) ID() (ID, error) {
	if n == nil || n.nowFn == nil {
		return 0, ErrUninitialized
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	now := n.nowFn()
	var err error

	if now < n.lastMs {
		if now, err = n.waitUntil(now, n.lastMs); err != nil {
			return 0, err
		}
	}

	seq := int64(0)
	if now == n.lastMs {
		if n.seq < n.maxSeq {
			seq = n.seq + 1
		} else if now, err = n.waitUntil(now, n.lastMs+1); err != nil {
			return 0, err // sequence used up this ms and the clock went back
		}
	}

	elapsed := now - n.epochMs
	if elapsed > maxElapsedMs {
		return 0, ErrTimeOverflow
	}

	n.lastMs, n.seq = now, seq
	return ID(elapsed<<timeShift | n.shardID<<n.seqBits | seq), nil
}

// waitUntil sleeps until the clock reads at least target, starting from the
// reading now, and returns the new reading. It gives up with
// ErrClockBackwards if the clock is ever more than maxDrift behind lastMs,
// so a clock that keeps falling back cannot block ID indefinitely.
// The caller must hold n.mu.
func (n *Node) waitUntil(now, target int64) (int64, error) {
	for now < target {
		if drift := n.lastMs - now; drift > n.maxDrift {
			return 0, fmt.Errorf("%w by %dms", ErrClockBackwards, drift)
		}
		// The reading is whole milliseconds, so the true time may be late in
		// the current one. Sleep through the full milliseconds left, then
		// poll in short steps to wake near the boundary, not well past it.
		time.Sleep(time.Duration(target-now-1)*time.Millisecond + pollInterval)
		now = n.nowFn()
	}
	return now, nil
}

// Decode splits id into its parts using the Node's epoch and layout.
// It is the only way to decode IDs made with WithShards. A Node used only
// for decoding can be given any valid shard ID, since it issues nothing.
func (n *Node) Decode(id ID) Parts { return decode(id, n.epochMs, n.layout) }

// Parts holds the fields packed into an ID.
type Parts struct {
	Time  time.Time // when the ID was made, to the millisecond, in UTC
	Shard int64     // shard ID, 0..number of shards - 1
	Seq   int64     // sequence within that millisecond
}

// Decode splits id into its parts, assuming DefaultEpoch and the default
// layout of 8,192 shards. For IDs made with a custom epoch only, use
// DecodeWithEpoch; for IDs made with WithShards, use Node.Decode.
//
// Negative IDs are never issued, and Parse and JSON decoding reject them.
// Decoding one (for example, from an ID(x) conversion of bad data) returns
// parts that look valid but mean nothing; check id >= 0 first if the
// value did not come from this package.
func Decode(id ID) Parts { return decode(id, DefaultEpoch.UnixMilli(), defaultLayout) }

// DecodeWithEpoch splits id into its parts using the given epoch and the
// default layout of 8,192 shards. See Decode for negative IDs.
func DecodeWithEpoch(id ID, epoch time.Time) Parts {
	return decode(id, epoch.UnixMilli(), defaultLayout)
}

func decode(id ID, epochMs int64, l layout) Parts {
	v := int64(id)
	return Parts{
		Time:  time.UnixMilli(v>>timeShift + epochMs).UTC(),
		Shard: v >> l.seqBits & l.maxShard,
		Seq:   v & l.maxSeq,
	}
}

// ID is a 64-bit identifier. In JSON it is written as a string, such as
// "3930684247638020", so JavaScript clients do not lose precision.
// When reading JSON, both strings and plain numbers are accepted.
//
// Node.ID never returns 0, so the zero value can mean "no ID".
type ID int64

// Parse converts a base-10 string into an ID. Only the canonical form that
// String produces is accepted: no sign and no leading zeros, so "7" parses
// but "+7", "007" and "-0" do not. Each ID therefore has exactly one
// string form, which keeps string-keyed caches and lookups consistent.
func Parse(s string) (ID, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	// ParseInt accepted s, so it is non-empty and an optional sign followed
	// by digits. Requiring a leading 1-9 (or exactly "0") rules out signs,
	// which covers negatives, and leading zeros.
	if err != nil || (s != "0" && (s[0] < '1' || s[0] > '9')) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidID, s)
	}
	return ID(v), nil
}

// Int64 returns the ID as an int64, for storing in a database.
func (id ID) Int64() int64 { return int64(id) }

// String returns the ID in base 10.
func (id ID) String() string { return strconv.FormatInt(int64(id), 10) }

// MarshalJSON writes the ID as a JSON string.
func (id ID) MarshalJSON() ([]byte, error) {
	return strconv.AppendQuote(nil, id.String()), nil
}

// UnmarshalJSON reads an ID from a JSON string or number. JSON null
// leaves the ID unchanged.
func (id *ID) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == "null" {
		return nil
	}
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		if strings.IndexByte(s, '\\') >= 0 {
			// Escapes such as 1 are valid JSON but rare; let
			// encoding/json decode them rather than slow every call.
			if err := json.Unmarshal(b, &s); err != nil {
				return fmt.Errorf("%w: %s", ErrInvalidID, b)
			}
		} else {
			s = s[1 : len(s)-1]
		}
	}
	v, err := Parse(s)
	if err != nil {
		return err
	}
	*id = v
	return nil
}

// MarshalText writes the ID in base 10. This makes IDs work as JSON map
// keys and with encodings such as YAML and TOML.
func (id ID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// UnmarshalText reads an ID written in base 10.
func (id *ID) UnmarshalText(b []byte) error {
	v, err := Parse(string(b))
	if err != nil {
		return err
	}
	*id = v
	return nil
}

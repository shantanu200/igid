# igid

64-bit, time-sortable IDs in Go, using Instagram's sharded ID layout:

```
| 41 bits: ms since epoch | 13 bits: shard ID | 10 bits: sequence |
```

- 8,192 shards by default, each issuing up to 1,024 IDs per millisecond.
  `WithShards` changes the split (see [Shards](#shards)).
- IDs from one shard always increase; IDs from different shards sort by time.
- No coordination at runtime: each process only needs its own shard ID.
- No dependencies outside the standard library.

```
go get github.com/shantanu200/igid
```

## Usage

```go
node, err := igid.New(42) // shard ID, 0..8191, unique per running process
if err != nil {
	log.Fatal(err)
}

id, err := node.ID()
if err != nil {
	log.Fatal(err)
}

fmt.Println(id)      // 4026578633995888647
p := node.Decode(id) // {Time: 2026-11-09 12:00:00 UTC, Shard: 42, Seq: 7}
db.Exec("INSERT INTO posts (id) VALUES ($1)", id.Int64())
```

`ID` marshals to a JSON string (`"4026578633995888647"`), because IDs are larger
than 2^53 and JavaScript numbers would lose precision. It reads back from both
strings and numbers, and works as a JSON map key. `Parse` accepts only the
canonical form, so `"007"` and `"+7"` are rejected.

`ID()` never returns 0, so the zero value can mean "no ID".

## Options

| Option | Default | Purpose |
| --- | --- | --- |
| `WithShards(n)` | 8192 | Number of shards, a power of two. See below. |
| `WithEpoch(t)` | 2011-08-24 (Instagram's) | Where timestamps start. IDs fit for about 34.8 years after it, so the default runs out in June 2046. Pick a recent date for new systems, and never change it once IDs are stored. |
| `WithLastID(id)` | none | The highest ID this shard has stored. The Node only issues IDs after it. |
| `WithMaxDrift(d)` | 5ms | How far a custom clock may go back before `ID()` errors instead of waiting. |
| `WithClock(fn)` | monotonic | Replace the clock, for tests. |

## Shards

The shard ID and the sequence share 23 bits, so more shards means fewer IDs
per millisecond on each one. The timestamp, and the 34.8-year lifetime, stay
the same.

| `WithShards(n)` | IDs per ms per shard |
| --- | --- |
| 1,024 | 8,192 |
| 8,192 (default) | 1,024 |
| 65,536 | 128 |

`n` must be a power of two from 1 to 8,388,608; anything else makes `New`
return `ErrInvalidOption`.

Pick the value once, before any IDs are stored, and use the same value on
every node. Changing it later makes old IDs decode to the wrong shard, and
nodes running different values at the same time can issue the same ID.

The package-level `Decode` assumes the default layout. For other layouts,
decode with a `Node` built from the same options:

```go
opts := []igid.Option{igid.WithShards(1024)}
node, err := igid.New(shard, opts...)
// elsewhere, decode only:
decoder, err := igid.New(0, opts...)
p := decoder.Decode(id)
```

## Avoiding duplicates

The generator is unique per shard, so uniqueness depends on how you hand out
shard IDs:

- **Never run two processes with the same shard ID at once.** Watch rolling
  deploys and failover, where old and new instances overlap.
- **Restarts after a clock change.** The clock is monotonic within a process,
  but a restarted process starts again from the wall clock. If the wall clock
  was stepped back during the previous run, pass the shard's highest stored ID
  to `WithLastID`:

  ```go
  // (id >> 10) & 8191 extracts the shard in the default layout; with
  // WithShards(n) it is (id >> (23 - log2(n))) & (n - 1).
  var last int64
  db.QueryRow("SELECT COALESCE(max(id), 0) FROM posts WHERE (id >> 10) & 8191 = $1", shard).Scan(&last)
  node, err := igid.New(shard, igid.WithLastID(igid.ID(last)))
  ```

## Limits

- When a millisecond's sequence is used up (1,024 IDs by default), `ID()`
  sleeps until the next one. Sleeps overshoot slightly, so expect a bit under
  the full rate per shard at full load.
- `ID()` holds a lock, so one `Node` serves one shard; use more shards to scale
  past that.

## Testing

```
go test -race ./...
go test -run '^$' -fuzz FuzzParseRoundTrip -fuzztime 30s .
go test -run '^$' -bench . -benchmem .
```

## License

[MIT](LICENSE)

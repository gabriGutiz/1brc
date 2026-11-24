# 1brc - The One Billion Row Challenge *Solved With Go*

1️⃣🐝🏎️ [The One Billion Row Challenge](https://github.com/gunnarmorling/1brc)

A solution for aggregating one billion temperature measurements from a text
file. The original challenge is primarily focused on Java; this implementation
is written in Go.

## Version
Made with Go 1.22

## Input

The input file contains one measurement per line in this format:

```text
City name;temperature
```

For example:

```text
Hamburg;12.3
```

Temperatures are stored internally as integers representing tenths of a
degree. This avoids floating-point work while reading and aggregating rows.

## How to run

Run the source directly:

```bash
go run main.go -input <input_file>
```

Or build and run the binary:

```bash
go build main.go
./main -input <input_file>
```

The helper script builds the program and measures its execution time:

```bash
chmod +x run.sh
./run.sh <input_file>
```

## How it works

The program uses a pipeline with three stages: reading, processing, and
merging. For example, on a machine with four CPUs, the pipeline looks like
this (the actual number of consumers is determined at runtime):

```text
                         chunksChan
                       (buffered queue)
                              |
                              v
  input file          +----------------+
  ------------------> | chunkProducer  |
  read ~4 MB at a time +----------------+
                              |
             +----------------+----------------+
             |                |                |
             v                v                v
       +-----------+    +-----------+    +-----------+
       | Consumer 1|    | Consumer 2|    | Consumer 3|  ...
       | chunk #1   |    | chunk #2   |    | chunk #3   |
       +-----------+    +-----------+    +-----------+
             |                |                |
             v                v                v
       local city map   local city map   local city map
             \                |                /
              \               |               /
               +--------------+--------------+
                              |
                              v
                    +-------------------+
                    | main merge loop   |
                    | mapsChan          |
                    +-------------------+
                              |
                              v
                    sorted final output
```

### 1. Reading and splitting the file

`chunkProducer` reads the file into a buffer of approximately 4 MB. It finds
the last newline in the buffer and sends only complete lines to
`chunksChan`. If the buffer ends in the middle of a line, that incomplete part
is copied to the beginning of the next buffer. This guarantees that a city
measurement is processed by exactly one consumer.

`chunksChan` is buffered, so the producer can usually prepare the next chunk
while consumers are still processing earlier chunks.

### 2. Processing chunks in parallel

`process` starts `runtime.NumCPU() - 1` consumer goroutines. Each consumer
repeatedly receives a chunk and calls `chunkConsumer`. The consumers do not
share their working maps:

```text
                    One input chunk
                          |
                          v
              +-----------------------+
              | Consumer               |
              |                        |
              | city -> [min, max,    |
              |          sum, count]  |
              +-----------------------+
                          |
                          v
             one completed map on mapsChan
```

For every line, the consumer locates the semicolon and newline directly in
the byte slice. It parses the temperature into an integer number of tenths of
a degree, then updates the city's four values. A city that appears many times
in the same chunk is reduced to one map entry before the result is sent to the
main goroutine.

### 3. Merging the worker results

The main goroutine receives one local map at a time from `mapsChan` and merges
it into the final map:

```text
  Consumer 1 map: {Berlin -> [min, max, sum, count]}
  Consumer 2 map: {Berlin -> [min, max, sum, count]}
  Consumer 3 map: {Paris  -> [min, max, sum, count]}
                         |
                         v
              +------------------------+
              | final map               |
              | Berlin -> combined data |
              | Paris  -> combined data |
              +------------------------+
```

For a city already in the final map, merging takes the lower minimum, higher
maximum, and adds the sums and counts. Because consumers only modify their
own maps, no mutex is needed around the hot per-row update. The main goroutine
is the only owner of the final map.

`mapsChan` is unbuffered. After finishing a chunk, a consumer sends its local
map and waits until the main goroutine receives it. This provides natural
backpressure: workers do not produce an unbounded number of completed maps,
and the final map is merged while other workers continue processing.

After all chunks have been merged, city names are sorted alphabetically and
the result is printed in the required format:

```text
{City=min/average/max, ...}
```

The average is calculated from the integer sum and count, rounded to one
decimal place, and then formatted as a temperature.

## Why it is fast

The implementation combines parallelism with a low-overhead parsing path:

- **Parallel chunk processing:** file reading and several chunk consumers run
  concurrently, allowing multiple CPU cores to parse different parts of the
  file at the same time.
- **Large read chunks:** reading approximately 4 MB at a time reduces the
  number of file reads and the overhead of coordinating small pieces of work.
- **Safe chunk boundaries:** workers receive complete lines, so they do not
  need to reassemble records or synchronize while reading.
- **Byte-oriented parsing:** `bytes.IndexByte` finds separators directly in
  the input bytes. This avoids creating a slice of strings for every line.
- **Custom temperature parser:** the input temperature is parsed from bytes
  instead of using a general-purpose floating-point parser.
- **Fixed-point arithmetic:** temperatures are kept as integer tenths, which
  avoids floating-point calculations for every row and makes min/max updates
  simple integer comparisons.
- **Local aggregation:** each consumer updates its own map without locks. The
  expensive per-row work is therefore not serialized by a shared map or
  mutex.
- **One result per city per chunk:** a consumer sends its partial totals rather
  than sending every input row to the merger. This greatly reduces channel
  traffic and merge work.
- **Single output operation:** results are assembled with `strings.Builder`
  and printed after processing, rather than writing output for each row.

The final sort is required by the challenge's output format. It happens only
once per distinct city, so it is small compared with scanning and aggregating
one billion rows.

## Code Reading Tips

The main data structure is a pointer to a four-element integer array. The
indexes have fixed meanings throughout the program:

```text
city -> [minimum, maximum, sum, count]
         0        1        2    3
```

The same structure is used for a consumer's local map and for the final map.
This makes the merge operation small: compare indexes `0` and `1`, then add
indexes `2` and `3`.

The most important functions are:

| Function | Responsibility |
|----------|----------------|
| `process` | Creates channels, starts goroutines, and merges worker results |
| `chunkProducer` | Reads large buffers and sends newline-complete chunks |
| `chunkConsumer` | Parses rows and builds one local map per chunk |
| `customByteToInt` | Converts a temperature byte slice to integer tenths |
| `round` | Rounds the final average for output formatting |

### Integer measurements

The input value `12.3` is represented internally as `123`, and `-4.5` as
`-45`. This is fixed-point arithmetic:

```text
input       stored value       purpose
12.3        123                min/max/sum/count use integers
-4.5        -45                no floating-point error while aggregating
```

Every row can therefore update minimum, maximum, sum, and count using integer
operations. Floating-point conversion is delayed until the final output, when
the total sum is divided by the count and formatted to one decimal place.

### Custom temperature parser

`customByteToInt` relies on the challenge's fixed input format: a signed or
unsigned number with exactly one digit after the decimal point. It reads the
ASCII digits directly and skips general-purpose parsing. For example:

```text
"12.3"  -> 1 * 100 + 2 * 10 + 3 = 123
"-4.5"  -> -(4 * 10 + 5) = -45
```

This avoids creating a temporary string and avoids the extra validation and
floating-point work performed by a general parser. The optimization is safe
because the input format is guaranteed by the challenge; it is not a general
parser for arbitrary numeric text.

### Chunking strategy

`chunkProducer` reads about 4 MB at a time instead of reading one line at a
time. It finds the final newline and sends everything through that newline to
the workers. Any partial line is copied to the next buffer. The producer then
allocates a fresh buffer because the previous one may still be in use by a
consumer.

This strategy provides three benefits:

- Fewer file reads and fewer channel sends than line-by-line processing.
- Complete records for consumers without locks or cross-worker coordination.
- Enough work per consumer to offset goroutine and map-update overhead.

The buffered `chunksChan` allows the producer to stay ahead of consumers, but
the unbuffered `mapsChan` prevents completed maps from accumulating without
being merged.

## Profiling

CPU and heap profiles can be written to the `profiles` directory:

```bash
go run main.go -input <input_file> -cpuprofile cpu.prof -memprofile mem.prof
```

The directory must exist before running the command.

## Documented Source Version

`main.go` is the normal implementation used by the project. A separate,
commented copy is available in `main_documented.go` for reading the algorithm
without changing the normal build. It has a `documented` build constraint so
it does not create duplicate definitions when the package is built normally.

To run that file directly, provide it as the source file to `go run`:

```bash
go run main_documented.go -input <input_file>
```

The documented file contains the same behavior as `main.go`; changes to the
algorithm should be kept in sync between the two files.

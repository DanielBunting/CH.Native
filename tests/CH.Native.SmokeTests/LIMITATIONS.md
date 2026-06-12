# Client limitations found by cross-client smoke testing

Findings from roundtripping data between three clients against ClickHouse 25.11:

- **CH.Native** (this library, native TCP protocol, port 9000)
- **ClickHouse.Driver 1.2.0** (the official .NET client, HTTP, port 8123) — `CrossClientDriverInsertSmokeTests`
- **clickhouse-client** (the official CLI, exec'd inside the test container) — `CliTypeRoundTripSmokeTests`

Every roundtrip inserts through client A and reads back through client B against
*anchored* expected values (independent literals, never client-vs-client equality), so a
failure identifies which client deviates from ground truth.

Each limitation below is **pinned** by an active test (`#N` in
`Query/NativeLimitationProbeTests.cs`, `#DN` in `Query/ClientBehaviorComparisonTests.cs`,
which pins all three clients side by side): the test asserts the current limited
behavior and fails loudly if it ever changes. When that happens, promote the scenario
into regular matrix coverage, delete the probe, and update this file.

## Confirmed CH.Native limitations

### 1. DateTime64(7–9) truncates to 100 ns ticks

`DateTime64(9)` value `…00.123456789` reads back as `…00.1234567`. The server stores full
nanoseconds (CLI `toString` proves it); CH.Native materializes `System.DateTime`, whose
ticks are 100 ns. The last one (scale 8) or two (scale 9) fractional digits are lost.

- Source of truth: CLI canonical read shows all nine digits.
- Pinned by: `DateTime64_9_SubTickPrecision_TruncatedTo100ns`
- Fix direction: offer a raw-Int64 accessor (or `Instant`-like type) for scales 7–9.

### 2. Invalid UTF-8 in String is irrecoverably replaced with U+FFFD

A `String` holding bytes `0xFF 0x61` reads back as `"�a"` under **both** Eager and Lazy
string materialization. ClickHouse strings are arbitrary byte sequences; the server
stores the bytes faithfully (CLI `hex()` proves it), but CH.Native's reader decodes
UTF-8 with replacement, so the original bytes cannot be obtained. (Embedded NUL bytes,
by contrast, survive fine — `String_EmbeddedNul_Preserved`.)

- Source of truth: CLI `hex(val)` = `FF61`.
- Pinned by: `String_InvalidUtf8_ReplacedWithFffd`
- Fix direction: a byte-level accessor on the lazy-string path.

### 3. Interval types are not supported

`SELECT INTERVAL 3 DAY` throws `NotSupportedException: Column type 'IntervalDay' is not
supported.` The CLI (and HTTP driver) answer it.

- Pinned by: `Interval_NotSupported`

### 4. The Nothing type is not supported — bare `SELECT NULL` fails

`SELECT NULL` (`Nullable(Nothing)`) and `SELECT []` (`Array(Nothing)`) throw
`NotSupportedException: Column type 'Nothing' is not supported.` Both other clients
handle these. Notable because ORMs/diagnostics sometimes emit bare `SELECT NULL` probes.

- Pinned by: `SelectNull_NothingType_NotSupported`
- NB: a reader throw mid-block poisons the connection (partial response bytes remain in
  the pipe), so hitting #3/#4 on a pooled connection costs the connection.

### 5. Bulk insert into BFloat16 writes a malformed block

`BulkInserter` writing a `float` into a `BFloat16` column produces a block the server
rejects with error 261 `Unknown BlockInfo field number: 32` — i.e. the wire output is
corrupt, not merely unsupported. Reading BFloat16 works (returns `float`); CLI inserts
prove the column is fine.

- Pinned by: `BulkInsert_BFloat16_MalformedBlock_ServerRejects`
- Fix direction: either register a proper BFloat16 extractor (2-byte encoding) or fail
  fast client-side with `NotSupportedException` before any bytes hit the wire.

## Confirmed ClickHouse.Driver 1.2.0 limitations

Found by running the same probe scenarios through the official driver
(`ClientBehaviorComparisonTests`):

### D1. Interval types are not supported (either .NET client)

`SELECT INTERVAL 3 DAY` throws `ArgumentException: Unknown type: IntervalDay` (CH.Native
throws `NotSupportedException`). Only the CLI answers.

- Pinned by: `Interval_OnlyCliSupports`

### D2. DST fall-back overlap collapses — the .NET analogue of the clickhouse-jdbc bug

Zoned `DateTime` columns come back as wall-clock `System.DateTime` in the column zone
with no offset, so the two distinct instants sharing the 01:30 Europe/London wall clock
on 2024-10-27 read back with **identical ticks**. The `Kind` flag differs (`Unspecified`
for the BST row, `Utc` for the GMT row) but carries no usable offset — the UTC instant
of the pre-transition row is unrecoverable. CH.Native returns `DateTimeOffset` and keeps
the instants distinct.

- Pinned by: `DstFallBackOverlap_DriverCollapsesWallClock_NativePreservesInstants`
- Same class of bug as clickhouse-jdbc 0.9.0's collapse documented in
  jvm-clickhouse-native's `CrossClientTimezoneDstIT`.
- NB: the driver's *bulk copy* path (RowBinary) is not affected for UTC instants — the
  `Timezone_*` tests in `CrossClientDriverInsertSmokeTests` pass; the loss is on read.

### D3. DateTime64(7–9) truncates to 100 ns ticks (either .NET client)

Shared `System.DateTime` resolution limit — identical to CH.Native limitation #1.

- Pinned by: `DateTime64_9_BothNetClientsTruncateTo100ns`

### D4. Invalid UTF-8 replaced with U+FFFD (either .NET client)

Identical to CH.Native limitation #2.

- Pinned by: `InvalidUtf8_BothNetClientsReplaceWithFffd`

### D5. Parameter binding flushes Float64 min-denormal to zero

The driver's `{v:Float64}` parameters travel as text over HTTP and the server's default
fast float parser flushes `double.Epsilon` (5e-324) to `0` — the same server-side
text-path issue jvm-clickhouse-native documented (upstream workaround:
`SETTINGS precise_float_parsing=1`). CH.Native's binary bulk insert carries the exact
bit pattern. **Read** paths are unaffected in both clients: a binary-stored denormal
reads back bit-exact through both.

- Pinned by: `Float64MinDenormal_DriverTextParamFlushesToZero_NativeBinaryExact`

### D6. Dynamic/Variant values lose their runtime type

The driver materializes `Dynamic`/`Variant` values as strings (`42::Dynamic` → `"42"`).
CH.Native returns typed `ClickHouseDynamic`/`ClickHouseVariant` wrappers.

- Pinned by: `DynamicVariant_DriverReturnsStrings`

## Suspected gaps disproven by probing

These were listed as known gaps (or are bugs in other clients) but probing shows
CH.Native handles them correctly; each is pinned against regression:

| Behavior | Test |
|---|---|
| Int256/UInt256 **bulk insert works** via `BigInteger` (the "no extractor" note in `TypeBulkRoundTripSmokeTests` was stale) | `BulkInsert_Int256_UInt256_BigInteger_Works` |
| Decimal256 keeps all 76 digits via `ClickHouseDecimal` | `Decimal256_76Digits_FullPrecision` |
| Date32 spans the full 1900-01-01 … 2299-12-31 server range | `Date32_FullRange_1900_To_2299` |
| DST fall-back instants stay distinct (`DateTimeOffset` with correct offsets) — the clickhouse-jdbc 0.9.0 collapse bug does **not** affect CH.Native | `DateTime_FallBackOverlap_DistinctInstants`, `Timezone_*` in `CrossClientDriverInsertSmokeTests` |
| `-0.0` keeps its sign bit | `Float64_NegativeZero_SignPreserved` |
| Embedded NUL bytes in String survive | `String_EmbeddedNul_Preserved` |
| JSON columns are readable without `allow_experimental_json_type` on the reading session | `Json_ReadWithoutSessionFlag_Works` |
| Deeply nested composites (`Array(Map(String, Tuple(Int32, Array(Nullable(String)))))`) decode correctly | `DeepComposite_ArrayMapTupleArrayNullable` |
| UInt64 above `long.MaxValue` reads as proper `ulong` (no raw-bits negative surprise) | `UInt64` matrix facts, `UInt64_AboveLongMax` |

## Representation notes (differences, not bugs)

Pinned in `RepresentationDifferences_Enum_FixedString_Date32` where applicable:

| Scenario | CH.Native | ClickHouse.Driver |
|---|---|---|
| Enum8/16 | numeric value (`sbyte`/`short`); use `toString(val)` for names | member name (string) |
| FixedString | `byte[]` incl. trailing NUL padding | `string` incl. NUL padding |
| Date32 / Date | `DateOnly` | `DateTime` (Kind=Utc) |
| Zoned DateTime | `DateTimeOffset` (instant preserved) | wall-clock `DateTime` (see D2) |
| Geo Point | `CH.Native.Data.Geo.Point` | `Tuple<double, double>` |
| `SELECT NULL` / `SELECT []` | throws (#4) | `null` / empty array |

- **NULL inside Dynamic/Variant**: server-side `toString()` renders empty string /
  `ᴺᵁᴸᴸ` respectively — identical across all clients (server quirk, anchored in the CLI
  matrix).

## Harness limitations

- Testcontainers `ExecAsync` has no stdin, so CLI inserts inline `VALUES` in `--query`;
  megabyte-scale payload coverage stays in `BoundaryValueSmokeTests` (native path).

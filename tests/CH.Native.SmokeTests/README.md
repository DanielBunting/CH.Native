# CH.Native.SmokeTests

End-to-end smoke tests that exercise CH.Native against a **real ClickHouse server** and
cross-check its behavior against two independent clients:

- the **official `ClickHouse.Driver`** NuGet package (HTTP protocol, port 8123), and
- the **`clickhouse-client` CLI** (exec'd inside the server container, native protocol).

Where the unit and integration tests verify CH.Native against itself, this suite verifies
it against the rest of the ClickHouse ecosystem: same query, three clients, results must
agree — or the disagreement must be deliberately pinned and documented.

## Requirements

- **Docker** — the suite provisions a `clickhouse/clickhouse-server:25.11` container via
  Testcontainers (`Fixtures/SmokeTestFixture.cs`). There is no skip logic; without Docker
  the tests fail rather than skip.
- Nothing else on the host. The `clickhouse-client` binary is executed *inside* the server
  container, so it does not need to be installed locally.

All test classes share one container through the `[Collection("SmokeTest")]` xUnit
collection fixture, which exposes native (9000) and HTTP (8123) connection strings plus
variants for compression and lazy string materialization.

```bash
dotnet test tests/CH.Native.SmokeTests
```

## Test categories

### Type round-trips (CH.Native vs official driver)

| Class | What it covers |
|---|---|
| `TypeRoundTripSmokeTests` | One fact per scalar type (Bool, Int8…Int256, UInt128/256, floats, Decimals, dates, UUID, IP, …): SELECT through CH.Native and through `ClickHouse.Driver`, results compared via `ResultComparer`. |
| `CompositeTypeSmokeTests` | Array, Map, Tuple, nested combinations, and LowCardinality (including the UInt8→UInt16 dictionary-index overflow case). |
| `BoundaryValueSmokeTests` | Edge values: NaN/±Infinity/−0/subnormals, large strings, embedded null bytes, CJK and emoji strings, DateTime epoch/max. |
| `AggregateFunctionSmokeTests` | `SimpleAggregateFunction(fn, T)` pass-through and server-side `finalizeAggregation(state)` parity. Direct `AggregateFunction` column parity is intentionally out of scope (CH.Native surfaces an opaque `ClickHouseAggregateState` the driver has no equivalent for). |
| `MapMaterialisationSmokeTests` | The `ClickHouseMap<TKey, TValue>` opt-in path: duplicate keys preserved in wire order, as opposed to the legacy `Dictionary` materialization. |

### Write-path round-trips

| Class | What it covers |
|---|---|
| `TypeBulkRoundTripSmokeTests` | One fact per type: bulk-insert via `BulkInserter<T>` over the native protocol, read back via native, assert read-back equals what was written. |
| `BulkInsertSmokeTests` | Bulk insert of composite values (e.g. `Array(Int32)`). |
| `CrossClientDriverInsertSmokeTests` | The reverse direction the other suites don't cover: the official driver **writes** (via `ClickHouseBulkCopy` over HTTP), CH.Native **reads** over native, asserted against *anchored* CLR values so a failure points at the leg that deviated from ground truth. |

### CLI cross-checks (anchored, three-client)

| Class | What it covers |
|---|---|
| `CliTypeRoundTripSmokeTests` | Both directions against `clickhouse-client` with **anchored expected values**: (a) CLI inserts → CH.Native reads (canonical `toString()` rendering *and* raw typed values), (b) CH.Native inserts → CLI reads canonical. Anchoring means a failure identifies *which* client is wrong, not merely that two clients disagree. |
| `ClientBehaviorComparisonTests` | Three-way behavior pinning for scenarios where the clients genuinely differ. Each test records what `clickhouse-client` (ground truth), CH.Native, and `ClickHouse.Driver` do today; if an upstream release changes a pinned behavior, the test fails loudly. User-facing mismatches are documented in the Gotchas section of `docs/data-types.md`. |
| `NativeLimitationProbeTests` | Probes for CH.Native's own limitations, with the CLI and the driver as sources of truth. **PINNED LIMITATION** tests assert the *current limited behavior* — when one starts failing the limitation has changed: promote the scenario into matrix coverage, delete the probe, and update the docs. The rest pin behaviors probing proved correct, guarding against regressions and stale "known gap" folklore. |

### API-surface and transport smoke tests

| Class | What it covers |
|---|---|
| `AdoNetSmokeTests` | The ADO.NET surface (`ExecuteScalar`, `DbDataReader`, `GetSchemaTable`, connection state, multiple queries per connection) plus Dapper `QueryAsync`, compared against the official driver. |
| `LinqSmokeTests` | The LINQ provider end-to-end: Where/OrderBy/ThenBy, Contains, StartsWith, Skip/Take, Count, projections, empty tables. |
| `CompressionSmokeTests` | LZ4 and Zstd over the native protocol on basic types, large datasets, and mixed columns; uncompressed results compared against the driver. |
| `ProtocolSmokeTests` | Protocol mechanics: multi-block results (100K rows), empty result sets, wide tables, system-table queries, large-result parity with the driver. |

## Helpers

- `Helpers/NativeQueryHelper.cs` / `Helpers/DriverQueryHelper.cs` — run a query through
  CH.Native or `ClickHouse.Driver` and return rows in a comparable shape.
- `Helpers/CliQueryHelper.cs` — runs queries through `clickhouse-client` inside the
  container; SELECT output comes back as TSV-unescaped cells (`\N` = SQL NULL). Note:
  Testcontainers exec has no stdin, so CLI inserts must inline `VALUES` in the query.
- `Helpers/ResultComparer.cs` — compares post-read values against a pre-write source of
  truth, normalizing cross-CLR representation differences (`DateTimeOffset`↔`DateTime`,
  `Guid`↔string, `IPAddress`↔string, `decimal`↔`ClickHouseDecimal`, numeric widening).

## Conventions

- **Anchored values over agreement.** Wherever practical, tests assert against hard-coded
  expected values rather than only comparing two clients, so failures localize blame.
- **Pinned limitations are tests, not docs.** A known gap lives as a probe test asserting
  today's behavior; a probe that starts failing is the signal to upgrade coverage and
  update `docs/data-types.md`.
- **Reader failures poison the connection.** A column reader throwing mid-block leaves
  partial response bytes in the pipe, so probes that expect a reader throw each use their
  own dedicated connection.

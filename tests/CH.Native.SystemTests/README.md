# CH.Native.SystemTests

End-to-end / system-level tests that exercise the whole stack under realistic
conditions. Kept separate from `CH.Native.Tests` so heavyweight dependencies
(Toxiproxy, multi-node Docker, OpenTelemetry SDK, allocation tooling) and slow
runtimes don't bleed into the fast integration suite.

## Trait taxonomy

Every test is tagged with a `[Trait("Category", ...)]` so CI can pick subsets:

| Trait | Meaning | Typical runtime |
| ----- | ------- | --------------- |
| `Suite` | Meta-tests for the system-test suite itself, such as category coverage guards. | < 5 s |
| `Allocation` | Allocation-budget regression tests against a checked-in baseline. | < 30 s |
| `DependencyInjection` | DI package wiring, keyed data sources, rotating provider hooks, and health checks against real ClickHouse. | < 1 min |
| `Security` | Identifier, parameter, unicode, and wire escaping contracts with hostile payloads. | < 1 min |
| `Linq` | LINQ translation correctness against a seeded fact table. | < 1 min |
| `Streams` | Stream framing, malformed server responses, scan boundaries, and poisoned-pool recovery. | < 1 min |
| `Observability` | OpenTelemetry SDK pipeline + IProgress/Totals/Extremes coverage. | < 1 min |
| `Cancellation` | Cancellation recovers cleanly: pool, sockets, server-side query state. | < 1 min |
| `Cluster` | Multi-node ClickHouse (replicated + distributed + cluster DDL) scenarios. | 1–5 min |
| `Chaos` | Toxiproxy-driven failure injection: latency, packet loss, RST, mid-flush. | 1–5 min |
| `Resilience` | ResilientConnection × Toxiproxy: retry, circuit breaker, load balancer under failure. | 1–5 min |
| `ServerFailures` | Server-side error semantics: timeout, OOM, throttling, auth revocation, role switching. | 1–3 min |
| `Stress` | High-volume bulk-insert, concurrent / engine-variant / compression / backpressure / mixed-workload / archetypes. | 1–15 min |
| `VersionMatrix` | Smoke + type-fuzz across pinned ClickHouse images. | 5–15 min |
| `Soak` | Long-running mixed-workload stability tests. | 10–60 min (opt-in) |

Bulk-insert system coverage is intentionally spread across the scenario traits
above: cancellation contracts live under `Cancellation`, network and atomicity
failures under `Chaos`, extraction/data-corruption failures under `Stress`, and
stream-pump behavior under `Streams`.

## Running

```bash
# Allocation regressions only — fast, no Docker work beyond a single CH instance.
dotnet test tests/CH.Native.SystemTests --filter "Category=Allocation"

# Observability + Cancellation — fast, single CH instance.
dotnet test tests/CH.Native.SystemTests --filter "Category=Observability|Category=Cancellation"

# DI package wiring + health checks — fast, single CH instance.
dotnet test tests/CH.Native.SystemTests --filter "Category=DependencyInjection"

# Security, LINQ, and stream protocol coverage.
dotnet test tests/CH.Native.SystemTests --filter "Category=Security|Category=Linq|Category=Streams"

# Server-side failure semantics.
dotnet test tests/CH.Native.SystemTests --filter "Category=ServerFailures"

# Resilience × Chaos — needs the multi-Toxiproxy fixture.
dotnet test tests/CH.Native.SystemTests --filter "Category=Resilience"

# Stress only — heavy bulk-insert / engine / compression / backpressure on a single node.
dotnet test tests/CH.Native.SystemTests --filter "Category=Stress"

# Everything except soak (CI nightly default candidate).
dotnet test tests/CH.Native.SystemTests --filter "Category!=Soak"

# Soak only — explicit opt-in.
dotnet test tests/CH.Native.SystemTests --filter "Category=Soak"
```

Soak tests respect the `CHNATIVE_SOAK_DURATION` environment variable
(`TimeSpan` parse format, default `00:10:00`).

## Updating allocation baselines

Allocation budgets live in `baselines/allocation-budgets.json`. When an
intentional change moves the budget, regenerate with:

```bash
CHNATIVE_ALLOC_RECORD=1 dotnet test tests/CH.Native.SystemTests --filter "Category=Allocation"
```

The recording mode rewrites the baseline file in place. Review the diff and
commit it alongside the change that moved the number.

using CH.Native.Connection;
using CH.Native.Data.AggregateState;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// Reading <c>AggregateFunction(...)</c> and <c>SimpleAggregateFunction(...)</c>
/// columns — the per-row "intermediate state" emitted by <c>AggregatingMergeTree</c>
/// materialized views and any <c>*State()</c> aggregate.
///
/// Two distinct shapes:
/// <list type="bullet">
/// <item><c>AggregateFunction(fn, T...)</c> — opaque per-row state bytes, surfaced
/// as <see cref="ClickHouseAggregateState"/>. The bytes are produced and consumed by
/// ClickHouse; use <c>finalizeAggregation()</c> server-side to materialize a scalar.</item>
/// <item><c>SimpleAggregateFunction(fn, T)</c> — transparent wrapper. The wire
/// format is identical to <c>T</c>, the function name is a server-side merge hint,
/// and the column reads as the inner CLR type directly (no wrapper).</item>
/// </list>
/// </summary>
internal static class AggregateFunctionSample
{
    public static async Task RunAsync(string connectionString)
    {
        var src = $"sample_agg_events_{Guid.NewGuid():N}";
        var mv = $"sample_agg_mv_{Guid.NewGuid():N}";

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            // --- Source table + AggregatingMergeTree MV --------------------------
            // Realistic shape: events feed an MV that maintains running per-user
            // aggregates as opaque state columns.
            await connection.ExecuteNonQueryAsync($"""
                CREATE TABLE {src} (
                    user_id Int32,
                    event_value Int32
                ) ENGINE = MergeTree()
                ORDER BY user_id
                """);

            await connection.ExecuteNonQueryAsync($"""
                CREATE MATERIALIZED VIEW {mv} ENGINE = AggregatingMergeTree()
                ORDER BY user_id AS
                SELECT
                    user_id,
                    countState() AS event_count_state,
                    sumState(event_value) AS value_sum_state,
                    minState(event_value) AS min_event_state,
                    maxState(event_value) AS max_event_state
                FROM {src} GROUP BY user_id
                """);

            await connection.ExecuteNonQueryAsync($"""
                INSERT INTO {src} VALUES
                    (1, 10), (1, 20), (1, 5),
                    (2, 100),
                    (3, 7), (3, 7), (3, 7), (3, 7)
                """);
            Console.WriteLine($"Seeded {src} with 8 events across 3 users; MV {mv} captured the states.");

            // --- Read the opaque state column directly ---------------------------
            // SELECT * on a *State() MV used to throw NotSupportedException; this
            // is the path that now works for tier-1 aggregates.
            Console.WriteLine("\n--- SELECT id, *_state FROM mv — reads as ClickHouseAggregateState ---");
            await foreach (var row in connection.StreamAsync(
                $"SELECT user_id, event_count_state, value_sum_state, min_event_state, max_event_state " +
                $"FROM {mv} ORDER BY user_id"))
            {
                var userId = row.GetFieldValue<int>("user_id");
                var countState = row.GetFieldValue<ClickHouseAggregateState>("event_count_state");
                var sumState = row.GetFieldValue<ClickHouseAggregateState>("value_sum_state");
                var minState = row.GetFieldValue<ClickHouseAggregateState>("min_event_state");
                var maxState = row.GetFieldValue<ClickHouseAggregateState>("max_event_state");

                Console.WriteLine(
                    $"  user {userId}: " +
                    $"count={Hex(countState.State)} ({countState.State.Length}B), " +
                    $"sum={Hex(sumState.State)} ({sumState.State.Length}B), " +
                    $"min={Hex(minState.State)} ({minState.State.Length}B), " +
                    $"max={Hex(maxState.State)} ({maxState.State.Length}B)");
            }

            // --- Server-side finalization gives the scalar values ----------------
            // The opaque bytes aren't decoded client-side; project through
            // finalizeAggregation() (or the *Merge() variant) when you want the
            // final scalar.
            Console.WriteLine("\n--- SELECT finalizeAggregation(*) — server materializes the scalar ---");
            await foreach (var row in connection.StreamAsync($"""
                SELECT
                    user_id,
                    toInt64(finalizeAggregation(event_count_state)) AS event_count,
                    toInt64(finalizeAggregation(value_sum_state)) AS value_sum,
                    finalizeAggregation(min_event_state) AS min_event,
                    finalizeAggregation(max_event_state) AS max_event
                FROM {mv} ORDER BY user_id
                """))
            {
                Console.WriteLine(
                    $"  user {row.GetFieldValue<int>("user_id")}: " +
                    $"events={row.GetFieldValue<long>("event_count")}, " +
                    $"sum={row.GetFieldValue<long>("value_sum")}, " +
                    $"min={row.GetFieldValue<int>("min_event")}, " +
                    $"max={row.GetFieldValue<int>("max_event")}");
            }

            // --- ClickHouseAggregateState equality is byte-wise -----------------
            // Two states with identical bytes compare equal regardless of array
            // reference identity — safe for HashSet/Dictionary keys.
            var twin1 = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");
            var twin2 = new ClickHouseAggregateState(new byte[] { 1, 2, 3 }, "sum");
            Console.WriteLine($"\nIdentical-bytes equality: {twin1.Equals(twin2)} (matches ClickHouseMap convention).");

            // --- Unsupported function: actionable error -------------------------
            // For functions outside the tier-1 set, the driver throws an exception
            // that names the function and points to the workaround. Use a separate
            // connection: when the reader rejects an unsupported column header,
            // the pipe still has the partial response bytes — easier to isolate
            // than drain.
            Console.WriteLine("\n--- Unsupported function — driver throws an actionable error ---");
            await using (var aux = new ClickHouseConnection(connectionString))
            {
                await aux.OpenAsync();
                try
                {
                    _ = await aux.ExecuteScalarAsync<ClickHouseAggregateState>(
                        "SELECT uniqExactState(toUInt64(number)) FROM numbers(100)");
                }
                catch (NotSupportedException ex)
                {
                    Console.WriteLine($"  {ex.Message}");
                }
            }

            // --- SimpleAggregateFunction reads as the inner CLR type directly ---
            // The function name is a merge-time hint; the wire format is just T.
            Console.WriteLine("\n--- SimpleAggregateFunction(sum, Int64) reads as long ---");
            var simpleTable = $"sample_agg_simple_{Guid.NewGuid():N}";
            await connection.ExecuteNonQueryAsync(
                $"CREATE TABLE {simpleTable} (id Int32, total SimpleAggregateFunction(sum, Int64)) " +
                $"ENGINE = AggregatingMergeTree() ORDER BY id");
            try
            {
                await connection.ExecuteNonQueryAsync(
                    $"INSERT INTO {simpleTable} VALUES (1, 100), (1, 200), (2, 5)");
                await connection.ExecuteNonQueryAsync($"OPTIMIZE TABLE {simpleTable} FINAL");

                await foreach (var row in connection.StreamAsync<UserTotal>(
                    $"SELECT id, total FROM {simpleTable} ORDER BY id"))
                {
                    Console.WriteLine($"  user {row.Id}: total={row.Total} (CLR type: long, no wrapper)");
                }
            }
            finally
            {
                await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {simpleTable}");
            }
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {mv}");
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {src}");
            Console.WriteLine($"\nDropped {mv} and {src}.");
        }
    }

    private static string Hex(byte[] bytes) =>
        bytes.Length == 0 ? "(empty)" : Convert.ToHexString(bytes);
}

file class UserTotal
{
    [ClickHouseColumn(Name = "id")] public int Id { get; set; }
    [ClickHouseColumn(Name = "total")] public long Total { get; set; }
}

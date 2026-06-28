using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.Samples.Queries;

/// <summary>
/// Querying <c>AggregatingMergeTree</c> aggregates — the per-row "intermediate state"
/// emitted by <c>*State()</c> aggregates.
///
/// Two distinct shapes:
/// <list type="bullet">
/// <item><c>AggregateFunction(fn, T...)</c> — an opaque, server-internal state blob.
/// CH.Native is a push-and-query client and does not decode these client-side: query
/// the finalized value with <c>finalizeAggregation()</c> (or the matching <c>-Merge</c>
/// combinator) instead. Reading the raw state column fails fast with guidance; if you
/// genuinely need the raw bytes, <c>SELECT hex(col)</c> ships them as a String.</item>
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

            // --- Query the finalized values (the values you actually want) -------
            // finalizeAggregation() (or the *Merge() variant) materializes the scalar
            // server-side; it reads back through the ordinary typed readers.
            Console.WriteLine("\n--- SELECT finalizeAggregation(*) — the per-user aggregates ---");
            await foreach (var row in connection.QueryStreamAsync($"""
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

            // --- Reading a raw state column fails fast with guidance -------------
            // Selecting the opaque AggregateFunction state directly is not supported;
            // the driver throws an actionable error naming the workaround. Use a
            // separate connection: rejecting an unsupported column header leaves the
            // partial response in the pipe, so that connection is closed.
            Console.WriteLine("\n--- SELECT a raw state column — driver throws an actionable error ---");
            await using (var aux = new ClickHouseConnection(connectionString))
            {
                await aux.OpenAsync();
                try
                {
                    await foreach (var _ in aux.QueryStreamAsync($"SELECT value_sum_state FROM {mv}"))
                    {
                    }
                }
                catch (NotSupportedException ex)
                {
                    Console.WriteLine($"  {ex.Message}");
                }
            }

            // --- If you really need the raw bytes: hex() ships them as a String --
            // Version-proof and works through any client; decode/re-inject with unhex().
            Console.WriteLine("\n--- SELECT hex(state) — transfer the opaque bytes as a String ---");
            await foreach (var row in connection.QueryStreamAsync(
                $"SELECT user_id, hex(value_sum_state) AS sum_state_hex FROM {mv} ORDER BY user_id"))
            {
                Console.WriteLine(
                    $"  user {row.GetFieldValue<int>("user_id")}: sum_state={row.GetFieldValue<string>("sum_state_hex")}");
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

                await foreach (var row in connection.QueryStreamAsync<UserTotal>(
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
}

file class UserTotal
{
    [ClickHouseColumn(Name = "id")] public int Id { get; set; }
    [ClickHouseColumn(Name = "total")] public long Total { get; set; }
}

using System.Data.Common;
using CH.Native.BulkInsert;
using CH.Native.Compression;
using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.Results;
using CH.Native.SystemTests.Fixtures;
using CH.Native.Ado;
using Dapper;
using Xunit;

namespace CH.Native.SystemTests.Allocation;

/// <summary>
/// Allocation regression tests. Each test exercises a hot path against a real ClickHouse
/// instance, measures bytes allocated on the calling thread, and compares against a
/// checked-in baseline (with a tolerance band). Re-record with CHNATIVE_ALLOC_RECORD=1
/// when an intentional change moves the budget.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Allocation)]
public class AllocationBudgetTests
{
    private readonly SingleNodeFixture _fixture;
    private readonly AllocationBudget _budget = AllocationBudget.Load();

    public AllocationBudgetTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task ExecuteScalar_Int_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Warm caches (mappers, JIT) before measurement.
        _ = await conn.ExecuteScalarAsync<int>("SELECT 1");

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 100; i++)
                _ = await conn.ExecuteScalarAsync<int>("SELECT 1");
        });

        _budget.Assert("ExecuteScalar.Int.x100", bytes);
    }

    [Fact]
    public async Task QueryAsync_StreamRows_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        const string sql = "SELECT number, toString(number) FROM numbers(10000)";

        // Warm.
        await foreach (var _ in conn.QueryAsync(sql)) { }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await foreach (var row in conn.QueryAsync(sql))
            {
                _ = row.GetFieldValue<ulong>(0);
                _ = row.GetFieldValue<string>(1);
            }
        });

        _budget.Assert("QueryAsync.NumbersAndStrings.10k", bytes);
    }

    [Fact]
    public async Task ExecuteNonQuery_NoOp_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        _ = await conn.ExecuteNonQueryAsync("SELECT 1");

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 100; i++)
                _ = await conn.ExecuteNonQueryAsync("SELECT 1");
        });

        _budget.Assert("ExecuteNonQuery.Trivial.x100", bytes);
    }

    [Fact]
    public async Task ExecuteScalar_String_StaysWithinBudget()
    {
        // String materialization has a different allocation profile than int —
        // pin it so a regression in lazy-vs-eager string handling is visible.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        _ = await conn.ExecuteScalarAsync<string>("SELECT 'hello'");

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 100; i++)
                _ = await conn.ExecuteScalarAsync<string>("SELECT 'hello'");
        });

        _budget.Assert("ExecuteScalar.String.x100", bytes);
    }

    [Fact]
    public async Task BulkInsert_x10k_StaysWithinBudget()
    {
        // Bulk insert is the perf-headline path. Fills a missing budget.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var table = $"alloc_bi_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String) ENGINE = Memory");

        async Task RunAsync()
        {
            await using var ins = conn.CreateBulkInserter<Row>(table,
                new BulkInsertOptions { BatchSize = 1000 });
            await ins.InitAsync();
            for (int i = 0; i < 10_000; i++)
                await ins.AddAsync(new Row { Id = i, Name = "v" });
            await ins.CompleteAsync();
            await conn.ExecuteNonQueryAsync($"TRUNCATE TABLE {table}");
        }

        try
        {
            // Warm.
            await RunAsync();

            var bytes = await AllocationProbe.MeasureAsync(RunAsync);
            _budget.Assert("BulkInsert.10k", bytes);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task QueryAsync_Lz4Compressed_StaysWithinBudget()
    {
        // Compression toggles a different code path with its own buffer handling.
        var settings = _fixture.BuildSettings(b =>
        {
            b.WithCompression(true);
            b.WithCompressionMethod(CompressionMethod.Lz4);
        });
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        const string sql = "SELECT number, toString(number) FROM numbers(10000)";

        // Warm.
        await foreach (var _ in conn.QueryAsync(sql)) { }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await foreach (var row in conn.QueryAsync(sql))
            {
                _ = row.GetFieldValue<ulong>(0);
                _ = row.GetFieldValue<string>(1);
            }
        });

        _budget.Assert("QueryAsync.Lz4.NumbersAndStrings.10k", bytes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Group A — column reader breadth. Each test reads 10k rows of one type so
    // a regression in a specific reader is attributable to that reader.
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public Task QueryAsync_Int64_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.Int64.10k",
            "SELECT toInt64(number) FROM numbers(10000)",
            row => row.GetFieldValue<long>(0));

    [Fact]
    public Task QueryAsync_Float64_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.Float64.10k",
            "SELECT toFloat64(number) / 7 FROM numbers(10000)",
            row => row.GetFieldValue<double>(0));

    [Fact]
    public Task QueryAsync_Decimal128_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.Decimal128.10k",
            "SELECT toDecimal128(number, 4) FROM numbers(10000)",
            row => row.GetFieldValue<decimal>(0));

    [Fact]
    public Task QueryAsync_DateTime_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.DateTime.10k",
            "SELECT toDateTime('2020-01-01 00:00:00') + INTERVAL number SECOND FROM numbers(10000)",
            row => row.GetFieldValue<DateTime>(0));

    [Fact]
    public Task QueryAsync_DateTime64_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.DateTime64.10k",
            "SELECT toDateTime64('2020-01-01 00:00:00', 9) + INTERVAL number SECOND FROM numbers(10000)",
            row => row.GetFieldValue<DateTime>(0));

    [Fact]
    public Task QueryAsync_UUID_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.UUID.10k",
            "SELECT generateUUIDv4() FROM numbers(10000)",
            row => row.GetFieldValue<Guid>(0));

    [Fact]
    public Task QueryAsync_NullableInt32_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.Nullable_Int32.10k",
            "SELECT cast(if(number % 2 = 0, NULL, toInt32(number)) AS Nullable(Int32)) FROM numbers(10000)",
            row =>
            {
                if (!row.IsDBNull(0))
                    _ = row.GetFieldValue<int>(0);
            });

    [Fact]
    public Task QueryAsync_LowCardinalityString_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.LowCardinality_String.10k",
            "SELECT toLowCardinality(toString(number % 100)) FROM numbers(10000)",
            row => row.GetFieldValue<string>(0));

    [Fact]
    public Task QueryAsync_ArrayInt32_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.Array_Int32.10k",
            "SELECT [toInt32(number), toInt32(number + 1)] FROM numbers(10000)",
            row => row.GetFieldValue<int[]>(0));

    [Fact]
    public Task QueryAsync_FixedString16_StaysWithinBudget() =>
        ProjectionScenario(
            "QueryAsync.FixedString16.10k",
            "SELECT toFixedString('0123456789abcdef', 16) FROM numbers(10000)",
            row => row.GetFieldValue<byte[]>(0));

    private async Task ProjectionScenario(string scenario, string sql, Action<ClickHouseRow> read)
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        // Warm.
        await foreach (var row in conn.QueryAsync(sql)) { read(row); }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await foreach (var row in conn.QueryAsync(sql))
                read(row);
        });

        _budget.Assert(scenario, bytes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Group B — bulk insert variety. Writer-side counterpart to Group A.
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task BulkInsert_WideRow20cols_x10k_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var table = $"alloc_wide_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $@"CREATE TABLE {table} (
                c0 Int32, c1 Int64, c2 UInt32, c3 UInt64,
                c4 Int16, c5 UInt16, c6 UInt8, c7 Int8,
                c8 Float32, c9 Float64,
                c10 String, c11 String, c12 String, c13 String,
                c14 DateTime, c15 DateTime,
                c16 UUID, c17 UUID,
                c18 Int64, c19 Int64
            ) ENGINE = Memory");

        var dt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var guid = Guid.NewGuid();

        async Task RunAsync()
        {
            await using var ins = conn.CreateBulkInserter<WideRow>(table,
                new BulkInsertOptions { BatchSize = 1000 });
            await ins.InitAsync();
            for (int i = 0; i < 10_000; i++)
            {
                await ins.AddAsync(new WideRow
                {
                    C0 = i, C1 = i, C2 = (uint)i, C3 = (ulong)i,
                    C4 = (short)i, C5 = (ushort)i, C6 = (byte)(i & 0xFF), C7 = (sbyte)(i & 0x7F),
                    C8 = i, C9 = i,
                    C10 = "a", C11 = "b", C12 = "c", C13 = "d",
                    C14 = dt, C15 = dt,
                    C16 = guid, C17 = guid,
                    C18 = i, C19 = i,
                });
            }
            await ins.CompleteAsync();
            await conn.ExecuteNonQueryAsync($"TRUNCATE TABLE {table}");
        }

        try
        {
            await RunAsync();
            var bytes = await AllocationProbe.MeasureAsync(RunAsync);
            _budget.Assert("BulkInsert.WideRow_20cols.10k", bytes);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_LargeBatch_1M_100k_StaysWithinBudget()
    {
        // Targets buffer growth/reuse at scale — 10× bigger batch and 100× row
        // count vs the headline test. Catches per-batch leaks the smaller test
        // can't surface.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var table = $"alloc_bi_large_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String) ENGINE = Memory");

        async Task RunAsync()
        {
            await using var ins = conn.CreateBulkInserter<Row>(table,
                new BulkInsertOptions { BatchSize = 100_000 });
            await ins.InitAsync();
            for (int i = 0; i < 1_000_000; i++)
                await ins.AddAsync(new Row { Id = i, Name = "v" });
            await ins.CompleteAsync();
            await conn.ExecuteNonQueryAsync($"TRUNCATE TABLE {table}");
        }

        try
        {
            await RunAsync();
            var bytes = await AllocationProbe.MeasureAsync(RunAsync);
            _budget.Assert("BulkInsert.LargeBatch.1M_100k", bytes);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_NullableInt32_x10k_StaysWithinBudget()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var table = $"alloc_bi_null_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, v Nullable(Int32)) ENGINE = Memory");

        async Task RunAsync()
        {
            await using var ins = conn.CreateBulkInserter<NullableRow>(table,
                new BulkInsertOptions { BatchSize = 1000 });
            await ins.InitAsync();
            for (int i = 0; i < 10_000; i++)
                await ins.AddAsync(new NullableRow { Id = i, V = (i % 2 == 0) ? null : i });
            await ins.CompleteAsync();
            await conn.ExecuteNonQueryAsync($"TRUNCATE TABLE {table}");
        }

        try
        {
            await RunAsync();
            var bytes = await AllocationProbe.MeasureAsync(RunAsync);
            _budget.Assert("BulkInsert.Nullable_Int32.10k", bytes);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task BulkInsert_Lz4_x10k_StaysWithinBudget()
    {
        var settings = _fixture.BuildSettings(b =>
        {
            b.WithCompression(true);
            b.WithCompressionMethod(CompressionMethod.Lz4);
        });
        await using var conn = new ClickHouseConnection(settings);
        await conn.OpenAsync();

        var table = $"alloc_bi_lz4_{Guid.NewGuid():N}";
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {table} (id Int32, name String) ENGINE = Memory");

        async Task RunAsync()
        {
            await using var ins = conn.CreateBulkInserter<Row>(table,
                new BulkInsertOptions { BatchSize = 1000 });
            await ins.InitAsync();
            for (int i = 0; i < 10_000; i++)
                await ins.AddAsync(new Row { Id = i, Name = "v" });
            await ins.CompleteAsync();
            await conn.ExecuteNonQueryAsync($"TRUNCATE TABLE {table}");
        }

        try
        {
            await RunAsync();
            var bytes = await AllocationProbe.MeasureAsync(RunAsync);
            _budget.Assert("BulkInsert.Lz4.10k", bytes);
        }
        finally
        {
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {table}");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Group C — ADO reader path. This is where most third-party consumers live
    // (Dapper, EF, generic data tooling).
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DbDataReader_ReadAsync_IntString_StaysWithinBudget()
    {
        await using var conn = new ClickHouseDbConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        const string sql = "SELECT number, toString(number) FROM numbers(10000)";

        // Warm.
        await using (var warmCmd = conn.CreateCommand())
        {
            warmCmd.CommandText = sql;
            await using var warmReader = await warmCmd.ExecuteReaderAsync();
            while (await warmReader.ReadAsync())
            {
                _ = warmReader.GetValue(0);
                _ = warmReader.GetValue(1);
            }
        }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await using DbDataReader reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                _ = reader.GetValue(0);
                _ = reader.GetValue(1);
            }
        });

        _budget.Assert("DbDataReader.ReadAsync.Int_String.10k", bytes);
    }

    [Fact]
    public async Task DbDataReader_GetFieldValue_IntString_StaysWithinBudget()
    {
        // GetFieldValue<T> avoids the boxing GetValue() incurs — should sit
        // visibly below the GetValue baseline.
        await using var conn = new ClickHouseDbConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        const string sql = "SELECT number, toString(number) FROM numbers(10000)";

        await using (var warmCmd = conn.CreateCommand())
        {
            warmCmd.CommandText = sql;
            await using var warmReader = await warmCmd.ExecuteReaderAsync();
            while (await warmReader.ReadAsync())
            {
                _ = warmReader.GetFieldValue<ulong>(0);
                _ = warmReader.GetFieldValue<string>(1);
            }
        }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await using DbDataReader reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                _ = reader.GetFieldValue<ulong>(0);
                _ = reader.GetFieldValue<string>(1);
            }
        });

        _budget.Assert("DbDataReader.GetFieldValue.Int_String.10k", bytes);
    }

    [Fact]
    public async Task Dapper_Query_IntString_StaysWithinBudget()
    {
        // Dapper's reflection mapper is the most common consumer-facing path —
        // changes that improve our ADO surface are pointless if Dapper still
        // boxes everything on top.
        await using var conn = new ClickHouseDbConnection(_fixture.ConnectionString);
        await conn.OpenAsync();

        const string sql = "SELECT toUInt64(number) AS Value, toString(number) AS Name FROM numbers(10000)";

        // Warm.
        _ = (await conn.QueryAsync<DapperPair>(sql)).ToList();

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            var results = await conn.QueryAsync<DapperPair>(sql);
            foreach (var _ in results) { }
        });

        _budget.Assert("Dapper.Query_Int_String.10k", bytes);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Group D — pool & connection lifecycle. Pool dynamics affect every user;
    // a leak or per-rent allocation regression is a production-grade bug.
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DataSource_OpenConnection_Pooled_x100_StaysWithinBudget()
    {
        var options = new ClickHouseDataSourceOptions
        {
            Settings = _fixture.BuildSettings(),
            MinPoolSize = 4,
            MaxPoolSize = 8,
            PrewarmOnStart = true,
        };
        await using var ds = new ClickHouseDataSource(options);

        // Wait for prewarm to land at least one connection so we measure the
        // steady-state rent path, not the cold-create path.
        for (int i = 0; i < 50 && ds.GetStatistics().TotalCreated == 0; i++)
            await Task.Delay(50);

        // Additional warm to populate the pool with reusable connections.
        for (int i = 0; i < 8; i++)
        {
            await using var c = await ds.OpenConnectionAsync();
        }

        var bytes = await AllocationProbe.MeasureAsync(async () =>
        {
            for (int i = 0; i < 100; i++)
            {
                await using var c = await ds.OpenConnectionAsync();
            }
        });

        _budget.Assert("DataSource.OpenConnection.Pooled.x100", bytes);
    }

    private class Row
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "name", Order = 1)] public string Name { get; set; } = "";
    }

    private class NullableRow
    {
        [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
        [ClickHouseColumn(Name = "v", Order = 1)] public int? V { get; set; }
    }

    private class WideRow
    {
        [ClickHouseColumn(Name = "c0", Order = 0)] public int C0 { get; set; }
        [ClickHouseColumn(Name = "c1", Order = 1)] public long C1 { get; set; }
        [ClickHouseColumn(Name = "c2", Order = 2)] public uint C2 { get; set; }
        [ClickHouseColumn(Name = "c3", Order = 3)] public ulong C3 { get; set; }
        [ClickHouseColumn(Name = "c4", Order = 4)] public short C4 { get; set; }
        [ClickHouseColumn(Name = "c5", Order = 5)] public ushort C5 { get; set; }
        [ClickHouseColumn(Name = "c6", Order = 6)] public byte C6 { get; set; }
        [ClickHouseColumn(Name = "c7", Order = 7)] public sbyte C7 { get; set; }
        [ClickHouseColumn(Name = "c8", Order = 8)] public float C8 { get; set; }
        [ClickHouseColumn(Name = "c9", Order = 9)] public double C9 { get; set; }
        [ClickHouseColumn(Name = "c10", Order = 10)] public string C10 { get; set; } = "";
        [ClickHouseColumn(Name = "c11", Order = 11)] public string C11 { get; set; } = "";
        [ClickHouseColumn(Name = "c12", Order = 12)] public string C12 { get; set; } = "";
        [ClickHouseColumn(Name = "c13", Order = 13)] public string C13 { get; set; } = "";
        [ClickHouseColumn(Name = "c14", Order = 14)] public DateTime C14 { get; set; }
        [ClickHouseColumn(Name = "c15", Order = 15)] public DateTime C15 { get; set; }
        [ClickHouseColumn(Name = "c16", Order = 16)] public Guid C16 { get; set; }
        [ClickHouseColumn(Name = "c17", Order = 17)] public Guid C17 { get; set; }
        [ClickHouseColumn(Name = "c18", Order = 18)] public long C18 { get; set; }
        [ClickHouseColumn(Name = "c19", Order = 19)] public long C19 { get; set; }
    }

    private class DapperPair
    {
        public ulong Value { get; set; }
        public string Name { get; set; } = "";
    }
}

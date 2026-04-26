using CH.Native.Connection;
using CH.Native.Mapping;

namespace CH.Native.SystemTests.BulkInsertFailures.Helpers;

/// <summary>
/// Per-test table harness: creates a unique <c>MergeTree</c> table on construction,
/// drops it on dispose. Audits <c>SELECT count()</c> on demand from a fresh
/// connection (so a poisoned test connection cannot mask the result).
/// </summary>
internal sealed class BulkInsertTableHarness : IAsyncDisposable
{
    private readonly Func<ClickHouseConnectionSettings> _settingsFactory;

    public string TableName { get; }

    private BulkInsertTableHarness(string tableName, Func<ClickHouseConnectionSettings> settingsFactory)
    {
        TableName = tableName;
        _settingsFactory = settingsFactory;
    }

    public static async Task<BulkInsertTableHarness> CreateAsync(
        Func<ClickHouseConnectionSettings> settingsFactory,
        string namePrefix = "bi_test")
    {
        return await CreateAsync(settingsFactory, columnDdl: "id Int32, payload String", namePrefix);
    }

    /// <summary>
    /// Variant that accepts a custom column DDL so tests can exercise schemas
    /// other than <c>(id Int32, payload String)</c> (e.g. <c>Nullable(String)</c>,
    /// <c>FixedString(N)</c>) without duplicating the create/drop ceremony.
    /// </summary>
    public static async Task<BulkInsertTableHarness> CreateAsync(
        Func<ClickHouseConnectionSettings> settingsFactory,
        string columnDdl,
        string namePrefix = "bi_test")
    {
        var name = $"{namePrefix}_{Guid.NewGuid():N}";
        await using var conn = new ClickHouseConnection(settingsFactory());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {name} ({columnDdl}) ENGINE = MergeTree ORDER BY id");
        return new BulkInsertTableHarness(name, settingsFactory);
    }

    /// <summary>
    /// Total row count via a fresh connection. Use this for chaos audits — the
    /// connection that drove the test may be poisoned.
    /// </summary>
    public async Task<ulong> CountAsync()
    {
        await using var conn = new ClickHouseConnection(_settingsFactory());
        await conn.OpenAsync();
        return await conn.ExecuteScalarAsync<ulong>($"SELECT count() FROM {TableName}");
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await using var conn = new ClickHouseConnection(_settingsFactory());
            await conn.OpenAsync();
            await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {TableName}");
        }
        catch
        {
            // Test teardown — best effort. A poisoned proxy in a chaos test can
            // legitimately prevent drop; the next test's CREATE uses a unique name.
        }
    }
}

/// <summary>Standard test row matching the harness's <c>(id Int32, payload String)</c>.</summary>
internal sealed class StandardRow
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "payload", Order = 1)] public string Payload { get; set; } = "";
}

/// <summary>Variant for <c>Nullable(String)</c> columns. The C# type is
/// <c>string?</c> so callers can naturally write <c>Payload = null</c>.</summary>
internal sealed class NullablePayloadRow
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "payload", Order = 1)] public string? Payload { get; set; }
}

/// <summary>Forces the boxed/fallback insert path: the <c>Tags</c> property is
/// <c>int[]</c>, which the direct-extractor factory rejects with
/// <c>NotSupportedException</c>, flipping <c>_useDirectPath = false</c> in
/// <c>BulkInserter</c>. Used to exercise the IColumnWriter-side fixes.</summary>
internal sealed class RowWithArrayAndString
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "tags", Order = 1)] public int[] Tags { get; set; } = Array.Empty<int>();
    [ClickHouseColumn(Name = "payload", Order = 2)] public string Payload { get; set; } = "";
}

/// <summary>Same shape as <see cref="RowWithArrayAndString"/> but with a
/// <c>string?</c> payload for the <c>Nullable(String)</c> regression guard.</summary>
internal sealed class RowWithArrayAndNullableString
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "tags", Order = 1)] public int[] Tags { get; set; } = Array.Empty<int>();
    [ClickHouseColumn(Name = "payload", Order = 2)] public string? Payload { get; set; }
}

/// <summary>Same fallback-path trick as <see cref="RowWithArrayAndString"/>
/// (the <c>Tags</c> property forces <c>_useDirectPath = false</c>) but with a
/// <c>Map(String, Int32)</c> payload column. Used to pin
/// <c>MapColumnWriter</c>'s strict-null contract on the boxed insert path.</summary>
internal sealed class RowWithArrayAndMap
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "tags", Order = 1)] public int[] Tags { get; set; } = Array.Empty<int>();
    [ClickHouseColumn(Name = "mapping", Order = 2)] public Dictionary<string, int>? Mapping { get; set; }
}

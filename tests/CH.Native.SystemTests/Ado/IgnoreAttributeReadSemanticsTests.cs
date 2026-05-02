using CH.Native.Connection;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the read-side contract for <see cref="ClickHouseColumnAttribute.Ignore"/>:
/// the property is excluded from row mapping regardless of whether the SELECT
/// includes a matching column. Mirrors the bulk-insert-side semantics.
/// Covers both the slow-path <c>TypeMapper</c> (driving
/// <c>connection.QueryAsync&lt;T&gt;</c>) and the fast-path
/// <c>ReflectionTypedRowMapper</c> (driving <c>connection.QueryTypedAsync&lt;T&gt;</c>).
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class IgnoreAttributeReadSemanticsTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private readonly string _table = $"ignore_read_{Guid.NewGuid():N}";

    public IgnoreAttributeReadSemanticsTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_table} (user_id Int32, display_name String, ignored_field String) " +
            "ENGINE = MergeTree ORDER BY user_id");
        await conn.ExecuteNonQueryAsync(
            $"INSERT INTO {_table} VALUES (1, 'alice', 'hidden_a'), (2, 'bob', 'hidden_b')");
    }

    public async Task DisposeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_table}");
    }

    [Fact]
    public async Task SlowPath_QueryAsync_LeavesIgnoredPropertyAtDefault()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<RowWithIgnore>();
        await foreach (var row in conn.QueryAsync<RowWithIgnore>(
            $"SELECT user_id, display_name, ignored_field FROM {_table} ORDER BY user_id"))
        {
            rows.Add(row);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].UserId);
        Assert.Equal("alice", rows[0].DisplayName);
        Assert.Equal("default", rows[0].IgnoredField);
        Assert.Equal("default", rows[1].IgnoredField);
    }

    [Fact]
    public async Task FastPath_QueryTypedAsync_LeavesIgnoredPropertyAtDefault()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<RowWithIgnore>();
        await foreach (var row in conn.QueryTypedAsync<RowWithIgnore>(
            $"SELECT user_id, display_name, ignored_field FROM {_table} ORDER BY user_id"))
        {
            rows.Add(row);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].UserId);
        Assert.Equal("alice", rows[0].DisplayName);
        Assert.Equal("default", rows[0].IgnoredField);
    }

    [Fact]
    public async Task SlowPath_QueryAsync_StillPopulatesNonIgnoredColumnsViaSnakeCaseFallback()
    {
        // Sanity: skipping ignored properties must not break the
        // snake_case fallback for the others.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<RowWithIgnore>();
        await foreach (var row in conn.QueryAsync<RowWithIgnore>(
            $"SELECT user_id, display_name, ignored_field FROM {_table} ORDER BY user_id"))
        {
            rows.Add(row);
        }

        Assert.All(rows, r => Assert.NotEqual(0, r.UserId));
        Assert.All(rows, r => Assert.NotEmpty(r.DisplayName));
    }

    [Fact]
    public async Task ArgsCtorPath_IgnoreOnRequiredCtorParam_ThrowsTypedException()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in conn.QueryAsync<RecordWithIgnoredCtorParam>(
                $"SELECT user_id, display_name, ignored_field FROM {_table}"))
            {
            }
        });

        _output.WriteLine($"Surfaced: {ex.Message}");
        Assert.Contains("Ignore", ex.Message);
    }

    internal sealed class RowWithIgnore
    {
        [ClickHouseColumn(Name = "user_id")] public int UserId { get; set; }
        [ClickHouseColumn(Name = "display_name")] public string DisplayName { get; set; } = "";
        [ClickHouseColumn(Ignore = true)] public string IgnoredField { get; set; } = "default";
    }

    // Records propagate property attributes through the [property:] target,
    // so [ClickHouseColumn(Ignore = true)] applied to a positional ctor
    // param ends up on the generated property. Pin: that combination must
    // surface a typed exception rather than silently substitute default(T).
    internal sealed record RecordWithIgnoredCtorParam(
        [property: ClickHouseColumn(Name = "user_id")] int UserId,
        [property: ClickHouseColumn(Name = "display_name")] string DisplayName,
        [property: ClickHouseColumn(Ignore = true)] string IgnoredField);
}

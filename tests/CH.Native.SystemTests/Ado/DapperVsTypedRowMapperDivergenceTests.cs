using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Dapper;
using CH.Native.Mapping;
using CH.Native.SystemTests.Fixtures;
using Dapper;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the parity (and remaining divergence) between the typed-row-mapper
/// used by <c>connection.QueryAsync&lt;T&gt;</c> and the Dapper type-map
/// used by <c>dbConnection.QueryAsync&lt;T&gt;</c>. The typed path honours
/// <see cref="ClickHouseColumnAttribute"/> and has a built-in snake_case
/// fallback in <c>TypeMapper.TryGetOrdinal</c>. The Dapper path does not
/// honour <c>ClickHouseColumnAttribute</c>, but
/// <see cref="ClickHouseDapperIntegration.Register"/> sets
/// <c>DefaultTypeMap.MatchNamesWithUnderscores = true</c> so it reaches
/// the same out-of-the-box snake_case → PascalCase behaviour. Callers who
/// want raw-name Dapper mapping can override the property back to
/// <c>false</c> after calling <c>Register()</c>.
///
/// <para>
/// Note: <c>ClickHouseColumnAttribute.Ignore</c> is documented as a
/// <i>bulk-insert</i> opt-out, not a read-side skip. The read mappers do
/// not consult it. The historical "Dapper sees fields the typed mapper
/// hides" framing therefore doesn't apply at read time.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class DapperVsTypedRowMapperDivergenceTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;
    private readonly string _table = $"divergence_{Guid.NewGuid():N}";

    public DapperVsTypedRowMapperDivergenceTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    public async Task InitializeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync(
            $"CREATE TABLE {_table} (user_id Int32, display_name String) " +
            "ENGINE = MergeTree ORDER BY user_id");
        await conn.ExecuteNonQueryAsync(
            $"INSERT INTO {_table} VALUES (1, 'alice'), (2, 'bob')");
    }

    public async Task DisposeAsync()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();
        await conn.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {_table}");
    }

    [Fact]
    public async Task TypedPath_WithClickHouseColumnNameAttribute_MapsSnakeCaseToPascal()
    {
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<UserRowWithAttribute>();
        await foreach (var row in conn.QueryAsync<UserRowWithAttribute>(
            $"SELECT user_id, display_name FROM {_table} ORDER BY user_id"))
        {
            rows.Add(row);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].UserId);
        Assert.Equal("alice", rows[0].DisplayName);
    }

    [Fact]
    public async Task TypedPath_WithoutAttribute_FallsBackToSnakeCaseLookup()
    {
        // The typed mapper has a snake_case fallback in TypeMapper.TryGetOrdinal:
        // first GetOrdinal(propertyName), and if that misses, GetOrdinal(snake_case(propertyName)).
        // So UserRowNoAttribute (no [ClickHouseColumn]) still maps to user_id via the fallback.
        // This bridges the convention without per-property attributes — pin it as a contract.
        await using var conn = new ClickHouseConnection(_fx.BuildSettings());
        await conn.OpenAsync();

        var rows = new List<UserRowNoAttribute>();
        await foreach (var row in conn.QueryAsync<UserRowNoAttribute>(
            $"SELECT user_id, display_name FROM {_table} ORDER BY user_id"))
        {
            rows.Add(row);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].UserId);
        Assert.Equal("alice", rows[0].DisplayName);
    }

    [Fact]
    public async Task DapperPath_AfterRegister_BridgesSnakeCaseToPascalCase()
    {
        // ClickHouseDapperIntegration.Register sets
        // DefaultTypeMap.MatchNamesWithUnderscores = true so the Dapper path
        // bridges snake_case columns to PascalCase properties out of the
        // box, matching the typed mapper's snake_case fallback.
        ClickHouseDapperIntegration.Register();

        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var rows = (await conn.QueryAsync<UserRowDapperConvention>(
            $"SELECT user_id, display_name FROM {_table} ORDER BY user_id")).ToList();

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].UserId);
        Assert.Equal("alice", rows[0].DisplayName);
        Assert.Equal(2, rows[1].UserId);
        Assert.Equal("bob", rows[1].DisplayName);
    }

    [Fact]
    public async Task DapperPath_CallerCanOptOutOfUnderscoreMatching_AfterRegister()
    {
        // Verifies the override pattern documented on Register's XML doc:
        // a caller who wants raw-name Dapper mapping flips the global back
        // to false after calling Register, and the override survives. Without
        // the bridge, snake_case columns no longer populate PascalCase
        // properties — defaults remain.
        //
        // Uses a dedicated POCO type (UserRowOptOutPoco) so Dapper's
        // per-type deserializer cache built here with MNWU=false doesn't
        // pollute the cache for UserRowDapperConvention used in the bridge
        // test. Dapper compiles deserializer IL once per (sql, type) and
        // bakes in the column→property mapping current at compile time, so
        // toggling MNWU mid-process only affects deserializers compiled
        // after the toggle. Production callers don't typically toggle at
        // runtime, but the test must be hermetic.
        ClickHouseDapperIntegration.Register();
        var originalValue = DefaultTypeMap.MatchNamesWithUnderscores;
        DefaultTypeMap.MatchNamesWithUnderscores = false;
        try
        {
            await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
            await conn.OpenAsync();

            var rows = (await conn.QueryAsync<UserRowOptOutPoco>(
                $"SELECT user_id, display_name FROM {_table} ORDER BY user_id")).ToList();

            Assert.Equal(2, rows.Count);
            Assert.Equal(0, rows[0].UserId);
            Assert.Equal(string.Empty, rows[0].DisplayName);

            // A second Register call must not clobber the caller's override.
            ClickHouseDapperIntegration.Register();
            Assert.False(DefaultTypeMap.MatchNamesWithUnderscores);
        }
        finally
        {
            DefaultTypeMap.MatchNamesWithUnderscores = originalValue;
        }
    }

    [Fact]
    public async Task BothPaths_AgreeOnRowCount_EvenWhereTheyDifferOnFieldMapping()
    {
        // The divergence is in field-mapping rules, not in row emission.
        // Both paths see the same number of rows. Useful regression
        // guard: a future refactor that breaks one path's iteration
        // would surface here.
        ClickHouseDapperIntegration.Register();

        await using var nativeConn = new ClickHouseConnection(_fx.BuildSettings());
        await nativeConn.OpenAsync();
        await using var dbConn = new ClickHouseDbConnection(_fx.ConnectionString);
        await dbConn.OpenAsync();

        var typedCount = 0;
        await foreach (var _ in nativeConn.QueryAsync<UserRowWithAttribute>(
            $"SELECT user_id, display_name FROM {_table}"))
            typedCount++;

        var dapperCount = (await dbConn.QueryAsync<UserRowDapperConvention>(
            $"SELECT user_id, display_name FROM {_table}")).Count();

        _output.WriteLine($"Typed: {typedCount} rows, Dapper: {dapperCount} rows");
        Assert.Equal(typedCount, dapperCount);
    }

    internal sealed class UserRowWithAttribute
    {
        [ClickHouseColumn(Name = "user_id", Order = 0)] public int UserId { get; set; }
        [ClickHouseColumn(Name = "display_name", Order = 1)] public string DisplayName { get; set; } = "";
    }

    internal sealed class UserRowNoAttribute
    {
        public int UserId { get; set; }
        public string DisplayName { get; set; } = "";
    }

    // Dapper's MatchNamesWithUnderscores convention: PascalCase property
    // mapped to snake_case column.
    internal sealed class UserRowDapperConvention
    {
        public int UserId { get; set; }
        public string DisplayName { get; set; } = "";
    }

    // Same shape as UserRowDapperConvention but a distinct type so the
    // opt-out test gets its own slot in Dapper's per-type deserializer
    // cache, leaving the bridge test's deserializer untouched.
    internal sealed class UserRowOptOutPoco
    {
        public int UserId { get; set; }
        public string DisplayName { get; set; } = "";
    }
}

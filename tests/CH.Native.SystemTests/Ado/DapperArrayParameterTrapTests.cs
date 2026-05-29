using System.Data;
using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Commands;
using CH.Native.Results;
// CH.Native.Dapper not imported to avoid IDbConnection extension ambiguity with Dapper namespace; qualify Register() calls below.
using CH.Native.Exceptions;
using CH.Native.SystemTests.Fixtures;
using Dapper;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Ado;

/// <summary>
/// Pins the most-cited Dapper user-trap: passing an array parameter to a
/// query that needs <c>Array(T)</c> on the wire (e.g. <c>hasAny</c>). Dapper's
/// default list-expansion rewrites the parameter into a tuple literal — which
/// ClickHouse rejects because the function signature requires an array.
///
/// <para>
/// The escape hatches are:
/// </para>
/// <list type="number">
/// <item>Drop to ADO.NET <see cref="ClickHouseCommand"/> with an explicit
/// <see cref="ClickHouseDbParameter.ClickHouseType"/> set to <c>Array(...)</c>.</item>
/// <item>Register <see cref="ClickHouseDapperArrayHandler{T}"/> via
/// <see cref="ClickHouseDapperIntegration.Register"/>, then use Dapper natively
/// with <c>T[]</c>.</item>
/// </list>
///
/// <para>
/// The trap demonstration uses <see cref="List{T}"/> rather than <c>T[]</c> so
/// it is robust against another test class having already called
/// <c>CH.Native.Dapper.ClickHouseDapperIntegration.Register()</c> earlier in the run — the array
/// handler covers <c>T[]</c>, not <c>List&lt;T&gt;</c>.
/// </para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Suite)]
public class DapperArrayParameterTrapTests
{
    private readonly SingleNodeFixture _fx;
    private readonly ITestOutputHelper _output;

    public DapperArrayParameterTrapTests(SingleNodeFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _output = output;
    }

    // The "Trap_DapperHasAny_*" tests that previously lived here documented a
    // Dapper limitation (List<int> expanding to a tuple instead of an array)
    // that is no longer reachable in practice: CH.Native.Dapper's fast path
    // routes arrays through the native parameter binder, and once
    // ClickHouseDapperIntegration.Register() has run process-wide the array
    // handler covers the Dapper IDbConnection path too. The surviving
    // EscapeHatch_* tests below pin the documented recovery paths.

    [Fact]
    public async Task EscapeHatch_AdoNetCommand_WithExplicitClickHouseDbParameter_RoundTrips()
    {
        // Bypass Dapper. ADO.NET callers attach a ClickHouseDbParameter with
        // an explicit ClickHouseType — the array serialises as Array(Int32)
        // on the wire and hasAny() succeeds.
        await using var conn = new ClickHouseConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        using var cmd = (ClickHouseCommand)conn.CreateCommand();
        cmd.CommandText = "SELECT 1 WHERE hasAny([1, 2, 4], @ids)";
        cmd.Parameters.Add(new ClickHouseParameter
        {
            ParameterName = "ids",
            Value = new[] { 1, 2, 3 },
            ClickHouseType = "Array(Int32)",
        });

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(1, Convert.ToInt32(result));
    }

    [Fact]
    public async Task EscapeHatch_DapperWithArrayHandlerRegistered_RoundTrips()
    {
        // Register the global Dapper array handler. With it in place, T[]
        // parameters bind verbatim and the wire SQL gets Array(Int32) rather
        // than a tuple expansion. Registration is idempotent and process-wide;
        // safe to call here without affecting other tests.
        CH.Native.Dapper.ClickHouseDapperIntegration.Register();

        await using var conn = new ClickHouseConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var ids = new[] { 1, 2, 3 };
        var rows = (await conn.QueryAsync<int>(
            "SELECT 1 WHERE hasAny([1, 2, 4], @ids)",
            new { ids })).ToList();

        Assert.Single(rows);
        Assert.Equal(1, rows[0]);
    }
}

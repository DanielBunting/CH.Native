using System.Data;
using CH.Native.Ado;
using CH.Native.Dapper;
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
/// <item>Drop to ADO.NET <see cref="ClickHouseDbCommand"/> with an explicit
/// <see cref="ClickHouseDbParameter.ClickHouseType"/> set to <c>Array(...)</c>.</item>
/// <item>Register <see cref="ClickHouseDapperArrayHandler{T}"/> via
/// <see cref="ClickHouseDapperIntegration.Register"/>, then use Dapper natively
/// with <c>T[]</c>.</item>
/// </list>
///
/// <para>
/// The trap demonstration uses <see cref="List{T}"/> rather than <c>T[]</c> so
/// it is robust against another test class having already called
/// <c>ClickHouseDapperIntegration.Register()</c> earlier in the run — the array
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

    [Fact]
    public async Task Trap_DapperHasAny_WithListInt_FailsWithArrayTypeMismatch()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var ids = new List<int> { 1, 2, 3 };

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await conn.QueryAsync<int>(
                "SELECT 1 WHERE hasAny([1, 2, 4], @ids)",
                new { ids });
        });

        _output.WriteLine($"Trap surfaced: {ex.GetType().Name}: {ex.Message}");

        Assert.IsAssignableFrom<ClickHouseServerException>(ex);
    }

    [Fact]
    public async Task Trap_ErrorMessage_GrepsForArrayOrTuple()
    {
        // Defence-in-depth check: the diagnostic must surface the user-facing
        // hint they need to grep their logs for. Without "Array" or "Tuple"
        // in the message, a confused user has no breadcrumb back to the
        // documented escape hatches.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var ids = new List<int> { 1, 2, 3 };
        Exception? caught = null;
        try
        {
            await conn.QueryAsync<int>(
                "SELECT 1 WHERE hasAny([1, 2, 4], @ids)",
                new { ids });
        }
        catch (Exception ex) { caught = ex; }

        Assert.NotNull(caught);
        var message = caught!.Message;
        _output.WriteLine($"Error message: {message}");

        var mentionsArray = message.Contains("Array", StringComparison.OrdinalIgnoreCase);
        var mentionsTuple = message.Contains("Tuple", StringComparison.OrdinalIgnoreCase);
        Assert.True(mentionsArray || mentionsTuple,
            $"Expected error message to reference Array or Tuple so users can self-diagnose. Got: {message}");
    }

    [Fact]
    public async Task EscapeHatch_AdoNetCommand_WithExplicitClickHouseDbParameter_RoundTrips()
    {
        // Bypass Dapper. ADO.NET callers attach a ClickHouseDbParameter with
        // an explicit ClickHouseType — the array serialises as Array(Int32)
        // on the wire and hasAny() succeeds.
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        using var cmd = (ClickHouseDbCommand)conn.CreateCommand();
        cmd.CommandText = "SELECT 1 WHERE hasAny([1, 2, 4], @ids)";
        cmd.Parameters.Add(new ClickHouseDbParameter
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
        ClickHouseDapperIntegration.Register();

        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();

        var ids = new[] { 1, 2, 3 };
        var rows = (await conn.QueryAsync<int>(
            "SELECT 1 WHERE hasAny([1, 2, 4], @ids)",
            new { ids })).ToList();

        Assert.Single(rows);
        Assert.Equal(1, rows[0]);
    }
}

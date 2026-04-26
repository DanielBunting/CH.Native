using CH.Native.Commands;
using CH.Native.Connection;
using CH.Native.Data.Types;
using CH.Native.Parameters;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Security.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Pins the contract on hostile parameter <i>type-name</i> strings supplied via
/// <see cref="ClickHouseParameterCollection.Add(string, object?, string)"/>. The
/// <c>clickHouseType</c> argument is currently propagated unchecked into the
/// wire <c>{name:Type}</c> placeholder by <see cref="SqlParameterRewriter.Rewrite"/>,
/// which makes it a textbook injection vector.
///
/// <para><b>Most rejection tests in this file are expected to fail today.</b>
/// They drive the fix toward gating the <c>clickHouseType</c> string through
/// <see cref="ClickHouseTypeParser.Parse"/> (or an equivalent whitelist) before
/// it reaches the wire. Leaving them red is the point — they document the gap.</para>
///
/// <para>The wire-spy test asserts that, when validation throws, no query bytes
/// are sent. <see cref="SqlParameterRewriter.Process"/> is a pure string transform,
/// so an exception there guarantees zero wire activity by construction.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Security)]
public class ParameterTypeNameInjectionTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fixture;
    private SentinelTable _sentinel = null!;

    public ParameterTypeNameInjectionTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _sentinel = await SentinelTable.CreateAsync(() => _fixture.BuildSettings());
    }

    public Task DisposeAsync() => _sentinel.DisposeAsync().AsTask();

    // --- Valid type-name acceptance: these should pass today and remain green. ---

    [Theory]
    [InlineData("Int32")]
    [InlineData("UInt64")]
    [InlineData("String")]
    [InlineData("Nullable(Int32)")]
    [InlineData("Array(String)")]
    [InlineData("Map(String, Int32)")]
    [InlineData("Tuple(Int32, String)")]
    [InlineData("DateTime64(3)")]
    [InlineData("Decimal128(18)")]
    [InlineData("FixedString(32)")]
    public void Add_TypeName_ValidShape_Accepted(string typeName)
    {
        var parameters = new ClickHouseParameterCollection();

        var ex = Record.Exception(() => parameters.Add("v", "ignored", typeName));

        Assert.Null(ex);
        Assert.Equal(typeName, parameters["v"].ResolvedTypeName);
    }

    // --- Hostile type-name rejection: expected to FAIL today. ---

    [Theory]
    [InlineData("Int32; DROP TABLE existing")]
    [InlineData("Int32) UNION SELECT password FROM users -- ")]
    [InlineData("Int32 -- ")]
    [InlineData("Int32 /* hidden */")]
    [InlineData("Int32}\nbreakout")]
    [InlineData("Int32{x:Int32}")]
    public void Add_TypeName_Hostile_Rejected_AtAddOrAtRewrite(string hostileType)
    {
        // The rejection can be either eager (at Add) or lazy (at Process/Rewrite).
        // Both points are upstream of any wire activity, so either is acceptable —
        // this test passes if EITHER throws. It fails today because neither does.
        var parameters = new ClickHouseParameterCollection();

        Exception? caught = null;
        try
        {
            parameters.Add("v", 42, hostileType);
            // If Add accepted it, force the rewriter — it's the next gate.
            _ = SqlParameterRewriter.Process("SELECT @v", parameters);
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.True(
            caught is ArgumentException || caught is FormatException,
            $"Expected ArgumentException or FormatException, got {caught!.GetType().Name}: {caught.Message}");
    }

    [Fact]
    public void Add_TypeName_LeadingTrailingWhitespace_BehaviourPinned()
    {
        // Policy: trim-then-validate. Surrounding whitespace is stripped at the
        // setter boundary; the canonical (trimmed) form is what flows to the wire.
        // The parser would otherwise reject leading/trailing whitespace outright,
        // so trimming keeps the API forgiving without weakening the gate.
        var parameters = new ClickHouseParameterCollection();

        var ex = Record.Exception(() => parameters.Add("v", 42, " Int32 "));

        Assert.Null(ex);
        Assert.Equal("Int32", parameters["v"].ResolvedTypeName);
    }

    /// <summary>
    /// Wire-spy assertion: <see cref="SqlParameterRewriter.Process"/> is a pure
    /// client-side transform. If it throws on a hostile type name, we have a
    /// proof-by-construction that no wire bytes were sent. This test passes the
    /// moment validation is added at either <c>Add</c> or <c>Rewrite</c>.
    /// </summary>
    [Fact]
    public void Process_HostileTypeName_NoWireActivity()
    {
        var parameters = new ClickHouseParameterCollection();
        var hostileType = "Int32; DROP TABLE existing";

        Exception? caught = null;
        try
        {
            // Direct path to the rewriter — bypasses the connection entirely.
            // If this throws, no socket was opened, no query was sent.
            parameters.Add("v", 42, hostileType);
            _ = SqlParameterRewriter.Process("SELECT @v", parameters);
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
    }

    /// <summary>
    /// End-to-end variant: if the validation gap is closed, the connection-level
    /// call must throw before any data leaves the client. Even if the gap is open,
    /// the server-side query parser will reject the malformed <c>{v:Int32; DROP …}</c>
    /// placeholder — but at that point the bytes have already been sent. The
    /// <c>caught is ArgumentException</c> branch is what we ultimately want.
    /// </summary>
    [Fact]
    public async Task ExecuteScalar_HostileTypeName_ThrowsBeforeOrAtServer_SentinelSurvives()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        await using var command = conn.CreateCommand("SELECT @v");
        var hostileType = $"Int32; DROP TABLE {_sentinel.TableName}";

        // The rejection can happen either at parameter-add time (eager validation in
        // ClickHouseParameter.ClickHouseType setter) or later at the wire — both are
        // upstream of any byte leaving the client, so either is acceptable.
        var ex = await Record.ExceptionAsync(async () =>
        {
            command.Parameters.Add("v", 42, hostileType);
            await command.ExecuteScalarAsync<int>();
        });

        Assert.NotNull(ex);
        Assert.True(await _sentinel.ExistsAsync(),
            $"Sentinel {_sentinel.TableName} was dropped — type-name injection breached the wire.");
    }
}

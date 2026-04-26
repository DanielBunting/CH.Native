using System.Text;
using CH.Native.Connection;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Security.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Round-trips every hostile string payload from the Gap-6 design doc through a
/// real ClickHouse server, both via parameterised <c>SELECT @v</c> and via
/// parameterised <c>INSERT … VALUES</c> followed by <c>SELECT</c>. The point is
/// to confirm the escape contracts at <see cref="ParameterSerializer.EscapeStringForParameter"/>
/// hold end-to-end against the server's two-pass parameter decoder, not just at
/// the unit boundary.
///
/// <para>Where a payload directly threatens a sentinel table, an out-of-band
/// existence check on a separate connection asserts the table survived.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Security)]
public class ParameterValueEscapingTests : IAsyncLifetime
{
    private readonly SingleNodeFixture _fixture;
    private SentinelTable _sentinel = null!;

    public ParameterValueEscapingTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    public async Task InitializeAsync()
    {
        _sentinel = await SentinelTable.CreateAsync(() => _fixture.BuildSettings());
    }

    public Task DisposeAsync() => _sentinel.DisposeAsync().AsTask();

    [Theory]
    [InlineData("' OR '1'='1")]
    [InlineData("\\' OR \\'1\\'=\\'1")]
    [InlineData("'); DROP TABLE existing; --")]
    [InlineData("foo'; --")]
    public async Task Parameter_SingleQuoteAndComment_ExactRoundTrip(string payload)
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var result = await conn.ExecuteScalarAsync<string>("SELECT @v", new { v = payload });

        Assert.Equal(payload, result);
    }

    [Fact]
    public async Task Parameter_TerminatorThenComment_NoTruncation_SentinelSurvives()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var payload = $"foo'; DROP TABLE {_sentinel.TableName} -- ";

        var result = await conn.ExecuteScalarAsync<string>("SELECT @v", new { v = payload });

        Assert.Equal(payload, result);
        Assert.True(await _sentinel.ExistsAsync(),
            $"Sentinel table {_sentinel.TableName} was dropped — escape failed.");
    }

    [Theory]
    [InlineData("foo/*")]
    [InlineData("*/bar")]
    [InlineData("foo/* hidden */bar")]
    public async Task Parameter_BlockComment_NoEffect(string payload)
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var result = await conn.ExecuteScalarAsync<string>("SELECT @v", new { v = payload });

        Assert.Equal(payload, result);
    }

    [Fact]
    public async Task Parameter_NullByte_PreservedNotTruncated()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var payload = "admin\0bypass";

        var result = await conn.ExecuteScalarAsync<string>("SELECT @v", new { v = payload });

        Assert.NotNull(result);
        Assert.Equal(payload.Length, result!.Length);
        Assert.Contains('\0', result);
        Assert.Equal(payload, result);
    }

    [Theory]
    [InlineData("foo\nbar")]
    [InlineData("foo\\bar")]
    [InlineData("foo\\\\bar")]
    [InlineData("foo\\'bar")]
    [InlineData("foo\tbar")]
    [InlineData("foo\rbar")]
    public async Task Parameter_BackslashAndControlSequences_DoubleEscaped(string payload)
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var result = await conn.ExecuteScalarAsync<string>("SELECT @v", new { v = payload });

        Assert.Equal(payload, result);
    }

    [Fact]
    public async Task Parameter_AllControlBytes_0x01_To_0x1F_RoundTrip()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var sb = new StringBuilder();
        for (char c = (char)0x01; c <= 0x1F; c++)
            sb.Append(c);
        var payload = sb.ToString();

        var result = await conn.ExecuteScalarAsync<string>("SELECT @v", new { v = payload });

        Assert.NotNull(result);
        Assert.Equal(payload.Length, result!.Length);
        Assert.True(
            Encoding.UTF8.GetBytes(payload).SequenceEqual(Encoding.UTF8.GetBytes(result)),
            "Control-byte payload did not round-trip byte-for-byte.");
    }

    [Fact]
    public async Task Parameter_ClassicDropPayload_TableSurvives()
    {
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();

        var payload = $"'; DROP TABLE {_sentinel.TableName}; --";

        var result = await conn.ExecuteScalarAsync<string>("SELECT @v", new { v = payload });

        Assert.Equal(payload, result);
        Assert.True(await _sentinel.ExistsAsync(),
            $"Sentinel table {_sentinel.TableName} was dropped — escape failed.");
    }

    public static IEnumerable<object[]> InsertSelectPayloads => new[]
    {
        new object[] { "' OR '1'='1" },
        new object[] { "foo'; DROP TABLE existing -- " },
        new object[] { "foo/*" },
        new object[] { "*/bar" },
        new object[] { "admin\0bypass" },
        new object[] { "foo\nbar" },
        new object[] { "foo\\bar" },
        new object[] { "foo\\\\bar" },
        new object[] { "foo\\'bar" },
        new object[] { "foo\tbar" },
        new object[] { "foo\rbar" },
        new object[] { "back\bspace" },
        new object[] { "form\ffeed" },
    };

    [Theory]
    [MemberData(nameof(InsertSelectPayloads))]
    public async Task InsertThenSelect_HostileValues_ExactRoundTrip(string payload)
    {
        await using var harness = await EscapeTableHarness.CreateAsync(() => _fixture.BuildSettings());

        await harness.InsertAsync(1, payload);
        var roundTripped = await harness.ReadValueAsync(1);

        Assert.Equal(payload, roundTripped);
    }
}

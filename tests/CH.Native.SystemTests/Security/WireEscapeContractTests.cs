using CH.Native.Connection;
using CH.Native.Linq;
using CH.Native.SystemTests.Fixtures;
using CH.Native.SystemTests.Security.Helpers;
using Xunit;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// Pins the equivalence between the LINQ-inline literal escape path
/// (<c>ParameterSerializer.EscapeString</c>, single-pass server decode) and the
/// parameter-wire escape path (<c>ParameterSerializer.EscapeStringForParameter</c>,
/// two-pass server decode). Both reach the same server, but via different
/// transports — if either side breaks for a payload the other handles, the
/// gotcha is exactly the kind of double-escape boundary bug Gap 6 calls out.
///
/// <para>For each payload, the test inserts a row with the value via the
/// parameter path, then reads it back two ways:
/// (1) by parameterised <c>WHERE id = @id</c> (parameter path on read),
/// (2) by LINQ <c>Where(r =&gt; r.Value == payload)</c> (LINQ-inline path).
/// All three (input, parameter-read, inline-read) must match byte-for-byte.</para>
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Security)]
public class WireEscapeContractTests
{
    private readonly SingleNodeFixture _fixture;

    public WireEscapeContractTests(SingleNodeFixture fixture)
    {
        _fixture = fixture;
    }

    public static IEnumerable<object[]> Payloads => new[]
    {
        new object[] { "O'Brien'; -- " },
        new object[] { "path\\foo\\\\bar" },
        new object[] { "line1\nline2" },
        new object[] { "before\0after" },
        new object[] { "tab\there" },
        new object[] { "carriage\rreturn" },
        new object[] { "Hello \U0001F30D" },
    };

    [Theory]
    [MemberData(nameof(Payloads))]
    public async Task BothPaths_Payload_ThreeWayEquality(string payload)
    {
        await using var harness = await EscapeTableHarness.CreateAsync(() => _fixture.BuildSettings());

        // Insert via parameter path (the canonical write path).
        await harness.InsertAsync(1, payload);

        // Read 1: parameter path.
        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var paramRead = await conn.ExecuteScalarAsync<string>(
            $"SELECT value FROM {harness.TableName} WHERE id = @id",
            new { id = 1 });

        // Read 2: LINQ-inline literal path. The provider inlines `payload` into
        // the generated SQL via EscapeString (single-pass).
        var linqRows = await conn.Table<SecurityRow>(harness.TableName)
            .Where(r => r.Value == payload)
            .ToListAsync();

        Assert.Equal(payload, paramRead);
        Assert.Single(linqRows);
        Assert.Equal(payload, linqRows[0].Value);

        // Three-way equality: paramRead == linqRead == payload.
        Assert.Equal(paramRead, linqRows[0].Value);
    }

    [Theory]
    [InlineData(0x01)]
    [InlineData(0x07)]
    [InlineData(0x08)] // backspace
    [InlineData(0x0B)] // vertical tab
    [InlineData(0x0C)] // form feed
    [InlineData(0x0E)]
    [InlineData(0x1F)]
    public async Task BothPaths_SingleControlByte_ThreeWayEquality(int byteValue)
    {
        var payload = $"x{(char)byteValue}y";

        await using var harness = await EscapeTableHarness.CreateAsync(() => _fixture.BuildSettings());

        await harness.InsertAsync(1, payload);

        await using var conn = new ClickHouseConnection(_fixture.BuildSettings());
        await conn.OpenAsync();
        var paramRead = await conn.ExecuteScalarAsync<string>(
            $"SELECT value FROM {harness.TableName} WHERE id = @id",
            new { id = 1 });

        var linqRows = await conn.Table<SecurityRow>(harness.TableName)
            .Where(r => r.Value == payload)
            .ToListAsync();

        Assert.Equal(payload, paramRead);
        Assert.Single(linqRows);
        Assert.Equal(payload, linqRows[0].Value);
    }
}

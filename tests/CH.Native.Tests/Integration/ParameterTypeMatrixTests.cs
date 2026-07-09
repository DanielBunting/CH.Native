using System.Net;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Systematic <c>{v:Type}</c> parameter round-trip over a matrix of scalar and array types (ported from
/// the driver's parameterized-select type matrix). Each case binds a CLR value with an explicit
/// ClickHouse type and asserts the server sees it equal to the equivalent server-side literal — a
/// round-trip that exercises the full <see cref="Parameters.ParameterSerializer"/> path per type.
/// </summary>
[Collection("ClickHouse")]
public class ParameterTypeMatrixTests
{
    private readonly ClickHouseFixture _fixture;

    public ParameterTypeMatrixTests(ClickHouseFixture fixture) => _fixture = fixture;

    private static readonly Guid Uuid = new("11111111-2222-3333-4444-555555555555");

    public static IEnumerable<object[]> Scalars() => new[]
    {
        new object[] { "Int8", (sbyte)-5, "-5" },
        new object[] { "Int16", (short)-1234, "-1234" },
        new object[] { "Int32", 42, "42" },
        new object[] { "Int64", 9876543210L, "9876543210" },
        new object[] { "UInt8", (byte)200, "200" },
        new object[] { "UInt16", (ushort)60000, "60000" },
        new object[] { "UInt32", 4000000000u, "4000000000" },
        new object[] { "UInt64", 18000000000000000000UL, "18000000000000000000" },
        new object[] { "Float32", 0.5f, "toFloat32(0.5)" },
        new object[] { "Float64", 3.5d, "3.5" },
        // literal is quoted so the exact scale-4 value is parsed (an unquoted float literal is imprecise)
        new object[] { "Decimal64(4)", 12.3456m, "toDecimal64('12.3456', 4)" },
        new object[] { "String", "hello", "'hello'" },
        new object[] { "FixedString(5)", "abcde", "'abcde'" },
        new object[] { "UUID", Uuid, $"toUUID('{Uuid}')" },
        new object[] { "Date", new DateOnly(2021, 1, 2), "toDate('2021-01-02')" },
        new object[] { "DateTime", new DateTime(2021, 1, 2, 3, 4, 5), "toDateTime('2021-01-02 03:04:05')" },
        new object[] { "DateTime64(3)", new DateTime(2021, 1, 2, 3, 4, 5, 678), "toDateTime64('2021-01-02 03:04:05.678', 3)" },
        new object[] { "Bool", true, "true" },
        new object[] { "IPv4", IPAddress.Parse("192.168.1.1"), "toIPv4('192.168.1.1')" },
        new object[] { "IPv6", IPAddress.Parse("2001:db8::1"), "toIPv6('2001:db8::1')" },
    };

    [Theory]
    [MemberData(nameof(Scalars))]
    public async Task ScalarParameter_RoundTripsEqualToLiteral(string chType, object value, string sqlLiteral)
        => await AssertEchoesAsync(chType, value, sqlLiteral);

    public static IEnumerable<object[]> Arrays() => new[]
    {
        new object[] { "Array(Int32)", new[] { 1, 2, 3 }, "[1, 2, 3]" },
        new object[] { "Array(String)", new[] { "a", "b" }, "['a', 'b']" },
        new object[] { "Array(UUID)", new[] { Uuid }, $"[toUUID('{Uuid}')]" },
        new object[] { "Array(Float64)", new[] { 1.5, 2.5 }, "[1.5, 2.5]" },
        new object[] { "Array(DateTime)", new[] { new DateTime(2021, 1, 2, 3, 4, 5) }, "[toDateTime('2021-01-02 03:04:05')]" },
        new object[] { "Array(FixedString(4))", new[] { "abcd", "wxyz" }, "[toFixedString('abcd', 4), toFixedString('wxyz', 4)]" },
        new object[] { "Array(LowCardinality(String))", new[] { "x", "y" }, "['x', 'y']" },
        new object[] { "Array(Decimal64(2))", new[] { 1.25m, 2.50m }, "[toDecimal64(1.25, 2), toDecimal64(2.50, 2)]" },
    };

    [Theory]
    [MemberData(nameof(Arrays))]
    public async Task ArrayParameter_RoundTripsEqualToLiteral(string chType, object value, string sqlLiteral)
        => await AssertEchoesAsync(chType, value, sqlLiteral);

    private async Task AssertEchoesAsync(string chType, object value, string sqlLiteral)
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // `=` yields UInt8 (0/1); read it as a byte so no bool-mapping assumption is needed.
        await using var command = connection.CreateCommand($"SELECT ({{v:{chType}}} = {sqlLiteral}) ? 1 : 0");
        command.Parameters.Add("v", value, chType);

        var equal = await command.ExecuteScalarAsync<byte>();
        Assert.Equal((byte)1, equal);
    }
}

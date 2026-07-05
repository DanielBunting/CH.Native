using System.Net;
using System.Runtime.CompilerServices;
using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Directly exercises the ADO typed getters that exist on the reader but were previously only
/// reached via <c>GetFieldValue&lt;T&gt;</c> (ported from the HTTP driver's DataReaderTests).
/// Unsigned / IP / Tuple have no dedicated getter, so those go through <c>GetFieldValue&lt;T&gt;</c>.
/// </summary>
[Collection("ClickHouse")]
public class DataReaderTypedGetterTests
{
    private readonly ClickHouseFixture _fixture;

    public DataReaderTypedGetterTests(ClickHouseFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task TypedGetters_ReturnCorrectValues()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT toFloat32(1.5) AS f, toInt16(-42) AS s, toDecimal64(12.34, 2) AS d, " +
            "toUInt8(200) AS b, toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS g");

        Assert.True(await reader.ReadAsync());

        Assert.Equal(1.5f, reader.GetFloat(0));
        Assert.Equal((short)-42, reader.GetInt16(1));
        Assert.Equal(12.34m, reader.GetDecimal(2));
        Assert.Equal((byte)200, reader.GetByte(3));
        Assert.Equal(Guid.Parse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), reader.GetGuid(4));
    }

    [Fact]
    public async Task Unsigned_Ip_Tuple_ThroughGetFieldValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync(
            "SELECT toUInt32(4000000000) AS u, toIPv4('1.2.3.4') AS ip, tuple(1, 'a') AS t");

        Assert.True(await reader.ReadAsync());

        Assert.Equal(4000000000u, reader.GetFieldValue<uint>(0));
        Assert.Equal(IPAddress.Parse("1.2.3.4"), reader.GetFieldValue<IPAddress>(1));

        var tuple = reader.GetFieldValue<ITuple>(2);
        Assert.Equal(2, tuple.Length);
    }

    [Fact]
    public async Task GetBytes_ThrowsNotSupported_PointingAtGetFieldValue()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await using var reader = await connection.ExecuteReaderAsync("SELECT 'abc' AS s");
        Assert.True(await reader.ReadAsync());

        Assert.Throws<NotSupportedException>(() => reader.GetBytes(0, 0, new byte[8], 0, 8));
    }
}

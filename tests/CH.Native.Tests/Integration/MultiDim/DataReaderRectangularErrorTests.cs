using CH.Native.Connection;
using CH.Native.Exceptions;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration.MultiDim;

/// <summary>
/// Error-contract coverage for reading a jagged column as a rectangular <c>int[,]</c> via
/// <c>GetFieldValue&lt;T[,]&gt;</c> (ported from the driver's GetFieldValueMultidim_* failure cases).
/// These pin CH.Native's <b>current behavior</b>, which diverges from the driver's DbDataReader
/// contract: ragged data throws <see cref="ClickHouseTypeConversionException"/> (driver:
/// InvalidOperationException); a null value <b>returns null</b> (driver: InvalidCastException); a
/// scalar / wrong-leaf throws <see cref="InvalidCastException"/> but without the driver's ordinal /
/// target-type message text.
/// </summary>
[Collection("ClickHouse")]
public class DataReaderRectangularErrorTests
{
    private readonly ClickHouseFixture _fixture;

    public DataReaderRectangularErrorTests(ClickHouseFixture fixture) => _fixture = fixture;

    private async Task<CH.Native.Results.ClickHouseDataReader> OpenSingleRowAsync(
        ClickHouseConnection connection, string selectExpr)
    {
        var reader = await connection.ExecuteReaderAsync($"SELECT {selectExpr} AS v");
        Assert.True(await reader.ReadAsync());
        return reader;
    }

    [Fact]
    public async Task RaggedRows_ThrowsConversionException()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var reader = await OpenSingleRowAsync(connection, "[[1, 2, 3], [4, 5]]::Array(Array(Int32))");

        var ex = Assert.Throws<ClickHouseTypeConversionException>(() => reader.GetFieldValue<int[,]>(0));
        Assert.Contains("rectangular", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ScalarValue_ThrowsInvalidCast()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var reader = await OpenSingleRowAsync(connection, "toInt32(42)");

        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int[,]>(0));
    }

    [Fact]
    public async Task WrongLeafType_ThrowsInvalidCast()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var reader = await OpenSingleRowAsync(connection, "[['a', 'b'], ['c', 'd']]::Array(Array(String))");

        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<int[,]>(0));
    }

    [Fact]
    public async Task NullValue_ReturnsNull_Divergence()
    {
        // Divergence: the driver throws InvalidCastException per the DbDataReader contract;
        // CH.Native returns default(int[,]) == null for a SQL-null read as a reference-type array.
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var reader = await OpenSingleRowAsync(connection, "CAST(NULL AS Nullable(Int32))");

        Assert.Null(reader.GetFieldValue<int[,]>(0));
    }
}

using System.Data;
using CH.Native.Ado;
using Xunit;
using F = CH.Native.Tests.Unit.Ado.FakeClickHouseDataReader;

namespace CH.Native.Tests.Unit.Ado;

/// <summary>
/// Contract surface for <see cref="ClickHouseDbDataReader"/> using the
/// <see cref="FakeClickHouseDataReader"/> test double. Pins ADO.NET behaviours
/// that consumers (Dapper, EF Core, hand-rolled DbDataReader code) depend on.
/// </summary>
public class ClickHouseDbDataReaderContractTests
{
    private static F BuildReader(params F.ColumnDef[] columns) =>
        new(columns, rows: Array.Empty<object?[]>());

    private static F BuildReaderWithRows(F.ColumnDef[] columns, params object?[][] rows) =>
        new(columns, rows);

    private static readonly F.ColumnDef IntCol = new("id", typeof(int), "Int32", IsNullable: false);
    private static readonly F.ColumnDef NullableIntCol = new("maybe", typeof(int?), "Nullable(Int32)", IsNullable: true);
    private static readonly F.ColumnDef StringCol = new("name", typeof(string), "String", IsNullable: false);

    [Fact]
    public void IsClosed_NewReader_IsFalse()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);
        Assert.False(sut.IsClosed);
    }

    [Fact]
    public void Close_DisposesInnerAndFlipsIsClosed()
    {
        var inner = BuildReader(IntCol);
        var sut = new ClickHouseDbDataReader(inner);

        sut.Close();

        Assert.True(sut.IsClosed);
        Assert.True(inner.Disposed);
    }

    [Fact]
    public void Close_Idempotent()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        sut.Close();
        sut.Close();  // second call is a no-op

        Assert.True(sut.IsClosed);
    }

    [Fact]
    public async Task CloseAsync_DisposesInnerAndFlipsIsClosed()
    {
        var inner = BuildReader(IntCol);
        await using var sut = new ClickHouseDbDataReader(inner);

        await sut.CloseAsync();

        Assert.True(sut.IsClosed);
        Assert.True(inner.Disposed);
    }

    [Fact]
    public void Read_AfterClose_ThrowsObjectDisposed()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);
        sut.Close();

        Assert.Throws<ObjectDisposedException>(() => sut.Read());
    }

    [Fact]
    public async Task ReadAsync_AfterClose_ThrowsObjectDisposed()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);
        sut.Close();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await sut.ReadAsync(CancellationToken.None));
    }

    [Fact]
    public void GetValue_AfterClose_ThrowsObjectDisposed()
    {
        using var inner = BuildReaderWithRows(new[] { IntCol }, new object?[] { 42 });
        using var sut = new ClickHouseDbDataReader(inner);
        sut.Read();
        sut.Close();

        Assert.Throws<ObjectDisposedException>(() => sut.GetValue(0));
    }

    [Fact]
    public void NextResult_AlwaysReturnsFalse()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.False(sut.NextResult());
    }

    [Fact]
    public async Task NextResultAsync_AlwaysReturnsFalse()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.False(await sut.NextResultAsync(CancellationToken.None));
    }

    [Fact]
    public void Depth_IsZero()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);
        Assert.Equal(0, sut.Depth);
    }

    [Fact]
    public void RecordsAffected_IsNegativeOne()
    {
        // ClickHouse doesn't return affected-row counts via the native protocol —
        // standard ADO.NET behaviour is -1 for "unknown".
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);
        Assert.Equal(-1, sut.RecordsAffected);
    }

    [Fact]
    public void GetBytes_Throws_NotSupportedWithGuidance()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        var ex = Assert.Throws<NotSupportedException>(() =>
            sut.GetBytes(0, 0, new byte[8], 0, 8));
        Assert.Contains("GetFieldValue<byte[]>", ex.Message);
    }

    [Fact]
    public void GetChars_Throws_NotSupportedWithGuidance()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        var ex = Assert.Throws<NotSupportedException>(() =>
            sut.GetChars(0, 0, new char[8], 0, 8));
        Assert.Contains("GetString", ex.Message);
    }

    [Fact]
    public void GetOrdinal_FoundColumn_ReturnsIndex()
    {
        using var inner = BuildReader(IntCol, StringCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Equal(0, sut.GetOrdinal("id"));
        Assert.Equal(1, sut.GetOrdinal("name"));
    }

    [Fact]
    public void GetOrdinal_CaseInsensitive()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Equal(0, sut.GetOrdinal("ID"));
        Assert.Equal(0, sut.GetOrdinal("Id"));
    }

    [Fact]
    public void GetOrdinal_MissingColumn_ThrowsArgument()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Throws<ArgumentException>(() => sut.GetOrdinal("missing"));
    }

    [Fact]
    public void GetName_ReturnsColumnName()
    {
        using var inner = BuildReader(IntCol, StringCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Equal("id", sut.GetName(0));
        Assert.Equal("name", sut.GetName(1));
    }

    [Fact]
    public void GetName_OutOfRange_Throws()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Throws<IndexOutOfRangeException>(() => sut.GetName(99));
    }

    [Fact]
    public void GetDataTypeName_ReturnsClickHouseType()
    {
        using var inner = BuildReader(IntCol, StringCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Equal("Int32", sut.GetDataTypeName(0));
        Assert.Equal("String", sut.GetDataTypeName(1));
    }

    [Fact]
    public void GetFieldType_ReturnsClrType()
    {
        using var inner = BuildReader(IntCol, StringCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Equal(typeof(int), sut.GetFieldType(0));
        Assert.Equal(typeof(string), sut.GetFieldType(1));
    }

    [Fact]
    public void FieldCount_AfterEnsureInitialized_MatchesInner()
    {
        using var inner = BuildReader(IntCol, StringCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Equal(2, sut.FieldCount);
    }

    [Fact]
    public void HasRows_True_WhenFirstRowAvailable()
    {
        using var inner = BuildReaderWithRows(new[] { IntCol }, new object?[] { 7 });
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.True(sut.HasRows);
    }

    [Fact]
    public void HasRows_False_OnEmptyResult()
    {
        using var inner = BuildReader(IntCol);
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.False(sut.HasRows);
    }

    [Fact]
    public void Read_FirstCall_ReturnsTrue_WhenRowsExist()
    {
        using var inner = BuildReaderWithRows(new[] { IntCol }, new object?[] { 1 }, new object?[] { 2 });
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.True(sut.Read());
        Assert.Equal(1, sut.GetInt32(0));
        Assert.True(sut.Read());
        Assert.Equal(2, sut.GetInt32(0));
        Assert.False(sut.Read());
    }

    [Fact]
    public void GetSchemaTable_Shape_OneRowPerColumn()
    {
        using var inner = BuildReader(IntCol, NullableIntCol, StringCol);
        using var sut = new ClickHouseDbDataReader(inner);

        var schema = sut.GetSchemaTable();

        Assert.NotNull(schema);
        Assert.Equal("SchemaTable", schema!.TableName);
        Assert.Equal(3, schema.Rows.Count);
        Assert.Equal("id", schema.Rows[0]["ColumnName"]);
        Assert.Equal(0, schema.Rows[0]["ColumnOrdinal"]);
        Assert.Equal(typeof(int), schema.Rows[0]["DataType"]);
        Assert.Equal("Int32", schema.Rows[0]["ProviderType"]);
        Assert.Equal(false, schema.Rows[0]["AllowDBNull"]);

        // The Nullable(Int32) column has AllowDBNull=true, derived from the
        // ClickHouse type-name prefix.
        Assert.Equal("maybe", schema.Rows[1]["ColumnName"]);
        Assert.Equal("Nullable(Int32)", schema.Rows[1]["ProviderType"]);
        Assert.Equal(true, schema.Rows[1]["AllowDBNull"]);
    }

    [Fact]
    public void Indexer_ByOrdinal_CallsGetValue()
    {
        using var inner = BuildReaderWithRows(new[] { IntCol }, new object?[] { 42 });
        using var sut = new ClickHouseDbDataReader(inner);

        sut.Read();
        Assert.Equal(42, sut[0]);
    }

    [Fact]
    public void Indexer_ByName_LooksUpThroughGetOrdinal()
    {
        using var inner = BuildReaderWithRows(new[] { IntCol, StringCol },
            new object?[] { 7, "hello" });
        using var sut = new ClickHouseDbDataReader(inner);

        sut.Read();
        Assert.Equal("hello", sut["name"]);
    }

    [Fact]
    public void GetValue_NullValue_ReturnsDBNull()
    {
        // GetValue surfaces null as DBNull.Value to match ADO.NET conventions.
        using var inner = BuildReaderWithRows(new[] { NullableIntCol }, new object?[] { null });
        using var sut = new ClickHouseDbDataReader(inner);

        sut.Read();
        Assert.Equal(DBNull.Value, sut.GetValue(0));
    }

    [Fact]
    public void IsDBNull_True_ForNull_False_ForValue()
    {
        using var inner = BuildReaderWithRows(new[] { NullableIntCol, IntCol },
            new object?[] { null, 7 });
        using var sut = new ClickHouseDbDataReader(inner);

        sut.Read();
        Assert.True(sut.IsDBNull(0));
        Assert.False(sut.IsDBNull(1));
    }

    [Fact]
    public void GetValues_FillsArrayUpToCount()
    {
        using var inner = BuildReaderWithRows(new[] { IntCol, StringCol },
            new object?[] { 7, "hi" });
        using var sut = new ClickHouseDbDataReader(inner);

        sut.Read();
        var buffer = new object[5];
        var count = sut.GetValues(buffer);

        Assert.Equal(2, count);
        Assert.Equal(7, buffer[0]);
        Assert.Equal("hi", buffer[1]);
    }

    [Fact]
    public void Dispose_DisposesTimeoutCts()
    {
        // The reader takes optional ownership of a CommandTimeout CTS — verify
        // Dispose disposes it. Use a CTS we can observe via IsCancellationRequested
        // after Dispose on the underlying source (CTS doesn't track Disposed
        // directly, but we can confirm Dispose doesn't throw and Cancel after
        // Dispose throws ObjectDisposedException).
        var cts = new CancellationTokenSource();
        var inner = BuildReader(IntCol);
        var sut = new ClickHouseDbDataReader(inner, cts);

        sut.Dispose();

        Assert.Throws<ObjectDisposedException>(() => cts.Cancel());
    }

    [Fact]
    public async Task DisposeAsync_DisposesTimeoutCts()
    {
        var cts = new CancellationTokenSource();
        var inner = BuildReader(IntCol);
        var sut = new ClickHouseDbDataReader(inner, cts);

        await sut.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => cts.Cancel());
    }

    [Fact]
    public void QueryId_ProxiesInner()
    {
        var inner = new F(new[] { IntCol }, queryId: "abc-123");
        using var sut = new ClickHouseDbDataReader(inner);

        Assert.Equal("abc-123", sut.QueryId);
    }
}

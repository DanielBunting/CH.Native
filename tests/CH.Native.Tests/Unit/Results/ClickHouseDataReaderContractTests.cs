using System.Buffers;
using CH.Native.Data;
using CH.Native.Protocol.Messages;
using CH.Native.Results;
using Xunit;

namespace CH.Native.Tests.Unit.Results;

/// <summary>
/// Fast (no-Docker) ADO.NET contract surface for <see cref="ClickHouseDataReader"/>.
/// </summary>
/// <remarks>
/// When the standalone <c>ClickHouseDbDataReader</c> wrapper was folded into
/// <see cref="ClickHouseDataReader"/> (now a direct <see cref="System.Data.Common.DbDataReader"/>),
/// its in-memory contract tests were dropped and the equivalent assertions only
/// survived as Docker-dependent integration tests — so a <c>--filter Category!=Integration</c>
/// run no longer exercised the metadata surface (<c>GetName</c>, <c>GetOrdinal</c>,
/// <c>GetDataTypeName</c>, <c>GetFieldType</c>), the deliberate <c>GetBytes</c>/<c>GetChars</c>
/// <see cref="NotSupportedException"/> guidance, or timeout-CTS disposal. These pin that
/// surface again without a server by feeding the reader an in-memory message stream.
/// </remarks>
public class ClickHouseDataReaderContractTests
{
    // Pool that hands back fresh arrays and ignores returns, so TypedColumn.Dispose
    // never trips ArrayPool.Shared's foreign-buffer guard.
    private sealed class NoReturnPool<T> : ArrayPool<T>
    {
        public static readonly NoReturnPool<T> Instance = new();
        public override T[] Rent(int minimumLength) => new T[minimumLength];
        public override void Return(T[] array, bool clearArray = false) { }
    }

    private static async IAsyncEnumerable<object> Enumerate(object[] messages)
    {
        foreach (var m in messages)
            yield return m;
        await Task.CompletedTask;
    }

    /// <summary>
    /// Two-column result (<c>id Int32</c>, <c>name String</c>) with two rows, then
    /// end-of-stream — the minimum needed to drive the schema/metadata surface.
    /// </summary>
    private static ClickHouseDataReader CreateReader()
    {
        var idCol = new TypedColumn<int>(new[] { 1, 2 }, length: 2, pool: NoReturnPool<int>.Instance);
        var nameCol = new TypedColumn<string?>(new[] { "alice", "bob" }, length: 2, pool: NoReturnPool<string?>.Instance);

        var block = new TypedBlock
        {
            TableName = "",
            ColumnNames = new[] { "id", "name" },
            ColumnTypes = new[] { "Int32", "String" },
            Columns = new ITypedColumn[] { idCol, nameCol },
        };

        return new ClickHouseDataReader(
            Enumerate(new object[] { new DataMessage { Block = block }, EndOfStreamMessage.Instance })
                .GetAsyncEnumerator());
    }

    // ---- column metadata -------------------------------------------------

    [Fact]
    public void GetName_ReturnsColumnName()
    {
        using var sut = CreateReader();
        Assert.Equal("id", sut.GetName(0));
        Assert.Equal("name", sut.GetName(1));
    }

    [Fact]
    public void GetOrdinal_FindsColumn()
    {
        using var sut = CreateReader();
        Assert.Equal(0, sut.GetOrdinal("id"));
        Assert.Equal(1, sut.GetOrdinal("name"));
    }

    [Fact]
    public void GetOrdinal_IsCaseInsensitive()
    {
        using var sut = CreateReader();
        Assert.Equal(1, sut.GetOrdinal("NAME"));
    }

    [Fact]
    public void GetOrdinal_MissingColumn_ThrowsArgumentException()
    {
        using var sut = CreateReader();
        var ex = Assert.Throws<ArgumentException>(() => sut.GetOrdinal("nope"));
        Assert.Contains("nope", ex.Message);
    }

    [Fact]
    public void GetDataTypeName_ReturnsClickHouseTypeName()
    {
        using var sut = CreateReader();
        Assert.Equal("Int32", sut.GetDataTypeName(0));
        Assert.Equal("String", sut.GetDataTypeName(1));
    }

    [Fact]
    public void GetFieldType_ReturnsClrType()
    {
        using var sut = CreateReader();
        Assert.Equal(typeof(int), sut.GetFieldType(0));
        Assert.Equal(typeof(string), sut.GetFieldType(1));
    }

    [Fact]
    public void FieldCount_ReflectsBlock()
    {
        using var sut = CreateReader();
        Assert.Equal(2, sut.FieldCount);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(2)]
    [InlineData(99)]
    public void GetName_OrdinalOutOfRange_Throws(int ordinal)
    {
        using var sut = CreateReader();
        Assert.Throws<ArgumentOutOfRangeException>(() => sut.GetName(ordinal));
    }

    // ---- unsupported bulk accessors -------------------------------------

    [Fact]
    public void GetBytes_Throws_WithGuidance()
    {
        using var sut = CreateReader();
        var ex = Assert.Throws<NotSupportedException>(
            () => sut.GetBytes(0, 0, new byte[8], 0, 8));
        Assert.Contains("GetFieldValue<byte[]>", ex.Message);
    }

    [Fact]
    public void GetChars_Throws_WithGuidance()
    {
        using var sut = CreateReader();
        var ex = Assert.Throws<NotSupportedException>(
            () => sut.GetChars(0, 0, new char[8], 0, 8));
        Assert.Contains("GetString", ex.Message);
    }

    // ---- single-result-set contract -------------------------------------

    [Fact]
    public void NextResult_AlwaysFalse()
    {
        using var sut = CreateReader();
        Assert.False(sut.NextResult());
    }

    [Fact]
    public async Task NextResultAsync_AlwaysFalse()
    {
        await using var sut = CreateReader();
        Assert.False(await sut.NextResultAsync(CancellationToken.None));
    }

    [Fact]
    public void Depth_IsZero()
    {
        using var sut = CreateReader();
        Assert.Equal(0, sut.Depth);
    }

    [Fact]
    public void RecordsAffected_IsNegativeOne()
    {
        using var sut = CreateReader();
        Assert.Equal(-1, sut.RecordsAffected);
    }

    // ---- lifecycle -------------------------------------------------------

    [Fact]
    public async Task IsClosed_FlipsAfterDispose()
    {
        var sut = CreateReader();
        Assert.False(sut.IsClosed);
        await sut.DisposeAsync();
        Assert.True(sut.IsClosed);
    }

    [Fact]
    public async Task Dispose_DisposesAttachedTimeoutCts()
    {
        // ClickHouseCommand.ExecuteDbDataReaderAsync attaches a per-command timeout CTS
        // via AttachAdoLifetime; the reader owns it and must dispose it on teardown so
        // the timer doesn't leak. Accessing Token on a disposed CTS throws.
        var cts = new CancellationTokenSource();
        var sut = CreateReader();
        sut.AttachAdoLifetime(cts, connectionToClose: null);

        await sut.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => _ = cts.Token);
    }
}

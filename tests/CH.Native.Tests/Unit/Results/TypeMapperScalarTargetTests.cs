using System.Buffers;
using CH.Native.Data;
using CH.Native.Protocol.Messages;
using CH.Native.Results;
using Xunit;

namespace CH.Native.Tests.Unit.Results;

/// <summary>
/// Covers the scalar branch of <see cref="TypeMapper{T}"/> — the branch that binds
/// T to ordinal 0 instead of assembling a POCO from columns.
/// </summary>
/// <remarks>
/// The branch exists because without it a scalar T contributes zero bindings and
/// every row silently maps to <c>default(T)</c>, the decoded value discarded. The
/// primitive half of the branch (typed getter) is well covered by the query tests;
/// the ENUM half is not, and it is the half that can't use the typed getter — an
/// enum has to route through the boxed converter because ClickHouse hands the value
/// over as either its underlying integer (<c>Enum8</c>) or its name (<c>String</c>),
/// and neither is the enum type itself. These pin that route without a server.
/// </remarks>
public class TypeMapperScalarTargetTests
{
    private enum Status : sbyte
    {
        Unknown = 0,
        Active = 1,
        Retired = 2,
    }

    // Hands back fresh arrays and ignores returns, so TypedColumn.Dispose never
    // trips ArrayPool.Shared's foreign-buffer guard.
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

    private static ClickHouseDataReader SingleColumnReader<TCol>(
        string columnName, string columnType, TCol[] values)
    {
        var block = new TypedBlock
        {
            TableName = "",
            ColumnNames = new[] { columnName },
            ColumnTypes = new[] { columnType },
            Columns = new ITypedColumn[]
            {
                new TypedColumn<TCol>(values, length: values.Length, pool: NoReturnPool<TCol>.Instance),
            },
        };

        return new ClickHouseDataReader(
            Enumerate(new object[] { new DataMessage { Block = block }, EndOfStreamMessage.Instance })
                .GetAsyncEnumerator());
    }

    [Fact]
    public async Task EnumScalar_FromUnderlyingInteger_Converts()
    {
        // Enum8 arrives as sbyte. A typed getter would throw on the cast; the
        // scalar map must route through the converter instead.
        await using var reader = SingleColumnReader("s", "Enum8('Unknown' = 0, 'Active' = 1, 'Retired' = 2)",
            new sbyte[] { 2, 1 });

        Assert.True(await reader.ReadAsync());
        var mapper = new TypeMapper<Status>(reader);

        Assert.Equal(Status.Retired, mapper.Map(reader));
        Assert.True(await reader.ReadAsync());
        Assert.Equal(Status.Active, mapper.Map(reader));
    }

    [Fact]
    public async Task EnumScalar_FromStringName_Converts()
    {
        // The other representation: an enum projected as its name.
        await using var reader = SingleColumnReader("s", "String", new string?[] { "Retired" });

        Assert.True(await reader.ReadAsync());
        var mapper = new TypeMapper<Status>(reader);

        Assert.Equal(Status.Retired, mapper.Map(reader));
    }

    [Fact]
    public async Task NullableEnumScalar_IsRecognisedAsScalar()
    {
        // IsScalarTarget unwraps Nullable before the enum check — without that,
        // Status? falls through to the POCO path and maps to null for every row.
        await using var reader = SingleColumnReader("s", "Enum8('Active' = 1)", new sbyte[] { 1 });

        Assert.True(await reader.ReadAsync());
        var mapper = new TypeMapper<Status?>(reader);

        Assert.Equal(Status.Active, mapper.Map(reader));
    }

    [Fact]
    public void ScalarAgainstColumnlessResult_ThrowsInsteadOfBindingNothing()
    {
        // The scalar branch binds ordinal 0, so a result set with no columns has
        // nothing to bind. Failing loudly at mapper-construction beats the
        // alternative the branch was added to prevent: silently handing back
        // default(T) for every row.
        var reader = new ClickHouseDataReader(
            Enumerate(new object[] { EndOfStreamMessage.Instance }).GetAsyncEnumerator());

        var ex = Assert.Throws<InvalidOperationException>(() => new TypeMapper<int>(reader));

        Assert.Contains("Int32", ex.Message);
    }

    [Fact]
    public async Task PrimitiveScalar_BindsOrdinalZero()
    {
        // The regression the scalar branch was added for: before it, this returned
        // default(long) for every row rather than the decoded value.
        await using var reader = SingleColumnReader("n", "Int64", new long[] { 77 });

        Assert.True(await reader.ReadAsync());
        var mapper = new TypeMapper<long>(reader);

        Assert.Equal(77L, mapper.Map(reader));
    }
}

using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

/// <summary>
/// Pins that the row-streaming APIs return actual column values when <c>T</c> is a
/// scalar (<c>int</c>, <c>ulong</c>, <c>string</c>, <c>Guid</c>, …) rather than a POCO.
///
/// <para>
/// Both streaming APIs map rows through a mapper that binds <em>writable properties</em>
/// to columns. A scalar has none, so the mapper produces no bindings and every row
/// falls back to a freshly-constructed <c>T</c> — i.e. <c>default(T)</c>. The values
/// decoded off the wire are simply never read. Two independent mappers share the
/// shape: <c>TypeMapper&lt;T&gt;</c> behind <c>QueryStreamAsync</c>, and
/// <c>ReflectionTypedRowMapper&lt;T&gt;</c> behind <c>QueryTypedAsync</c>.
/// </para>
///
/// <para>
/// Every assertion here uses a <b>non-zero sentinel</b> (values offset by 7, a fixed
/// UUID, a real timestamp). That is deliberate: <c>SELECT number FROM numbers(3)</c>
/// starts at 0, so a broken mapper returning <c>default(T)</c> produces
/// <c>[0,0,0]</c> — indistinguishable from a correct read. Any test written against
/// a zero-based sequence would pass while the mapping was completely broken, which
/// is how this went unnoticed: every pre-existing call site in the suite consumes
/// these APIs as <c>await foreach (var _ in …)</c> and asserts only row counts.
/// </para>
///
/// <para>
/// The <c>Control_</c> tests establish that the wire and decode paths are sound, so
/// a failure in the rest of the file localises to row mapping and nothing else.
/// </para>
/// </summary>
[Collection("ClickHouse")]
public class ScalarRowMappingTests
{
    private const string IntSql = "SELECT toInt32(number + 7) AS v FROM numbers(3)";
    private const string UInt64Sql = "SELECT number + 7 AS v FROM numbers(3)";
    private static readonly int[] ExpectedInts = { 7, 8, 9 };

    private readonly ClickHouseFixture _fixture;

    public ScalarRowMappingTests(ClickHouseFixture fixture) => _fixture = fixture;

    private async Task<ClickHouseConnection> OpenAsync()
    {
        var conn = new ClickHouseConnection(_fixture.ConnectionString);
        await conn.OpenAsync();
        return conn;
    }

    /// <summary>POCO with a writable property — the shape both mappers are built for.</summary>
    public class Row
    {
        public int v { get; set; }
    }

    // ---------------------------------------------------------------- controls

    [Fact]
    public async Task Control_ExecuteScalar_ReturnsValue()
    {
        await using var conn = await OpenAsync();
        Assert.Equal(4242, await conn.ExecuteScalarAsync<int>("SELECT 4242"));
    }

    [Fact]
    public async Task Control_ExecuteReader_TypedGetter_ReturnsValues()
    {
        // Same SQL the scalar-stream tests use. This is the proof that the bytes
        // on the wire are correct and correctly decoded: if this passes and the
        // scalar-stream tests fail, only the row mapper can be at fault.
        await using var conn = await OpenAsync();

        var values = new List<int>();
        await using (var reader = await conn.ExecuteReaderAsync(IntSql))
        {
            while (await reader.ReadAsync())
                values.Add(reader.GetInt32(0));
        }

        Assert.Equal(ExpectedInts, values);
    }

    [Fact]
    public async Task Control_QueryStream_Poco_ReturnsValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<int>();
        await foreach (var row in conn.QueryStreamAsync<Row>(IntSql))
            values.Add(row.v);

        Assert.Equal(ExpectedInts, values);
    }

    [Fact]
    public async Task Control_QueryTyped_Poco_ReturnsValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<int>();
        await foreach (var row in conn.QueryTypedAsync<Row>(IntSql))
            values.Add(row.v);

        Assert.Equal(ExpectedInts, values);
    }

    // ------------------------------------------------- QueryStreamAsync scalars

    [Fact]
    public async Task QueryStream_ScalarInt_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<int>();
        await foreach (var v in conn.QueryStreamAsync<int>(IntSql))
            values.Add(v);

        Assert.Equal(ExpectedInts, values);
    }

    [Fact]
    public async Task QueryStream_ScalarUInt64_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<ulong>();
        await foreach (var v in conn.QueryStreamAsync<ulong>(UInt64Sql))
            values.Add(v);

        Assert.Equal(new ulong[] { 7, 8, 9 }, values);
    }

    [Fact]
    public async Task QueryStream_ScalarDouble_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<double>();
        await foreach (var v in conn.QueryStreamAsync<double>(
            "SELECT toFloat64(number) + 0.5 AS v FROM numbers(3)"))
            values.Add(v);

        Assert.Equal(new[] { 0.5, 1.5, 2.5 }, values);
    }

    [Fact]
    public async Task QueryStream_ScalarString_ReturnsColumnValues()
    {
        // Distinct failure mode from the value types: string is a reference type
        // with no parameterless constructor, so the mapper takes its args-ctor
        // branch and tries to bind String's own ctor parameter ('value') to a
        // column of that name. It throws instead of silently zeroing — loud, but
        // still no value ever reaches the caller.
        await using var conn = await OpenAsync();

        var values = new List<string>();
        await foreach (var v in conn.QueryStreamAsync<string>(
            "SELECT concat('row-', toString(number + 7)) AS v FROM numbers(3)"))
            values.Add(v);

        Assert.Equal(new[] { "row-7", "row-8", "row-9" }, values);
    }

    [Fact]
    public async Task QueryStream_ScalarGuid_ReturnsColumnValues()
    {
        // A fixed UUID, so default(Guid) (all-zero) can never be mistaken for it.
        await using var conn = await OpenAsync();
        const string uuid = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";

        var values = new List<Guid>();
        await foreach (var v in conn.QueryStreamAsync<Guid>($"SELECT toUUID('{uuid}') AS v"))
            values.Add(v);

        Assert.Equal(new[] { Guid.Parse(uuid) }, values);
    }

    [Fact]
    public async Task QueryStream_ScalarDateTime_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<DateTime>();
        await foreach (var v in conn.QueryStreamAsync<DateTime>(
            "SELECT toDateTime('2024-03-05 06:07:08') AS v"))
            values.Add(v);

        Assert.Equal(new[] { new DateTime(2024, 3, 5, 6, 7, 8) }, values);
    }

    [Fact]
    public async Task QueryStream_ScalarNullableInt_ReturnsValuesAndNulls()
    {
        // Nullable<int> must round-trip BOTH a real value and a SQL NULL —
        // default(int?) is null, so a broken mapper is indistinguishable from a
        // correct one on the null rows alone. The non-null row is what discriminates.
        await using var conn = await OpenAsync();

        var values = new List<int?>();
        await foreach (var v in conn.QueryStreamAsync<int?>(
            "SELECT if(number = 1, NULL, toInt32(number + 7)) AS v FROM numbers(3)"))
            values.Add(v);

        Assert.Equal(new int?[] { 7, null, 9 }, values);
    }

    [Fact]
    public async Task QueryStream_ScalarWithMultipleColumns_TakesFirstColumn()
    {
        // Pins the semantic for an over-wide result set: a scalar T binds to
        // ordinal 0, matching ExecuteScalarAsync. Without this the "which column?"
        // question is left to whatever the implementation happens to do.
        await using var conn = await OpenAsync();

        var values = new List<int>();
        await foreach (var v in conn.QueryStreamAsync<int>(
            "SELECT toInt32(number + 7) AS a, toInt32(number + 100) AS b FROM numbers(3)"))
            values.Add(v);

        Assert.Equal(ExpectedInts, values);
    }

    // -------------------------------------------------- QueryTypedAsync scalars

    [Fact]
    public async Task QueryTyped_ScalarInt_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<int>();
        await foreach (var v in conn.QueryTypedAsync<int>(IntSql))
            values.Add(v);

        Assert.Equal(ExpectedInts, values);
    }

    [Fact]
    public async Task QueryTyped_ScalarUInt64_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<ulong>();
        await foreach (var v in conn.QueryTypedAsync<ulong>(UInt64Sql))
            values.Add(v);

        Assert.Equal(new ulong[] { 7, 8, 9 }, values);
    }

    [Fact]
    public async Task QueryTyped_ScalarDouble_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<double>();
        await foreach (var v in conn.QueryTypedAsync<double>(
            "SELECT toFloat64(number) + 0.5 AS v FROM numbers(3)"))
            values.Add(v);

        Assert.Equal(new[] { 0.5, 1.5, 2.5 }, values);
    }

    [Fact]
    public async Task QueryTyped_ScalarDateTime_ReturnsColumnValues()
    {
        await using var conn = await OpenAsync();

        var values = new List<DateTime>();
        await foreach (var v in conn.QueryTypedAsync<DateTime>(
            "SELECT toDateTime('2024-03-05 06:07:08') AS v"))
            values.Add(v);

        Assert.Equal(new[] { new DateTime(2024, 3, 5, 6, 7, 8) }, values);
    }
}

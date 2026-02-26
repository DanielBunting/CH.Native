using CH.Native.SmokeTests.Fixtures;
using CH.Native.SmokeTests.Helpers;
using Xunit;

namespace CH.Native.SmokeTests.Query;

[Collection("SmokeTest")]
public class CompositeTypeSmokeTests
{
    private readonly SmokeTestFixture _fixture;

    public CompositeTypeSmokeTests(SmokeTestFixture fixture)
    {
        _fixture = fixture;
    }

    private async Task RunCompositeTest(string columnDef, string insertValues, string selectExpr = "*")
    {
        var table = $"smoke_composite_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} ({columnDef}) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES {insertValues}");

            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT {selectExpr} FROM {table}");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT {selectExpr} FROM {table}");

            ResultComparer.AssertResultsEqual(native, driver, $"Composite: {columnDef}");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public Task ArrayOfInt32() =>
        RunCompositeTest("val Array(Int32)", "([1, 2, 3]),([]),([42])");

    [Fact]
    public Task ArrayOfArrayOfInt32() =>
        RunCompositeTest("val Array(Array(Int32))", "([[1, 2], [3, 4]]),([[]]),([[42]])");

    [Fact]
    public Task ArrayOfNullableString() =>
        RunCompositeTest("val Array(Nullable(String))", "(['hello', NULL, 'world']),([]),([NULL])");

    [Fact]
    public Task MapStringInt32() =>
        RunCompositeTest("val Map(String, Int32)", "({'a': 1, 'b': 2}),({})");

    [Fact]
    public Task MapStringNullableInt32() =>
        RunCompositeTest("val Map(String, Nullable(Int32))", "({'a': 1, 'b': NULL}),({})");

    [Fact]
    public Task MapStringArrayInt32() =>
        RunCompositeTest("val Map(String, Array(Int32))", "({'x': [1, 2], 'y': [3]}),({})");

    [Fact]
    public Task TupleIntString() =>
        RunCompositeTest("val Tuple(Int32, String)", "((1, 'hello')),((42, 'world'))");

    [Fact]
    public Task TupleNullableStringInt32() =>
        RunCompositeTest("val Tuple(Nullable(String), Int32)", "(('hello', 1)),((NULL, 42))");

    [Fact]
    public Task ArrayOfTuple() =>
        RunCompositeTest("val Array(Tuple(Int32, String))", "([(1, 'a'), (2, 'b')]),([])");

    [Fact]
    public Task LowCardinalityString() =>
        RunCompositeTest("val LowCardinality(String)", "('red'),('green'),('blue'),('red'),('green')");

    [Fact]
    public async Task LowCardinalityFixedString()
    {
        await RunCompositeTest(
            "val LowCardinality(FixedString(8))",
            "('hello'),('test1234'),('abc')");
    }

    [Fact]
    public async Task LowCardinalityOverflowUInt8ToUInt16()
    {
        // 300+ unique values forces overflow from UInt8 to UInt16 index
        var table = $"smoke_composite_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (val LowCardinality(String)) ENGINE = Memory");

            var values = string.Join(",", Enumerable.Range(0, 300).Select(i => $"('val_{i}')"));
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES {values}");

            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT val FROM {table} ORDER BY val");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT val FROM {table} ORDER BY val");

            ResultComparer.AssertResultsEqual(native, driver, "LowCardinality overflow UInt8->UInt16");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task LowCardinalityOverflowUInt16ToUInt32()
    {
        // 70K+ unique values forces overflow from UInt16 to UInt32 index
        var table = $"smoke_composite_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"CREATE TABLE {table} (val LowCardinality(String)) ENGINE = Memory");

            // Insert in batches to avoid huge SQL strings
            for (int batch = 0; batch < 70; batch++)
            {
                var values = string.Join(",", Enumerable.Range(batch * 1000, 1000).Select(i => $"('val_{i}')"));
                await NativeQueryHelper.ExecuteNonQueryAsync(
                    _fixture.NativeConnectionString,
                    $"INSERT INTO {table} VALUES {values}");
            }

            // Compare count and spot-check
            var nativeCount = await NativeQueryHelper.ExecuteScalarAsync<ulong>(
                _fixture.NativeConnectionString,
                $"SELECT count() FROM {table}");

            var driverCount = await DriverQueryHelper.ExecuteScalarAsync(
                _fixture.DriverConnectionString,
                $"SELECT count() FROM {table}");

            Assert.Equal(70000UL, nativeCount);
            Assert.Equal(70000UL, Convert.ToUInt64(driverCount));

            // Spot-check some values
            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT val FROM {table} WHERE val IN ('val_0', 'val_35000', 'val_69999') ORDER BY val");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT val FROM {table} WHERE val IN ('val_0', 'val_35000', 'val_69999') ORDER BY val");

            ResultComparer.AssertResultsEqual(native, driver, "LowCardinality overflow UInt16->UInt32");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Fact]
    public async Task NestedType()
    {
        var table = $"smoke_composite_{Guid.NewGuid():N}";
        try
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $@"CREATE TABLE {table} (
                    id Int32,
                    nested Nested(
                        key String,
                        value Int32
                    )
                ) ENGINE = Memory");

            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"INSERT INTO {table} VALUES (1, ['a', 'b'], [10, 20]), (2, [], [])");

            // Nested columns are accessed as arrays
            var native = await NativeQueryHelper.QueryAsync(
                _fixture.NativeConnectionString,
                $"SELECT id, nested.key, nested.value FROM {table} ORDER BY id");

            var driver = await DriverQueryHelper.QueryAsync(
                _fixture.DriverConnectionString,
                $"SELECT id, nested.key, nested.value FROM {table} ORDER BY id");

            ResultComparer.AssertResultsEqual(native, driver, "Nested");
        }
        finally
        {
            await NativeQueryHelper.ExecuteNonQueryAsync(
                _fixture.NativeConnectionString,
                $"DROP TABLE IF EXISTS {table}");
        }
    }
}

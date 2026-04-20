using System.Net;
using BenchmarkDotNet.Attributes;
using CH.Native.Benchmarks.Infrastructure;
using NativeConnection = CH.Native.Connection.ClickHouseConnection;
using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace CH.Native.Benchmarks.Benchmarks;

/// <summary>
/// Cross-driver per-type read suite. For every ClickHouse primitive, measures
/// four scenarios: { Native, Driver } × { Bulk 100k, Single LIMIT 1 }.
///
/// - Bulk highlights per-row decode cost (throughput-bound).
/// - Single highlights per-query overhead and any one-off setup amortisation
///   (e.g. parameterised type construction, schema parsing).
/// - Native vs Driver shows the practical wins of the native TCP path against
///   the official HTTP-based ClickHouse.Driver for each individual type.
/// </summary>
[Config(typeof(ProtocolComparisonConfig))]
public class BenchmarkTypeSuite
{
    private const int BulkRowCount = 100_000;

    // SETTINGS clause appended to all queries — Time/Time64 require it on 25.10
    // and it's a no-op for other types.
    private const string Settings = " SETTINGS enable_time_time64_type=1";

    // Tables — one per type, populated once in GlobalSetup.
    private const string TInt8 = "ts_int8";
    private const string TInt16 = "ts_int16";
    private const string TInt32 = "ts_int32";
    private const string TInt64 = "ts_int64";
    private const string TInt128 = "ts_int128";
    private const string TUInt8 = "ts_uint8";
    private const string TUInt16 = "ts_uint16";
    private const string TUInt32 = "ts_uint32";
    private const string TUInt64 = "ts_uint64";
    private const string TFloat32 = "ts_float32";
    private const string TFloat64 = "ts_float64";
    private const string TBFloat16 = "ts_bfloat16";
    private const string TBool = "ts_bool";
    private const string TString = "ts_string";
    private const string TFixedString = "ts_fixedstring";
    private const string TDate = "ts_date";
    private const string TDate32 = "ts_date32";
    private const string TDateTime = "ts_datetime";
    private const string TDateTime64_3 = "ts_dt64_3";
    private const string TDateTime64_9 = "ts_dt64_9";
    private const string TTime = "ts_time";
    private const string TTime64_0 = "ts_time64_0";
    private const string TTime64_3 = "ts_time64_3";
    private const string TTime64_6 = "ts_time64_6";
    private const string TTime64_7 = "ts_time64_7";
    private const string TTime64_9 = "ts_time64_9";
    private const string TUuid = "ts_uuid";
    private const string TIPv4 = "ts_ipv4";
    private const string TIPv6 = "ts_ipv6";
    private const string TDecimal64 = "ts_dec64";

    private NativeConnection _native = null!;
    private DriverConnection _driver = null!;

    [GlobalSetup]
    public async Task GlobalSetup()
    {
        await BenchmarkContainerManager.Instance.EnsureInitializedAsync();
        var manager = BenchmarkContainerManager.Instance;

        _native = new NativeConnection(manager.NativeConnectionString);
        await _native.OpenAsync();
        await _native.ExecuteNonQueryAsync("SET enable_time_time64_type=1");

        _driver = new DriverConnection(manager.DriverConnectionString);
        await _driver.OpenAsync();

        await EnsureAsync(TInt8, "Int8", "toInt8(number % 100)");
        await EnsureAsync(TInt16, "Int16", "toInt16(number % 30000)");
        await EnsureAsync(TInt32, "Int32", "toInt32(number)");
        await EnsureAsync(TInt64, "Int64", "toInt64(number)");
        await EnsureAsync(TInt128, "Int128", "toInt128(number)");
        await EnsureAsync(TUInt8, "UInt8", "toUInt8(number % 200)");
        await EnsureAsync(TUInt16, "UInt16", "toUInt16(number % 60000)");
        await EnsureAsync(TUInt32, "UInt32", "toUInt32(number)");
        await EnsureAsync(TUInt64, "UInt64", "toUInt64(number)");
        await EnsureAsync(TFloat32, "Float32", "toFloat32(number * 1.5)");
        await EnsureAsync(TFloat64, "Float64", "toFloat64(number * 1.5)");
        await EnsureAsync(TBFloat16, "BFloat16", "toBFloat16(number * 0.5)");
        await EnsureAsync(TBool, "Bool", "number % 2 = 0");
        await EnsureAsync(TString, "String", "concat('row_', toString(number))");
        await EnsureAsync(TFixedString, "FixedString(16)", "toFixedString(concat('row_', toString(number % 100000)), 16)");
        await EnsureAsync(TDate, "Date", "toDate('2024-01-01') + number % 365");
        await EnsureAsync(TDate32, "Date32", "toDate32('1970-01-01') + number % 100000");
        await EnsureAsync(TDateTime, "DateTime", "toDateTime('2024-01-01 00:00:00') + number % 86400");
        await EnsureAsync(TDateTime64_3, "DateTime64(3)", "toDateTime64('2024-01-01 00:00:00.000', 3) + number % 86400");
        await EnsureAsync(TDateTime64_9, "DateTime64(9)", "toDateTime64('2024-01-01 00:00:00.000000000', 9) + number % 86400");
        await EnsureAsync(TTime, "Time", "number % 86400");
        await EnsureAsync(TTime64_0, "Time64(0)", "number % 86400");
        await EnsureAsync(TTime64_3, "Time64(3)", "number % 86400");
        await EnsureAsync(TTime64_6, "Time64(6)", "number % 86400");
        await EnsureAsync(TTime64_7, "Time64(7)", "number % 86400");
        await EnsureAsync(TTime64_9, "Time64(9)", "number % 86400");
        await EnsureAsync(TUuid, "UUID", "generateUUIDv4()");
        await EnsureAsync(TIPv4, "IPv4", "toIPv4('10.0.0.0') + number % 65536");
        await EnsureAsync(TIPv6, "IPv6", "toIPv6(concat('::1.0.0.', toString(number % 256)))");
        await EnsureAsync(TDecimal64, "Decimal64(4)", "toDecimal64(number * 1.234, 4)");
    }

    [GlobalCleanup]
    public async Task GlobalCleanup()
    {
        await _native.DisposeAsync();
        await _driver.DisposeAsync();
    }

    private async Task EnsureAsync(string table, string columnType, string genExpr)
    {
        await _native.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {table} (id UInt64, v {columnType})
            ENGINE = MergeTree() ORDER BY id
            SETTINGS enable_time_time64_type=1");

        var existing = await _native.ExecuteScalarAsync<ulong>($"SELECT count() FROM {table}");
        if (existing == (ulong)BulkRowCount) return;

        await _native.ExecuteNonQueryAsync($"TRUNCATE TABLE {table}");
        await _native.ExecuteNonQueryAsync($@"
            INSERT INTO {table}
            SELECT number AS id, {genExpr} AS v FROM numbers({BulkRowCount})
            SETTINGS enable_time_time64_type=1");
    }

    // --- Native helpers ---

    private async Task<long> NativeBulkAsync<T>(string table) where T : struct
    {
        long count = 0;
        await foreach (var row in _native.QueryAsync($"SELECT v FROM {table}{Settings}"))
        {
            row.GetFieldValue<T>("v");
            count++;
        }
        return count;
    }

    private async Task<long> NativeBulkRefAsync<T>(string table) where T : class
    {
        long count = 0;
        await foreach (var row in _native.QueryAsync($"SELECT v FROM {table}{Settings}"))
        {
            row.GetFieldValue<T>("v");
            count++;
        }
        return count;
    }

    private async Task<T> NativeSingleAsync<T>(string table) where T : struct
    {
        await foreach (var row in _native.QueryAsync($"SELECT v FROM {table} LIMIT 1{Settings}"))
        {
            return row.GetFieldValue<T>("v");
        }
        throw new InvalidOperationException("no row");
    }

    private async Task<T?> NativeSingleRefAsync<T>(string table) where T : class
    {
        await foreach (var row in _native.QueryAsync($"SELECT v FROM {table} LIMIT 1{Settings}"))
        {
            return row.GetFieldValue<T>("v");
        }
        return null;
    }

    // --- Driver helpers (uses generic GetValue so we don't have to know what the
    // Driver returns for new types like BFloat16/Time/Time64). ---

    private async Task<long> DriverBulkAsync(string table)
    {
        using var cmd = _driver.CreateCommand();
        cmd.CommandText = $"SELECT v FROM {table}{Settings}";
        using var reader = await cmd.ExecuteReaderAsync();
        long count = 0;
        while (await reader.ReadAsync())
        {
            reader.GetValue(0);
            count++;
        }
        return count;
    }

    private async Task<object?> DriverSingleAsync(string table)
    {
        using var cmd = _driver.CreateCommand();
        cmd.CommandText = $"SELECT v FROM {table} LIMIT 1{Settings}";
        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync()) return null;
        return reader.GetValue(0);
    }

    // --- Bulk Native ---
    [Benchmark] public Task<long> Bulk_Native_Int8() => NativeBulkAsync<sbyte>(TInt8);
    [Benchmark] public Task<long> Bulk_Native_Int16() => NativeBulkAsync<short>(TInt16);
    [Benchmark] public Task<long> Bulk_Native_Int32() => NativeBulkAsync<int>(TInt32);
    [Benchmark] public Task<long> Bulk_Native_Int64() => NativeBulkAsync<long>(TInt64);
    [Benchmark] public Task<long> Bulk_Native_Int128() => NativeBulkAsync<Int128>(TInt128);
    [Benchmark] public Task<long> Bulk_Native_UInt8() => NativeBulkAsync<byte>(TUInt8);
    [Benchmark] public Task<long> Bulk_Native_UInt16() => NativeBulkAsync<ushort>(TUInt16);
    [Benchmark] public Task<long> Bulk_Native_UInt32() => NativeBulkAsync<uint>(TUInt32);
    [Benchmark] public Task<long> Bulk_Native_UInt64() => NativeBulkAsync<ulong>(TUInt64);
    [Benchmark] public Task<long> Bulk_Native_Float32() => NativeBulkAsync<float>(TFloat32);
    [Benchmark] public Task<long> Bulk_Native_Float64() => NativeBulkAsync<double>(TFloat64);
    [Benchmark] public Task<long> Bulk_Native_BFloat16() => NativeBulkAsync<float>(TBFloat16);
    [Benchmark] public Task<long> Bulk_Native_Bool() => NativeBulkAsync<bool>(TBool);
    [Benchmark] public Task<long> Bulk_Native_String() => NativeBulkRefAsync<string>(TString);
    [Benchmark] public Task<long> Bulk_Native_FixedString() => NativeBulkRefAsync<byte[]>(TFixedString);
    [Benchmark] public Task<long> Bulk_Native_Date() => NativeBulkAsync<DateOnly>(TDate);
    [Benchmark] public Task<long> Bulk_Native_Date32() => NativeBulkAsync<DateOnly>(TDate32);
    [Benchmark] public Task<long> Bulk_Native_DateTime() => NativeBulkAsync<DateTime>(TDateTime);
    [Benchmark] public Task<long> Bulk_Native_DateTime64_3() => NativeBulkAsync<DateTime>(TDateTime64_3);
    [Benchmark] public Task<long> Bulk_Native_DateTime64_9() => NativeBulkAsync<DateTime>(TDateTime64_9);
    [Benchmark] public Task<long> Bulk_Native_Time() => NativeBulkAsync<TimeOnly>(TTime);
    [Benchmark] public Task<long> Bulk_Native_Time64_0() => NativeBulkAsync<TimeOnly>(TTime64_0);
    [Benchmark] public Task<long> Bulk_Native_Time64_3() => NativeBulkAsync<TimeOnly>(TTime64_3);
    [Benchmark] public Task<long> Bulk_Native_Time64_6() => NativeBulkAsync<TimeOnly>(TTime64_6);
    [Benchmark] public Task<long> Bulk_Native_Time64_7() => NativeBulkAsync<TimeOnly>(TTime64_7);
    [Benchmark] public Task<long> Bulk_Native_Time64_9() => NativeBulkAsync<TimeOnly>(TTime64_9);
    [Benchmark] public Task<long> Bulk_Native_UUID() => NativeBulkAsync<Guid>(TUuid);
    [Benchmark] public Task<long> Bulk_Native_IPv4() => NativeBulkRefAsync<IPAddress>(TIPv4);
    [Benchmark] public Task<long> Bulk_Native_IPv6() => NativeBulkRefAsync<IPAddress>(TIPv6);
    [Benchmark] public Task<long> Bulk_Native_Decimal64() => NativeBulkAsync<decimal>(TDecimal64);

    // --- Bulk Driver ---
    [Benchmark] public Task<long> Bulk_Driver_Int8() => DriverBulkAsync(TInt8);
    [Benchmark] public Task<long> Bulk_Driver_Int16() => DriverBulkAsync(TInt16);
    [Benchmark] public Task<long> Bulk_Driver_Int32() => DriverBulkAsync(TInt32);
    [Benchmark] public Task<long> Bulk_Driver_Int64() => DriverBulkAsync(TInt64);
    [Benchmark] public Task<long> Bulk_Driver_Int128() => DriverBulkAsync(TInt128);
    [Benchmark] public Task<long> Bulk_Driver_UInt8() => DriverBulkAsync(TUInt8);
    [Benchmark] public Task<long> Bulk_Driver_UInt16() => DriverBulkAsync(TUInt16);
    [Benchmark] public Task<long> Bulk_Driver_UInt32() => DriverBulkAsync(TUInt32);
    [Benchmark] public Task<long> Bulk_Driver_UInt64() => DriverBulkAsync(TUInt64);
    [Benchmark] public Task<long> Bulk_Driver_Float32() => DriverBulkAsync(TFloat32);
    [Benchmark] public Task<long> Bulk_Driver_Float64() => DriverBulkAsync(TFloat64);
    [Benchmark] public Task<long> Bulk_Driver_BFloat16() => DriverBulkAsync(TBFloat16);
    [Benchmark] public Task<long> Bulk_Driver_Bool() => DriverBulkAsync(TBool);
    [Benchmark] public Task<long> Bulk_Driver_String() => DriverBulkAsync(TString);
    [Benchmark] public Task<long> Bulk_Driver_FixedString() => DriverBulkAsync(TFixedString);
    [Benchmark] public Task<long> Bulk_Driver_Date() => DriverBulkAsync(TDate);
    [Benchmark] public Task<long> Bulk_Driver_Date32() => DriverBulkAsync(TDate32);
    [Benchmark] public Task<long> Bulk_Driver_DateTime() => DriverBulkAsync(TDateTime);
    [Benchmark] public Task<long> Bulk_Driver_DateTime64_3() => DriverBulkAsync(TDateTime64_3);
    [Benchmark] public Task<long> Bulk_Driver_DateTime64_9() => DriverBulkAsync(TDateTime64_9);
    [Benchmark] public Task<long> Bulk_Driver_Time() => DriverBulkAsync(TTime);
    [Benchmark] public Task<long> Bulk_Driver_Time64_0() => DriverBulkAsync(TTime64_0);
    [Benchmark] public Task<long> Bulk_Driver_Time64_3() => DriverBulkAsync(TTime64_3);
    [Benchmark] public Task<long> Bulk_Driver_Time64_6() => DriverBulkAsync(TTime64_6);
    [Benchmark] public Task<long> Bulk_Driver_Time64_7() => DriverBulkAsync(TTime64_7);
    [Benchmark] public Task<long> Bulk_Driver_Time64_9() => DriverBulkAsync(TTime64_9);
    [Benchmark] public Task<long> Bulk_Driver_UUID() => DriverBulkAsync(TUuid);
    [Benchmark] public Task<long> Bulk_Driver_IPv4() => DriverBulkAsync(TIPv4);
    [Benchmark] public Task<long> Bulk_Driver_IPv6() => DriverBulkAsync(TIPv6);
    [Benchmark] public Task<long> Bulk_Driver_Decimal64() => DriverBulkAsync(TDecimal64);

    // --- Single Native ---
    [Benchmark] public Task<sbyte> Single_Native_Int8() => NativeSingleAsync<sbyte>(TInt8);
    [Benchmark] public Task<short> Single_Native_Int16() => NativeSingleAsync<short>(TInt16);
    [Benchmark] public Task<int> Single_Native_Int32() => NativeSingleAsync<int>(TInt32);
    [Benchmark] public Task<long> Single_Native_Int64() => NativeSingleAsync<long>(TInt64);
    [Benchmark] public Task<Int128> Single_Native_Int128() => NativeSingleAsync<Int128>(TInt128);
    [Benchmark] public Task<byte> Single_Native_UInt8() => NativeSingleAsync<byte>(TUInt8);
    [Benchmark] public Task<ushort> Single_Native_UInt16() => NativeSingleAsync<ushort>(TUInt16);
    [Benchmark] public Task<uint> Single_Native_UInt32() => NativeSingleAsync<uint>(TUInt32);
    [Benchmark] public Task<ulong> Single_Native_UInt64() => NativeSingleAsync<ulong>(TUInt64);
    [Benchmark] public Task<float> Single_Native_Float32() => NativeSingleAsync<float>(TFloat32);
    [Benchmark] public Task<double> Single_Native_Float64() => NativeSingleAsync<double>(TFloat64);
    [Benchmark] public Task<float> Single_Native_BFloat16() => NativeSingleAsync<float>(TBFloat16);
    [Benchmark] public Task<bool> Single_Native_Bool() => NativeSingleAsync<bool>(TBool);
    [Benchmark] public Task<string?> Single_Native_String() => NativeSingleRefAsync<string>(TString);
    [Benchmark] public Task<byte[]?> Single_Native_FixedString() => NativeSingleRefAsync<byte[]>(TFixedString);
    [Benchmark] public Task<DateOnly> Single_Native_Date() => NativeSingleAsync<DateOnly>(TDate);
    [Benchmark] public Task<DateOnly> Single_Native_Date32() => NativeSingleAsync<DateOnly>(TDate32);
    [Benchmark] public Task<DateTime> Single_Native_DateTime() => NativeSingleAsync<DateTime>(TDateTime);
    [Benchmark] public Task<DateTime> Single_Native_DateTime64_3() => NativeSingleAsync<DateTime>(TDateTime64_3);
    [Benchmark] public Task<DateTime> Single_Native_DateTime64_9() => NativeSingleAsync<DateTime>(TDateTime64_9);
    [Benchmark] public Task<TimeOnly> Single_Native_Time() => NativeSingleAsync<TimeOnly>(TTime);
    [Benchmark] public Task<TimeOnly> Single_Native_Time64_0() => NativeSingleAsync<TimeOnly>(TTime64_0);
    [Benchmark] public Task<TimeOnly> Single_Native_Time64_3() => NativeSingleAsync<TimeOnly>(TTime64_3);
    [Benchmark] public Task<TimeOnly> Single_Native_Time64_6() => NativeSingleAsync<TimeOnly>(TTime64_6);
    [Benchmark] public Task<TimeOnly> Single_Native_Time64_7() => NativeSingleAsync<TimeOnly>(TTime64_7);
    [Benchmark] public Task<TimeOnly> Single_Native_Time64_9() => NativeSingleAsync<TimeOnly>(TTime64_9);
    [Benchmark] public Task<Guid> Single_Native_UUID() => NativeSingleAsync<Guid>(TUuid);
    [Benchmark] public Task<IPAddress?> Single_Native_IPv4() => NativeSingleRefAsync<IPAddress>(TIPv4);
    [Benchmark] public Task<IPAddress?> Single_Native_IPv6() => NativeSingleRefAsync<IPAddress>(TIPv6);
    [Benchmark] public Task<decimal> Single_Native_Decimal64() => NativeSingleAsync<decimal>(TDecimal64);

    // --- Single Driver ---
    [Benchmark] public Task<object?> Single_Driver_Int8() => DriverSingleAsync(TInt8);
    [Benchmark] public Task<object?> Single_Driver_Int16() => DriverSingleAsync(TInt16);
    [Benchmark] public Task<object?> Single_Driver_Int32() => DriverSingleAsync(TInt32);
    [Benchmark] public Task<object?> Single_Driver_Int64() => DriverSingleAsync(TInt64);
    [Benchmark] public Task<object?> Single_Driver_Int128() => DriverSingleAsync(TInt128);
    [Benchmark] public Task<object?> Single_Driver_UInt8() => DriverSingleAsync(TUInt8);
    [Benchmark] public Task<object?> Single_Driver_UInt16() => DriverSingleAsync(TUInt16);
    [Benchmark] public Task<object?> Single_Driver_UInt32() => DriverSingleAsync(TUInt32);
    [Benchmark] public Task<object?> Single_Driver_UInt64() => DriverSingleAsync(TUInt64);
    [Benchmark] public Task<object?> Single_Driver_Float32() => DriverSingleAsync(TFloat32);
    [Benchmark] public Task<object?> Single_Driver_Float64() => DriverSingleAsync(TFloat64);
    [Benchmark] public Task<object?> Single_Driver_BFloat16() => DriverSingleAsync(TBFloat16);
    [Benchmark] public Task<object?> Single_Driver_Bool() => DriverSingleAsync(TBool);
    [Benchmark] public Task<object?> Single_Driver_String() => DriverSingleAsync(TString);
    [Benchmark] public Task<object?> Single_Driver_FixedString() => DriverSingleAsync(TFixedString);
    [Benchmark] public Task<object?> Single_Driver_Date() => DriverSingleAsync(TDate);
    [Benchmark] public Task<object?> Single_Driver_Date32() => DriverSingleAsync(TDate32);
    [Benchmark] public Task<object?> Single_Driver_DateTime() => DriverSingleAsync(TDateTime);
    [Benchmark] public Task<object?> Single_Driver_DateTime64_3() => DriverSingleAsync(TDateTime64_3);
    [Benchmark] public Task<object?> Single_Driver_DateTime64_9() => DriverSingleAsync(TDateTime64_9);
    [Benchmark] public Task<object?> Single_Driver_Time() => DriverSingleAsync(TTime);
    [Benchmark] public Task<object?> Single_Driver_Time64_0() => DriverSingleAsync(TTime64_0);
    [Benchmark] public Task<object?> Single_Driver_Time64_3() => DriverSingleAsync(TTime64_3);
    [Benchmark] public Task<object?> Single_Driver_Time64_6() => DriverSingleAsync(TTime64_6);
    [Benchmark] public Task<object?> Single_Driver_Time64_7() => DriverSingleAsync(TTime64_7);
    [Benchmark] public Task<object?> Single_Driver_Time64_9() => DriverSingleAsync(TTime64_9);
    [Benchmark] public Task<object?> Single_Driver_UUID() => DriverSingleAsync(TUuid);
    [Benchmark] public Task<object?> Single_Driver_IPv4() => DriverSingleAsync(TIPv4);
    [Benchmark] public Task<object?> Single_Driver_IPv6() => DriverSingleAsync(TIPv6);
    [Benchmark] public Task<object?> Single_Driver_Decimal64() => DriverSingleAsync(TDecimal64);
}

using Dapper;

namespace CH.Native.Dapper;

/// <summary>
/// Entry point for CH.Native's Dapper integration. Call
/// <see cref="Register"/> once during application startup to enable array
/// parameters (<c>int[]</c>, <c>string[]</c>, etc.) in Dapper queries against
/// ClickHouse.
/// </summary>
public static class ClickHouseDapperIntegration
{
    private static int _registered;

    /// <summary>
    /// Registers Dapper type handlers for the CLR array types we want to bind
    /// as <c>Array(T)</c> on the wire. Safe to call multiple times — subsequent
    /// invocations are no-ops.
    /// </summary>
    /// <remarks>
    /// Registered types: <c>bool[]</c>, signed/unsigned integer arrays
    /// (<c>sbyte[]</c>, <c>short[]</c>, <c>int[]</c>, <c>long[]</c>,
    /// <c>byte[]</c>, <c>ushort[]</c>, <c>uint[]</c>, <c>ulong[]</c>),
    /// floating-point arrays (<c>float[]</c>, <c>double[]</c>,
    /// <c>decimal[]</c>), <c>string[]</c>, <c>Guid[]</c>, <c>DateTime[]</c>,
    /// <c>DateTimeOffset[]</c>, and <c>DateOnly[]</c>.
    /// </remarks>
    public static void Register()
    {
        // Idempotent — registering the same handler twice is harmless with
        // Dapper but wastes cycles on repeated init paths.
        if (Interlocked.Exchange(ref _registered, 1) == 1)
            return;

        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<bool>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<sbyte>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<short>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<int>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<long>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<byte>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<ushort>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<uint>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<ulong>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<float>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<double>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<decimal>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<string>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<Guid>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<DateTime>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<DateTimeOffset>());
        SqlMapper.AddTypeHandler(new ClickHouseDapperArrayHandler<DateOnly>());
    }
}

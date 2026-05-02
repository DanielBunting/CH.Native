using Dapper;

namespace CH.Native.Dapper;

/// <summary>
/// Entry point for CH.Native's Dapper integration. Call
/// <see cref="Register"/> once during application startup to enable array
/// parameters (<c>int[]</c>, <c>string[]</c>, etc.) in Dapper queries against
/// ClickHouse and to bring Dapper's column→property mapping into line with
/// CH.Native's typed read path (snake_case columns map to PascalCase
/// properties out of the box).
/// </summary>
public static class ClickHouseDapperIntegration
{
    private static int _registered;

    /// <summary>
    /// Registers Dapper type handlers for the CLR array types we want to bind
    /// as <c>Array(T)</c> on the wire, and enables
    /// <see cref="DefaultTypeMap.MatchNamesWithUnderscores"/> so that
    /// snake_case columns (e.g. <c>user_id</c>) populate PascalCase
    /// properties (e.g. <c>UserId</c>) without per-property attributes —
    /// matching <c>connection.QueryAsync&lt;T&gt;</c>'s built-in
    /// <c>TypeMapper</c> snake_case fallback. Safe to call multiple times —
    /// subsequent invocations are no-ops.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Registered types: <c>bool[]</c>, signed/unsigned integer arrays
    /// (<c>sbyte[]</c>, <c>short[]</c>, <c>int[]</c>, <c>long[]</c>,
    /// <c>byte[]</c>, <c>ushort[]</c>, <c>uint[]</c>, <c>ulong[]</c>),
    /// floating-point arrays (<c>float[]</c>, <c>double[]</c>,
    /// <c>decimal[]</c>), <c>string[]</c>, <c>Guid[]</c>, <c>DateTime[]</c>,
    /// <c>DateTimeOffset[]</c>, and <c>DateOnly[]</c>.
    /// </para>
    /// <para>
    /// <b>Process-global side effect.</b> <c>MatchNamesWithUnderscores</c> is
    /// a process-wide Dapper setting; flipping it affects every Dapper
    /// consumer in the AppDomain. Callers who want raw-name mapping can
    /// override it back after calling <c>Register()</c>:
    /// <code>
    /// ClickHouseDapperIntegration.Register();
    /// Dapper.DefaultTypeMap.MatchNamesWithUnderscores = false;
    /// </code>
    /// The override survives later <c>Register()</c> calls because the
    /// property is only assigned on the first invocation.
    /// </para>
    /// </remarks>
    public static void Register()
    {
        // Idempotent — registering the same handler twice is harmless with
        // Dapper but wastes cycles on repeated init paths. The
        // MatchNamesWithUnderscores flip lives inside this guard so a
        // caller-supplied override (set after the first Register) survives
        // any later Register calls.
        if (Interlocked.Exchange(ref _registered, 1) == 1)
            return;

        DefaultTypeMap.MatchNamesWithUnderscores = true;

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

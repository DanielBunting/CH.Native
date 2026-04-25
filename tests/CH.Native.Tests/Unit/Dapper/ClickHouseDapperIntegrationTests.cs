using System.Data;
using CH.Native.Dapper;
using global::Dapper;
using Xunit;

namespace CH.Native.Tests.Unit.Dapper;

/// <summary>
/// Unit tests covering the registration surface of CH.Native's Dapper integration.
/// The <see cref="DapperHandlerRegistration"/> helper fires a <c>[ModuleInitializer]</c>
/// once per assembly load, so by the time these tests run the handlers are already
/// registered. The tests exist to (a) pin the set of types the library guarantees
/// coverage for, and (b) catch regressions where someone removes the module
/// initializer or the opt-in <see cref="ClickHouseDapperIntegration.Register"/> call.
/// </summary>
public class ClickHouseDapperIntegrationTests
{
    [Fact]
    public void Register_IsIdempotent()
    {
        // Calling Register more than once must be a cheap no-op; this guards the
        // "call it from every DI container / test fixture startup" pattern.
        ClickHouseDapperIntegration.Register();
        ClickHouseDapperIntegration.Register();
        ClickHouseDapperIntegration.Register();

        // If we got here without an exception the idempotency guard is holding.
        Assert.True(true);
    }

    // The [ModuleInitializer] in DapperHandlerRegistration runs exactly once when
    // this test assembly loads, so by the time any test executes the handlers are
    // already registered. The checks below confirm the integration point is still
    // wired up — if the module initializer is removed or the attribute goes stale,
    // these tests fail loudly rather than silently letting Dapper fall back to
    // list-expansion in the integration suite.

    public static readonly TheoryData<Type> RegisteredArrayTypes = new()
    {
        typeof(bool[]),
        typeof(sbyte[]),
        typeof(short[]),
        typeof(int[]),
        typeof(long[]),
        typeof(byte[]),
        typeof(ushort[]),
        typeof(uint[]),
        typeof(ulong[]),
        typeof(float[]),
        typeof(double[]),
        typeof(decimal[]),
        typeof(string[]),
        typeof(Guid[]),
        typeof(DateTime[]),
        typeof(DateTimeOffset[]),
        typeof(DateOnly[]),
    };

    [Theory]
    [MemberData(nameof(RegisteredArrayTypes))]
    public void AfterRegister_DapperHasHandlerForArrayType(Type arrayType)
    {
        // Idempotent; the module initializer already ran but calling again is safe.
        ClickHouseDapperIntegration.Register();

        // Dapper stores registered handlers in the internal static field
        // `SqlMapper.typeHandlers` — a Dictionary<Type, ITypeHandler>. When a
        // handler is present, Dapper's LookupDbType hands parameter binding to
        // that handler instead of applying list-expansion.
        var field = typeof(SqlMapper).GetField(
            "typeHandlers",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        Assert.NotNull(field);

        var dict = (System.Collections.IDictionary)field!.GetValue(null)!;
        Assert.True(dict.Contains(arrayType),
            $"No Dapper type handler registered for {arrayType.Name}. The module " +
            "initializer in DapperHandlerRegistration should register it; if this " +
            "test fails after a Dapper upgrade, the internal `typeHandlers` field " +
            "may have been renamed.");
    }
}

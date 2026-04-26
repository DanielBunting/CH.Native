using System.Runtime.CompilerServices;
using CH.Native.Dapper;

namespace CH.Native.Tests.Support;

/// <summary>
/// Registers CH.Native's Dapper type handlers once per test assembly load so
/// that Dapper tests using array parameters (<c>new { ids = new[] { 1, 2, 3 } }</c>)
/// bind to ClickHouse <c>Array(T)</c> rather than falling through to Dapper's
/// default list-expansion into a tuple literal.
/// </summary>
internal static class DapperHandlerRegistration
{
    [ModuleInitializer]
    internal static void Init()
    {
        ClickHouseDapperIntegration.Register();
    }
}

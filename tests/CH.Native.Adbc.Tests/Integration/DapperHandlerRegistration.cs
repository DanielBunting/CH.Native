using System.Runtime.CompilerServices;
using CH.Native.Dapper;

namespace CH.Native.Adbc.Tests.Integration;

/// <summary>
/// Registers CH.Native's Dapper type handlers once per test-assembly load so the parity tests
/// exercise Dapper exactly as a real CH.Native+Dapper consumer would.
/// </summary>
internal static class DapperHandlerRegistration
{
    [ModuleInitializer]
    internal static void Init() => ClickHouseDapperIntegration.Register();
}

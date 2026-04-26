using CH.Native.Mapping;

namespace CH.Native.SystemTests.Security.Helpers;

/// <summary>
/// Two-column row matching <see cref="EscapeTableHarness"/>'s <c>(id Int32, value String)</c>.
/// Used by bulk-insert and LINQ tests in the Security suite.
/// </summary>
internal sealed class SecurityRow
{
    [ClickHouseColumn(Name = "id", Order = 0)] public int Id { get; set; }
    [ClickHouseColumn(Name = "value", Order = 1)] public string Value { get; set; } = string.Empty;
}

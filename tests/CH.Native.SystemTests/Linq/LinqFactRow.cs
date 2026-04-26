using CH.Native.Mapping;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Row shape for the shared LINQ fact table. The fixture seeds 1000 rows with
/// deterministic values; tests query against this via <c>connection.Table&lt;LinqFactRow&gt;(name)</c>.
/// </summary>
public class LinqFactRow
{
    [ClickHouseColumn(Name = "id", Order = 0)]
    public long Id { get; set; }

    [ClickHouseColumn(Name = "country", Order = 1)]
    public string Country { get; set; } = string.Empty;

    [ClickHouseColumn(Name = "amount", Order = 2)]
    public double Amount { get; set; }

    [ClickHouseColumn(Name = "quantity", Order = 3)]
    public int Quantity { get; set; }

    [ClickHouseColumn(Name = "optional_code", Order = 4)]
    public int? OptionalCode { get; set; }

    [ClickHouseColumn(Name = "name", Order = 5)]
    public string Name { get; set; } = string.Empty;

    [ClickHouseColumn(Name = "created_at", Order = 6)]
    public DateTime CreatedAt { get; set; }

    [ClickHouseColumn(Name = "active", Order = 7)]
    public byte Active { get; set; }
}

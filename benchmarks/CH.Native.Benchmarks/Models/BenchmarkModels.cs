using CH.Native.Mapping;

namespace CH.Native.Benchmarks.Models;

/// <summary>
/// Row model for simple table queries.
/// </summary>
public class SimpleRow
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
    public DateTime Created { get; set; }
}

/// <summary>
/// Row model for large table queries.
/// </summary>
public class LargeTableRow
{
    public long Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public double Value { get; set; }
    public int Quantity { get; set; }
    public DateTime Created { get; set; }
}

/// <summary>
/// Row model for complex table queries.
/// </summary>
public class ComplexRow
{
    public long Id { get; set; }
    public long UserId { get; set; }
    public long ProductId { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public int Quantity { get; set; }
    public DateTime Created { get; set; }
}

/// <summary>
/// Row model for bulk insert benchmarks.
/// </summary>
public class InsertRow
{
    [ClickHouseColumn(Name = "id", Order = 0)]
    public long Id { get; set; }

    [ClickHouseColumn(Name = "name", Order = 1)]
    public string Name { get; set; } = string.Empty;

    [ClickHouseColumn(Name = "value", Order = 2)]
    public double Value { get; set; }
}

/// <summary>
/// Wide, value/date/numeric-heavy row for parallel bulk-insert benchmarks.
/// Mixes integer widths, floats, decimals, and date/time columns to exercise
/// the column-writer serialization path under load.
/// </summary>
public class WideInsertRow
{
    [ClickHouseColumn(Name = "id", Order = 0)]
    public long Id { get; set; }

    [ClickHouseColumn(Name = "seq", Order = 1)]
    public int Seq { get; set; }

    [ClickHouseColumn(Name = "code", Order = 2)]
    public string Code { get; set; } = string.Empty;

    [ClickHouseColumn(Name = "price", Order = 3)]
    public double Price { get; set; }

    [ClickHouseColumn(Name = "ratio", Order = 4)]
    public float Ratio { get; set; }

    [ClickHouseColumn(Name = "amount", Order = 5)]
    public decimal Amount { get; set; }

    [ClickHouseColumn(Name = "fee", Order = 6)]
    public decimal Fee { get; set; }

    [ClickHouseColumn(Name = "qty", Order = 7)]
    public int Qty { get; set; }

    [ClickHouseColumn(Name = "big", Order = 8)]
    public ulong Big { get; set; }

    [ClickHouseColumn(Name = "flag", Order = 9)]
    public byte Flag { get; set; }

    [ClickHouseColumn(Name = "created", Order = 10)]
    public DateTime Created { get; set; }

    [ClickHouseColumn(Name = "event_time", Order = 11)]
    public DateTime EventTime { get; set; }
}

/// <summary>
/// Aggregation result model.
/// </summary>
public class AggregationResult
{
    public string Category { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public long OrderCount { get; set; }
    public decimal TotalAmount { get; set; }
    public double AvgQuantity { get; set; }
}

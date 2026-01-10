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

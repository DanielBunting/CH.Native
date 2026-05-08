using CH.Native.Mapping;

namespace CH.Native.Samples.Hosting;

/// <summary>POCO for the <c>POST /events/bulk</c> endpoint's bulk-insert path.</summary>
public sealed class EventRow
{
    [ClickHouseColumn(Name = "event_id")] public Guid Id { get; set; }
    [ClickHouseColumn(Name = "ts")] public DateTime Timestamp { get; set; }
    [ClickHouseColumn(Name = "payload")] public string? Payload { get; set; }
}

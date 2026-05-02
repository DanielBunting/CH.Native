using CH.Native.Mapping;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// The attribute is consumed by reflection across BulkInserter, SchemaCache,
/// and the typed-row mapper. These tests pin the attribute's surface so a
/// rename / property removal would surface here, not at runtime in consumer
/// code.
/// </summary>
public class ClickHouseColumnAttributeTests
{
    private class Row
    {
        [ClickHouseColumn(Name = "renamed", Order = 0)]
        public int Id { get; set; }

        [ClickHouseColumn(ClickHouseType = "DateTime64(3)")]
        public DateTime At { get; set; }

        [ClickHouseColumn(Ignore = true)]
        public string Internal { get; set; } = "";

        public string Plain { get; set; } = "";
    }

    [Fact]
    public void Defaults_NameAndTypeAreNull_OrderIsMaxValue_IgnoreIsFalse()
    {
        var attr = new ClickHouseColumnAttribute();
        Assert.Null(attr.Name);
        Assert.Null(attr.ClickHouseType);
        Assert.Equal(int.MaxValue, attr.Order);
        Assert.False(attr.Ignore);
    }

    [Fact]
    public void Reflection_RecoversNameOverride()
    {
        var prop = typeof(Row).GetProperty(nameof(Row.Id))!;
        var attr = (ClickHouseColumnAttribute?)Attribute.GetCustomAttribute(prop, typeof(ClickHouseColumnAttribute));

        Assert.NotNull(attr);
        Assert.Equal("renamed", attr!.Name);
        Assert.Equal(0, attr.Order);
    }

    [Fact]
    public void Reflection_RecoversClickHouseTypeOverride()
    {
        var prop = typeof(Row).GetProperty(nameof(Row.At))!;
        var attr = (ClickHouseColumnAttribute?)Attribute.GetCustomAttribute(prop, typeof(ClickHouseColumnAttribute));

        Assert.NotNull(attr);
        Assert.Equal("DateTime64(3)", attr!.ClickHouseType);
    }

    [Fact]
    public void Reflection_RecoversIgnoreFlag()
    {
        var prop = typeof(Row).GetProperty(nameof(Row.Internal))!;
        var attr = (ClickHouseColumnAttribute?)Attribute.GetCustomAttribute(prop, typeof(ClickHouseColumnAttribute));

        Assert.NotNull(attr);
        Assert.True(attr!.Ignore);
    }

    [Fact]
    public void Reflection_PlainProperty_HasNoAttribute()
    {
        var prop = typeof(Row).GetProperty(nameof(Row.Plain))!;
        var attr = Attribute.GetCustomAttribute(prop, typeof(ClickHouseColumnAttribute));
        Assert.Null(attr);
    }

    [Fact]
    public void AttributeUsage_ProhibitsMultipleAndTargetsProperty()
    {
        var usage = (AttributeUsageAttribute?)Attribute.GetCustomAttribute(
            typeof(ClickHouseColumnAttribute), typeof(AttributeUsageAttribute));

        Assert.NotNull(usage);
        Assert.Equal(AttributeTargets.Property, usage!.ValidOn);
        Assert.False(usage.AllowMultiple);
    }
}

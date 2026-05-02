using CH.Native.Data;
using CH.Native.Mapping;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// <see cref="ReflectionTypedRowMapper{T}"/> is the read-side row mapper that
/// turns a column block back into POCO instances. It honours
/// <see cref="ClickHouseColumnAttribute.Name"/> for property-to-column lookup
/// (case-insensitively) and falls back to the property name. These tests pin
/// that contract so a regression in the attribute-discovery path surfaces
/// here, not as silently-empty properties on consumer reads.
/// </summary>
public class AttributeMappingTests
{
    public class RowWithAttributes
    {
        [ClickHouseColumn(Name = "user_id")]
        public int UserId { get; set; }

        [ClickHouseColumn(Name = "user_name")]
        public string Name { get; set; } = "";

        // No attribute — should map by property name.
        public bool Active { get; set; }
    }

    public class RowWithIgnoredProperty
    {
        [ClickHouseColumn(Name = "id")]
        public int Id { get; set; }

        [ClickHouseColumn(Ignore = true)]
        public string Internal { get; set; } = "computed-default";
    }

    public class BaseRow
    {
        [ClickHouseColumn(Name = "id")]
        public int Id { get; set; }
    }

    public class DerivedRow : BaseRow
    {
        [ClickHouseColumn(Name = "name")]
        public string Name { get; set; } = "";
    }

    [Fact]
    public void Mapper_ResolvesPropertiesByAttributeName()
    {
        var mapper = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "user_id", "user_name", "active" });

        var columns = new ITypedColumn[]
        {
            new TypedColumn<int>(new[] { 42 }),
            new TypedColumn<string>(new[] { "alice" }),
            new TypedColumn<bool>(new[] { true }),
        };

        var row = mapper.MapRow(columns, 0);

        Assert.Equal(42, row.UserId);
        Assert.Equal("alice", row.Name);
        Assert.True(row.Active);
    }

    [Fact]
    public void Mapper_ColumnNameLookup_IsCaseInsensitive()
    {
        // The attribute Name "user_id" should match "USER_ID" in the schema.
        var mapper = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "USER_ID", "USER_NAME", "ACTIVE" });

        var columns = new ITypedColumn[]
        {
            new TypedColumn<int>(new[] { 7 }),
            new TypedColumn<string>(new[] { "bob" }),
            new TypedColumn<bool>(new[] { false }),
        };

        var row = mapper.MapRow(columns, 0);

        Assert.Equal(7, row.UserId);
        Assert.Equal("bob", row.Name);
        Assert.False(row.Active);
    }

    [Fact]
    public void Mapper_NoMatchingProperty_LeavesDefault()
    {
        // A column in the schema with no matching property is silently
        // skipped on the read side (the matching position gets a no-op
        // setter). Pin that — it's documented behaviour for forwards
        // compatibility (server adds a column, old client keeps reading).
        var mapper = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "user_id", "extra_column_no_property", "user_name", "active" });

        var columns = new ITypedColumn[]
        {
            new TypedColumn<int>(new[] { 1 }),
            new TypedColumn<string>(new[] { "ignored" }),
            new TypedColumn<string>(new[] { "carol" }),
            new TypedColumn<bool>(new[] { true }),
        };

        var row = mapper.MapRow(columns, 0);

        Assert.Equal(1, row.UserId);
        Assert.Equal("carol", row.Name);
        Assert.True(row.Active);
    }

    [Fact]
    public void Mapper_PropertyNotInSchema_LeavesPropertyDefault()
    {
        // A property without a matching column simply isn't populated —
        // it keeps its CLR default value.
        var mapper = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "user_id" });

        var columns = new ITypedColumn[]
        {
            new TypedColumn<int>(new[] { 99 }),
        };

        var row = mapper.MapRow(columns, 0);

        Assert.Equal(99, row.UserId);
        Assert.Equal("", row.Name);  // string default
        Assert.False(row.Active);    // bool default
    }

    [Fact]
    public void Mapper_DerivedClass_PicksUpInheritedAttributes()
    {
        // BaseRow.Id has [ClickHouseColumn(Name = "id")]; DerivedRow inherits.
        var mapper = TypedRowMapperFactory.GetMapper<DerivedRow>(
            new[] { "id", "name" });

        var columns = new ITypedColumn[]
        {
            new TypedColumn<int>(new[] { 5 }),
            new TypedColumn<string>(new[] { "derived" }),
        };

        var row = mapper.MapRow(columns, 0);

        Assert.Equal(5, row.Id);
        Assert.Equal("derived", row.Name);
    }

    [Fact]
    public void Factory_CachesByColumnNameSet()
    {
        // GetMapper caches per (T, columnNames) key. Two calls with the
        // same column-name array must return the same mapper instance —
        // pin so the cache key isn't broken by a refactor.
        var a = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "user_id", "user_name", "active" });
        var b = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "user_id", "user_name", "active" });

        Assert.Same(a, b);
    }

    [Fact]
    public void Factory_DifferentColumnSets_ProduceDifferentMappers()
    {
        var a = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "user_id", "user_name", "active" });
        var b = TypedRowMapperFactory.GetMapper<RowWithAttributes>(
            new[] { "user_id", "active" });

        Assert.NotSame(a, b);
    }
}

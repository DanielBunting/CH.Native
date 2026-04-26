using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Cross-factory tests asserting that <c>Nullable(<i>composite</i>)</c> compositions
/// are rejected at construction time, matching ClickHouse's schema-level rule
/// (<c>ILLEGAL_TYPE_OF_ARGUMENT</c>, code 43, "Nested type X cannot be inside
/// Nullable type"). The integration counterpart in
/// <c>BulkInsertCompositeTypeTests.Schema_NullableComposite_IsRejectedByServer</c>
/// proves the server agrees.
/// </summary>
public class NullableInnerValidationTests
{
    public static IEnumerable<object[]> ForbiddenInnerTypes() => new[]
    {
        new object[] { "Nullable(Array(String))" },
        new object[] { "Nullable(Array(Int32))" },
        new object[] { "Nullable(Map(String, Int32))" },
        new object[] { "Nullable(Tuple(Int32, String))" },
        new object[] { "Nullable(LowCardinality(String))" },
        new object[] { "Nullable(Nested(a Int32))" },
        new object[] { "Nullable(Nullable(Int32))" },
        new object[] { "Nullable(Dynamic)" },
        new object[] { "Nullable(Variant(Int32, String))" },
    };

    public static IEnumerable<object[]> AllowedInnerTypes() => new[]
    {
        new object[] { "Nullable(String)" },
        new object[] { "Nullable(Int32)" },
        new object[] { "Nullable(Int64)" },
        new object[] { "Nullable(UUID)" },
        new object[] { "Nullable(Float64)" },
        new object[] { "Nullable(FixedString(8))" },
        new object[] { "Nullable(Decimal(10, 2))" },
        new object[] { "Nullable(Date)" },
        new object[] { "Nullable(DateTime64(3))" },
        new object[] { "Nullable(IPv4)" },
        new object[] { "Nullable(IPv6)" },
        new object[] { "Nullable(JSON)" }, // server-accepted as of CH 26.2
    };

    [Theory]
    [MemberData(nameof(ForbiddenInnerTypes))]
    public void WriterFactory_RejectsForbiddenNullableComposite(string typeName)
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateWriter(typeName));
        Assert.Contains("cannot be inside Nullable type", ex.Message);
    }

    [Theory]
    [MemberData(nameof(ForbiddenInnerTypes))]
    public void ReaderFactory_RejectsForbiddenNullableComposite(string typeName)
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateReader(typeName));
        Assert.Contains("cannot be inside Nullable type", ex.Message);
    }

    [Theory]
    [MemberData(nameof(ForbiddenInnerTypes))]
    public void SkipperFactory_RejectsForbiddenNullableComposite(string typeName)
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateSkipper(typeName));
        Assert.Contains("cannot be inside Nullable type", ex.Message);
    }

    [Theory]
    [MemberData(nameof(AllowedInnerTypes))]
    public void WriterFactory_AcceptsScalarNullable(string typeName)
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        Assert.NotNull(factory.CreateWriter(typeName));
    }

    [Theory]
    [MemberData(nameof(AllowedInnerTypes))]
    public void ReaderFactory_AcceptsScalarNullable(string typeName)
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        Assert.NotNull(factory.CreateReader(typeName));
    }

    [Theory]
    [MemberData(nameof(AllowedInnerTypes))]
    public void SkipperFactory_AcceptsScalarNullable(string typeName)
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        Assert.NotNull(factory.CreateSkipper(typeName));
    }

    [Fact]
    public void LowCardinalityOfNullable_IsStillAllowed_NotRejected()
    {
        // The inverse — LowCardinality(Nullable(...)) — is the canonical pattern in
        // ClickHouse and must continue to construct cleanly. This guards against an
        // overzealous tightening that confuses the two directions.
        var w = new ColumnWriterFactory(ColumnWriterRegistry.Default).CreateWriter("LowCardinality(Nullable(String))");
        var r = new ColumnReaderFactory(ColumnReaderRegistry.Default).CreateReader("LowCardinality(Nullable(String))");
        var s = new ColumnSkipperFactory(ColumnSkipperRegistry.Default).CreateSkipper("LowCardinality(Nullable(String))");
        Assert.NotNull(w);
        Assert.NotNull(r);
        Assert.NotNull(s);
    }

    [Fact]
    public void ArrayOfNullable_IsStillAllowed_NotRejected()
    {
        // Inverse of Nullable(Array) — must still build.
        var w = new ColumnWriterFactory(ColumnWriterRegistry.Default).CreateWriter("Array(Nullable(String))");
        var r = new ColumnReaderFactory(ColumnReaderRegistry.Default).CreateReader("Array(Nullable(String))");
        var s = new ColumnSkipperFactory(ColumnSkipperRegistry.Default).CreateSkipper("Array(Nullable(String))");
        Assert.NotNull(w);
        Assert.NotNull(r);
        Assert.NotNull(s);
    }
}

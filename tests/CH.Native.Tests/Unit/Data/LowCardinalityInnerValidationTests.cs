using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Cross-factory tests asserting that <c>LowCardinality(<i>composite</i>)</c>
/// compositions are rejected at construction time, matching ClickHouse's schema-level
/// rule. Counterpart to <see cref="NullableInnerValidationTests"/> for the
/// <c>LowCardinality</c> wrapper. The integration probe in
/// <c>BulkInsertCompositeTypeTests.Schema_LowCardinalityComposite_IsRejectedByServer</c>
/// proves the server agrees.
/// </summary>
public class LowCardinalityInnerValidationTests
{
    public static IEnumerable<object[]> ForbiddenInnerTypes() => new[]
    {
        new object[] { "LowCardinality(Array(String))" },
        new object[] { "LowCardinality(Array(Int32))" },
        new object[] { "LowCardinality(Map(String, Int32))" },
        new object[] { "LowCardinality(Tuple(Int32, String))" },
        new object[] { "LowCardinality(Nested(a Int32))" },
        new object[] { "LowCardinality(LowCardinality(String))" },
        new object[] { "LowCardinality(JSON)" },
        new object[] { "LowCardinality(Dynamic)" },
        new object[] { "LowCardinality(Variant(Int32, String))" },
    };

    public static IEnumerable<object[]> AllowedInnerTypes() => new[]
    {
        new object[] { "LowCardinality(String)" },
        new object[] { "LowCardinality(Int32)" },
        new object[] { "LowCardinality(Int64)" },
        new object[] { "LowCardinality(UUID)" },
        new object[] { "LowCardinality(Float64)" },
        new object[] { "LowCardinality(FixedString(8))" },
        new object[] { "LowCardinality(Date)" },
        new object[] { "LowCardinality(DateTime64(3))" },
        new object[] { "LowCardinality(IPv4)" },
        new object[] { "LowCardinality(IPv6)" },
        new object[] { "LowCardinality(Nullable(String))" },   // canonical form
        new object[] { "LowCardinality(Nullable(Int32))" },    // canonical form
        new object[] { "LowCardinality(Nullable(UUID))" },     // canonical form
    };

    [Theory]
    [MemberData(nameof(ForbiddenInnerTypes))]
    public void WriterFactory_RejectsForbiddenLowCardinalityComposite(string typeName)
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateWriter(typeName));
        Assert.Contains("cannot be inside LowCardinality", ex.Message);
    }

    [Theory]
    [MemberData(nameof(ForbiddenInnerTypes))]
    public void ReaderFactory_RejectsForbiddenLowCardinalityComposite(string typeName)
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateReader(typeName));
        Assert.Contains("cannot be inside LowCardinality", ex.Message);
    }

    [Theory]
    [MemberData(nameof(ForbiddenInnerTypes))]
    public void SkipperFactory_RejectsForbiddenLowCardinalityComposite(string typeName)
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateSkipper(typeName));
        Assert.Contains("cannot be inside LowCardinality", ex.Message);
    }

    [Theory]
    [MemberData(nameof(AllowedInnerTypes))]
    public void WriterFactory_AcceptsAllowedLowCardinalityInner(string typeName)
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        Assert.NotNull(factory.CreateWriter(typeName));
    }

    [Theory]
    [MemberData(nameof(AllowedInnerTypes))]
    public void ReaderFactory_AcceptsAllowedLowCardinalityInner(string typeName)
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        Assert.NotNull(factory.CreateReader(typeName));
    }

    [Theory]
    [MemberData(nameof(AllowedInnerTypes))]
    public void SkipperFactory_AcceptsAllowedLowCardinalityInner(string typeName)
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        Assert.NotNull(factory.CreateSkipper(typeName));
    }

    [Fact]
    public void ArrayOfLowCardinality_IsStillAllowed_NotRejected()
    {
        // The inverse — Array(LowCardinality(...)) — is the canonical pattern when you
        // want a low-cardinality element type inside a collection. Guards against an
        // overzealous tightening that confuses the two directions.
        var w = new ColumnWriterFactory(ColumnWriterRegistry.Default).CreateWriter("Array(LowCardinality(String))");
        var r = new ColumnReaderFactory(ColumnReaderRegistry.Default).CreateReader("Array(LowCardinality(String))");
        var s = new ColumnSkipperFactory(ColumnSkipperRegistry.Default).CreateSkipper("Array(LowCardinality(String))");
        Assert.NotNull(w);
        Assert.NotNull(r);
        Assert.NotNull(s);
    }

    [Fact]
    public void LowCardinalityOfNullableOfForbidden_IsRejected()
    {
        // The validator peeks through one Nullable layer; a forbidden composite
        // beneath it must still be rejected (e.g. LowCardinality(Nullable(Array(...)))).
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateWriter("LowCardinality(Nullable(Array(String)))"));
        // Because Nullable(Array(...)) is itself rejected by NullableInnerValidator
        // when constructed normally, but here the LowCardinality factory strips the
        // Nullable wrapper before recursing — so the LowCardinality validator must
        // catch it first.
        Assert.Contains("LowCardinality", ex.Message);
    }
}

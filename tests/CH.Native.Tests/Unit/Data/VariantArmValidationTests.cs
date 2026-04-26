using CH.Native.Data;
using Xunit;

namespace CH.Native.Tests.Unit.Data;

/// <summary>
/// Cross-factory tests asserting that <c>Variant</c> arms ClickHouse rejects at
/// schema-creation time also fail at construction time. Counterpart to
/// <see cref="NullableInnerValidationTests"/> and
/// <see cref="LowCardinalityInnerValidationTests"/>. The integration probe in
/// <c>BulkInsertCompositeTypeTests.Schema_VariantArm_IsRejectedByServer</c>
/// proves the server agrees with this list.
/// </summary>
public class VariantArmValidationTests
{
    public static IEnumerable<object[]> ForbiddenArms() => new[]
    {
        new object[] { "Variant(Variant(Int32, String), Float64)" },        // nested Variant
        new object[] { "Variant(Nullable(Int32), String)" },                // Nullable arm
        new object[] { "Variant(LowCardinality(Nullable(String)), Int32)" },// LC(Nullable) arm
        new object[] { "Variant(Dynamic, Int32)" },                         // Dynamic arm
    };

    public static IEnumerable<object[]> AllowedArms() => new[]
    {
        new object[] { "Variant(Int32, String)" },
        new object[] { "Variant(Array(Int32), String)" },
        new object[] { "Variant(Map(String, Int32), Float64)" },
        new object[] { "Variant(Tuple(Int32, String), UUID)" },
        new object[] { "Variant(JSON, String)" },
        new object[] { "Variant(LowCardinality(String), Int32)" },          // LC(scalar) is fine
        new object[] { "Variant(LowCardinality(FixedString(8)), Int32)" },
        // Note: Variant(Nested(...)) is server-accepted but the writer factory has no
        // Nested writer registered (predates this work). Skipper/reader handle it; the
        // integration probe confirms server acceptance end-to-end.
    };

    [Theory]
    [MemberData(nameof(ForbiddenArms))]
    public void WriterFactory_RejectsForbiddenVariantArm(string typeName)
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateWriter(typeName));
        Assert.Contains("Variant arm", ex.Message);
    }

    [Theory]
    [MemberData(nameof(ForbiddenArms))]
    public void ReaderFactory_RejectsForbiddenVariantArm(string typeName)
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateReader(typeName));
        Assert.Contains("Variant arm", ex.Message);
    }

    [Theory]
    [MemberData(nameof(ForbiddenArms))]
    public void SkipperFactory_RejectsForbiddenVariantArm(string typeName)
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        var ex = Assert.Throws<FormatException>(() => factory.CreateSkipper(typeName));
        Assert.Contains("Variant arm", ex.Message);
    }

    [Theory]
    [MemberData(nameof(AllowedArms))]
    public void WriterFactory_AcceptsAllowedVariantArm(string typeName)
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        Assert.NotNull(factory.CreateWriter(typeName));
    }

    [Theory]
    [MemberData(nameof(AllowedArms))]
    public void ReaderFactory_AcceptsAllowedVariantArm(string typeName)
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        Assert.NotNull(factory.CreateReader(typeName));
    }

    [Theory]
    [MemberData(nameof(AllowedArms))]
    public void SkipperFactory_AcceptsAllowedVariantArm(string typeName)
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        Assert.NotNull(factory.CreateSkipper(typeName));
    }
}

using System.Net;
using CH.Native.Commands;
using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

/// <summary>
/// Unit tests for <c>SqlLiteralFormatter</c> — the inline-rendering pass that
/// makes <c>ClickHouseQueryable.ToSql()</c> emit runnable SQL with literal
/// values instead of unfilled <c>{name:Type}</c> placeholders.
/// </summary>
public class SqlLiteralFormatterTests
{
    private static string Render(string sql, params (string name, object? value)[] parameters)
    {
        var coll = new ClickHouseParameterCollection();
        foreach (var (name, value) in parameters)
            coll.Add(new ClickHouseParameter(name, value));
        var visitor = typeof(SqlLiteralFormatter)
            .GetMethod("RenderInline", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public)!;
        return (string)visitor.Invoke(null, new object[] { sql, coll })!;
    }

    [Fact]
    public void IntLiteral_RendersInline()
    {
        Assert.Equal("WHERE x > 42", Render("WHERE x > {p1:Int32}", ("p1", 42)));
    }

    [Fact]
    public void StringLiteral_IsSingleQuotedAndEscaped()
    {
        Assert.Equal("WHERE name = 'O\\'Brien'", Render("WHERE name = {p1:String}", ("p1", "O'Brien")));
    }

    [Fact]
    public void StringLiteral_BackslashIsDoubled()
    {
        Assert.Equal("WHERE path = 'C:\\\\foo'", Render("WHERE path = {p1:String}", ("p1", @"C:\foo")));
    }

    [Fact]
    public void StringLiteral_ControlCharsEscaped()
    {
        Assert.Equal("WHERE x = 'a\\nb'", Render("WHERE x = {p1:String}", ("p1", "a\nb")));
    }

    [Fact]
    public void Bool_RendersAsZeroOrOne()
    {
        Assert.Equal("WHERE active = 1", Render("WHERE active = {p1:UInt8}", ("p1", true)));
        Assert.Equal("WHERE active = 0", Render("WHERE active = {p1:UInt8}", ("p1", false)));
    }

    [Fact]
    public void DateTime_RendersAsQuotedIsoString()
    {
        var dt = new DateTime(2024, 3, 15, 10, 30, 45, DateTimeKind.Utc);
        Assert.Equal("WHERE ts = '2024-03-15 10:30:45'",
            Render("WHERE ts = {p1:DateTime}", ("p1", dt)));
    }

    [Fact]
    public void Guid_RendersAsQuotedUuid()
    {
        var g = new Guid("01234567-89ab-cdef-0123-456789abcdef");
        Assert.Equal("WHERE id = '01234567-89ab-cdef-0123-456789abcdef'",
            Render("WHERE id = {p1:UUID}", ("p1", g)));
    }

    [Fact]
    public void IpAddress_RendersAsQuoted()
    {
        Assert.Equal("WHERE ip = '192.168.1.1'",
            Render("WHERE ip = {p1:IPv4}", ("p1", IPAddress.Parse("192.168.1.1"))));
    }

    [Fact]
    public void Float_NaN_RendersAsNan()
    {
        Assert.Equal("SELECT nan", Render("SELECT {p1:Float64}", ("p1", double.NaN)));
    }

    [Fact]
    public void Float_Infinity_RendersAsInf()
    {
        Assert.Equal("SELECT inf", Render("SELECT {p1:Float64}", ("p1", double.PositiveInfinity)));
        Assert.Equal("SELECT -inf", Render("SELECT {p1:Float64}", ("p1", double.NegativeInfinity)));
    }

    [Fact]
    public void Null_RendersAsNULL()
    {
        Assert.Equal("WHERE x IS NULL OR x = NULL",
            Render("WHERE x IS NULL OR x = {p1:Nullable(String)}", ("p1", (object?)null)));
    }

    [Fact]
    public void IntArray_RendersAsBracketLiteral()
    {
        Assert.Equal("WHERE id IN [1, 2, 3]",
            Render("WHERE id IN {p1:Array(Int32)}", ("p1", new[] { 1, 2, 3 })));
    }

    [Fact]
    public void StringArray_QuotesAndEscapesEach()
    {
        Assert.Equal("WHERE name IN ['a', 'b\\'c']",
            Render("WHERE name IN {p1:Array(String)}", ("p1", new[] { "a", "b'c" })));
    }

    [Fact]
    public void Decimal_UsesInvariantCulture()
    {
        Assert.Equal("SELECT 1234.56",
            Render("SELECT {p1:Decimal(18,2)}", ("p1", 1234.56m)));
    }

    [Fact]
    public void ClickHouseDecimal_RendersExactly()
    {
        // ClickHouseDecimal can hold values beyond CLR decimal range; the
        // formatter must use its native culture-invariant ToString rather
        // than fall through to the generic Convert.ToString path.
        var v = CH.Native.Numerics.ClickHouseDecimal.Parse(
            "1234567890123456789012345.6789",
            System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal("SELECT 1234567890123456789012345.6789",
            Render("SELECT {p1:Decimal128(4)}", ("p1", v)));
    }

    [Fact]
    public void MultiplePlaceholders_AreAllReplaced()
    {
        var sql = "WHERE x = {p1:Int32} AND y = {p2:String} AND z = {p3:UInt8}";
        Assert.Equal("WHERE x = 1 AND y = 'two' AND z = 1",
            Render(sql, ("p1", 1), ("p2", "two"), ("p3", true)));
    }

    [Fact]
    public void UnknownPlaceholder_IsLeftIntact()
    {
        // Defensive: a missing parameter should leave the placeholder visible
        // in output so the broken SQL is obvious, not silently mis-rendered.
        Assert.Equal("WHERE x = {pX:Int32}",
            Render("WHERE x = {pX:Int32}"));
    }

    [Fact]
    public void TypeWithComma_IsToleratedInPlaceholder()
    {
        // Decimal(18, 2) has a comma in the type spec — the regex must not
        // truncate the placeholder's type.
        Assert.Equal("SELECT 1.5",
            Render("SELECT {p1:Decimal(18, 2)}", ("p1", 1.5m)));
    }

    [Fact]
    public void NoParameters_PassThroughUnchanged()
    {
        Assert.Equal("SELECT * FROM t WHERE 1 = 1",
            Render("SELECT * FROM t WHERE 1 = 1"));
    }
}

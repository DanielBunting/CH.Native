using CH.Native.Ado;
using CH.Native.Dapper;
using Xunit;

namespace CH.Native.Tests.Unit.Dapper;

/// <summary>
/// Unit tests for <see cref="ClickHouseDapperArrayHandler{T}"/>. These cover the
/// direct handler contract without needing a live Dapper pipeline — the integration
/// suite in <c>DapperTests.cs</c> validates the end-to-end Dapper → ClickHouse path.
/// </summary>
public class ClickHouseDapperArrayHandlerTests
{
    [Fact]
    public void SetValue_IntArray_AssignsRawArrayToParameter()
    {
        var handler = new ClickHouseDapperArrayHandler<int>();
        var parameter = new ClickHouseDbParameter { ParameterName = "ids" };
        var arr = new[] { 1, 2, 3 };

        handler.SetValue(parameter, arr);

        // The handler must NOT expand the array — the CH.Native parameter pipeline
        // takes the CLR array and infers Array(T) on the wire. Dapper's default
        // behaviour (list-expansion into a tuple literal) is what we're bypassing.
        Assert.Same(arr, parameter.Value);
    }

    [Fact]
    public void SetValue_StringArray_AssignsRawArrayToParameter()
    {
        var handler = new ClickHouseDapperArrayHandler<string>();
        var parameter = new ClickHouseDbParameter { ParameterName = "names" };
        var arr = new[] { "Alice", "Bob", "Charlie" };

        handler.SetValue(parameter, arr);

        Assert.Same(arr, parameter.Value);
    }

    [Fact]
    public void SetValue_NullArray_AssignsDbNullToParameter()
    {
        // Dapper may hand us null for a nullable-bound property. The handler must
        // translate that to DBNull.Value so the parameter path recognises it.
        var handler = new ClickHouseDapperArrayHandler<int>();
        var parameter = new ClickHouseDbParameter { ParameterName = "ids" };

        handler.SetValue(parameter, null);

        Assert.Equal(DBNull.Value, parameter.Value);
    }

    [Fact]
    public void SetValue_EmptyArray_AssignsEmptyArray()
    {
        // An empty array is a valid Array(Int32) value on the wire (length 0). The
        // handler must not coerce it to DBNull or null.
        var handler = new ClickHouseDapperArrayHandler<int>();
        var parameter = new ClickHouseDbParameter { ParameterName = "ids" };
        var arr = Array.Empty<int>();

        handler.SetValue(parameter, arr);

        Assert.Same(arr, parameter.Value);
    }

    [Fact]
    public void Parse_ArrayValue_ReturnsCast()
    {
        // Parse is invoked when Dapper materialises a row value into a typed field.
        // The current implementation only supports pass-through casts; if the value
        // is already T[] it's returned, otherwise null. Stub behaviour, but pinning
        // it so a future refactor doesn't silently change semantics.
        var handler = new ClickHouseDapperArrayHandler<int>();
        var arr = new[] { 7, 8, 9 };

        var result = handler.Parse(arr);

        Assert.Same(arr, result);
    }

    [Fact]
    public void Parse_NonArrayValue_ReturnsNull()
    {
        var handler = new ClickHouseDapperArrayHandler<int>();

        var result = handler.Parse("not-an-array");

        Assert.Null(result);
    }

    [Fact]
    public void Parse_NullValue_ReturnsNull()
    {
        var handler = new ClickHouseDapperArrayHandler<int>();

        var result = handler.Parse(null);

        Assert.Null(result);
    }
}

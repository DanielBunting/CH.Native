using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

public class BulkInsertOptionsTests
{
    [Fact]
    public void Default_BatchSize_Is10000()
    {
        Assert.Equal(10_000, new BulkInsertOptions().BatchSize);
    }

    [Fact]
    public void Default_IncludeNullColumns_IsTrue()
    {
        Assert.True(new BulkInsertOptions().IncludeNullColumns);
    }

    [Fact]
    public void Default_UsePooledArrays_IsTrue()
    {
        Assert.True(new BulkInsertOptions().UsePooledArrays);
    }

    [Fact]
    public void Default_PreferDirectStreaming_IsTrue()
    {
        Assert.True(new BulkInsertOptions().PreferDirectStreaming);
    }

    [Fact]
    public void Default_UseSchemaCache_IsNull_InheritsFromConnection()
    {
        // null = inherit from connection setting; explicit true/false overrides.
        Assert.Null(new BulkInsertOptions().UseSchemaCache);
    }

    [Fact]
    public void Default_Roles_IsNull_InheritsFromConnection()
    {
        Assert.Null(new BulkInsertOptions().Roles);
    }

    [Fact]
    public void Default_QueryId_IsNull()
    {
        Assert.Null(new BulkInsertOptions().QueryId);
    }

    [Fact]
    public void Default_StaticInstance_IsAccessible_AndIndependent()
    {
        // The static Default doesn't have to be a singleton, but it must
        // expose default values. Pin that.
        Assert.NotNull(BulkInsertOptions.Default);
        Assert.Equal(10_000, BulkInsertOptions.Default.BatchSize);
    }

    [Fact]
    public void BatchSize_CanBeOverridden()
    {
        var opt = new BulkInsertOptions { BatchSize = 500 };
        Assert.Equal(500, opt.BatchSize);
    }

    [Fact]
    public void Roles_EmptyList_DistinctFromNull()
    {
        // Empty list = "explicit SET ROLE NONE", null = "inherit". The type
        // must allow both representations to round-trip distinctly.
        var opt = new BulkInsertOptions { Roles = new List<string>() };
        Assert.NotNull(opt.Roles);
        Assert.Empty(opt.Roles);
    }

    [Fact]
    public void Default_ColumnTypes_IsNull()
    {
        Assert.Null(new BulkInsertOptions().ColumnTypes);
    }

    [Fact]
    public void ColumnTypes_RoundTrips()
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["id"] = "Int32",
            ["name"] = "String",
        };
        var opt = new BulkInsertOptions { ColumnTypes = dict };

        Assert.Same(dict, opt.ColumnTypes);
        Assert.Equal("Int32", opt.ColumnTypes!["id"]);
        Assert.Equal("String", opt.ColumnTypes!["NAME"]); // case-insensitive lookup honored by caller's dictionary
    }
}

using System.Data;
using CH.Native.Ado;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseDbParameterContractTests
{
    [Fact]
    public void Direction_Default_IsInput()
    {
        var p = new ClickHouseDbParameter();
        Assert.Equal(ParameterDirection.Input, p.Direction);
    }

    [Theory]
    [InlineData(ParameterDirection.Output)]
    [InlineData(ParameterDirection.InputOutput)]
    [InlineData(ParameterDirection.ReturnValue)]
    public void Direction_NonInput_Throws_NotSupported(ParameterDirection direction)
    {
        var p = new ClickHouseDbParameter();
        Assert.Throws<NotSupportedException>(() => p.Direction = direction);
    }

    [Fact]
    public void Direction_Input_IsAccepted()
    {
        var p = new ClickHouseDbParameter { Direction = ParameterDirection.Input };
        Assert.Equal(ParameterDirection.Input, p.Direction);
    }

    [Fact]
    public void ParameterName_NullAssignment_StoresEmpty()
    {
        var p = new ClickHouseDbParameter { ParameterName = null! };
        Assert.Equal(string.Empty, p.ParameterName);
    }

    [Fact]
    public void SourceColumn_NullAssignment_StoresEmpty()
    {
        var p = new ClickHouseDbParameter { SourceColumn = null! };
        Assert.Equal(string.Empty, p.SourceColumn);
    }

    [Fact]
    public void Value_AcceptsNull()
    {
        var p = new ClickHouseDbParameter { Value = null };
        Assert.Null(p.Value);
    }

    [Fact]
    public void Value_AcceptsDBNull()
    {
        var p = new ClickHouseDbParameter { Value = DBNull.Value };
        Assert.Equal(DBNull.Value, p.Value);
    }

    [Fact]
    public void ResetDbType_SetsToObject()
    {
        var p = new ClickHouseDbParameter { DbType = DbType.Int32 };
        p.ResetDbType();
        Assert.Equal(DbType.Object, p.DbType);
    }

    [Theory]
    [InlineData("Int32")]
    [InlineData("String")]
    [InlineData("DateTime64(3)")]
    [InlineData("Nullable(Int64)")]
    [InlineData("Array(UInt32)")]
    [InlineData("Decimal(38, 9)")]
    public void ClickHouseType_AcceptsValidTypeStrings(string type)
    {
        var p = new ClickHouseDbParameter { ClickHouseType = type };
        Assert.Equal(type, p.ClickHouseType);
    }

    [Fact]
    public void ClickHouseType_TrimsWhitespace()
    {
        var p = new ClickHouseDbParameter { ClickHouseType = "  Int32  " };
        Assert.Equal("Int32", p.ClickHouseType);
    }

    [Fact]
    public void ClickHouseType_AssignNull_ClearsValue()
    {
        var p = new ClickHouseDbParameter { ClickHouseType = "Int32" };
        p.ClickHouseType = null;
        Assert.Null(p.ClickHouseType);
    }

    [Theory]
    [InlineData("Int32; DROP TABLE users; --")]
    [InlineData("Int32(")]
    [InlineData("(Int32)")]
    public void ClickHouseType_RejectsMalformed_AsArgument(string bad)
    {
        // Syntactically malformed type strings are blocked at the setter so
        // they never reach the wire {name:Type} placeholder. Unknown but
        // syntactically valid identifiers (e.g. "NotARealType") are not
        // checked here — that surfaces server-side as a typed exception.
        var p = new ClickHouseDbParameter();
        Assert.Throws<ArgumentException>(() => p.ClickHouseType = bad);
    }
}

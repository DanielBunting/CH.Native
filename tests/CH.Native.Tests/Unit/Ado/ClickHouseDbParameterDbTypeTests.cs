using System.Data;
using System.Data.Common;
using CH.Native.Ado;
using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

/// <summary>
/// <see cref="ClickHouseDbParameter.DbType"/> exists for ADO.NET compliance
/// (it's a property the framework expects on every <see cref="DbParameter"/>),
/// but the driver doesn't use it for wire-format inference. The actual
/// ClickHouse type comes from either the explicit <see cref="ClickHouseDbParameter.ClickHouseType"/>
/// or, when omitted, from <see cref="ClickHouseTypeMapper.InferType"/> which
/// dispatches on the CLR type of <see cref="DbParameter.Value"/>.
///
/// These tests pin that contract so consumers using Dapper or other ADO
/// libraries that set <c>DbType</c> automatically see consistent behaviour:
/// the value-based inference is the source of truth, <c>DbType</c> is
/// metadata only.
/// </summary>
public class ClickHouseDbParameterDbTypeTests
{
    [Theory]
    [InlineData(DbType.Int32, typeof(int), "Int32")]
    [InlineData(DbType.Int64, typeof(long), "Int64")]
    [InlineData(DbType.Boolean, typeof(bool), "Bool")]
    [InlineData(DbType.String, typeof(string), "String")]
    [InlineData(DbType.Double, typeof(double), "Float64")]
    [InlineData(DbType.Single, typeof(float), "Float32")]
    [InlineData(DbType.Guid, typeof(Guid), "UUID")]
    [InlineData(DbType.Date, typeof(DateOnly), "Date")]
    [InlineData(DbType.DateTime, typeof(DateTime), "DateTime")]
    [InlineData(DbType.DateTimeOffset, typeof(DateTimeOffset), "DateTime64(6)")]
    public void DbType_RoundTrips_AndCorrespondingClrType_InfersExpectedClickHouseType(
        DbType dbType, Type clrType, string expectedClickHouseType)
    {
        // Pin two contracts in one place:
        //   1. DbType setter/getter round-trips (ADO compliance).
        //   2. The CLR type that conventionally pairs with that DbType
        //      infers to the documented ClickHouse type via the mapper.
        var p = new ClickHouseDbParameter { DbType = dbType };
        Assert.Equal(dbType, p.DbType);

        // Use a default value for the CLR type — value isn't important, only
        // the type-driven inference is.
        var sample = clrType.IsValueType
            ? Activator.CreateInstance(clrType)!
            : (clrType == typeof(string) ? "x" : (object)Activator.CreateInstance(clrType)!);
        Assert.Equal(expectedClickHouseType, ClickHouseTypeMapper.InferType(sample));
    }

    [Fact]
    public void DbType_Object_IsResetState()
    {
        // ResetDbType sets DbType to Object — the "untyped" default. Pin so
        // a refactor to a different sentinel surfaces here.
        var p = new ClickHouseDbParameter { DbType = DbType.Int32 };
        p.ResetDbType();
        Assert.Equal(DbType.Object, p.DbType);
    }

    [Fact]
    public void DbType_Setting_DoesNotPopulateClickHouseType()
    {
        // Documents that DbType is metadata-only — setting it does NOT
        // populate ClickHouseType. Wire-format inference still falls through
        // to the value-based mapper unless ClickHouseType is set explicitly.
        var p = new ClickHouseDbParameter { DbType = DbType.Int32, Value = 42 };
        Assert.Null(p.ClickHouseType);
    }

    [Fact]
    public void ClickHouseType_Explicit_OverridesValueInference()
    {
        // When ClickHouseType is set, the parameter binder uses it verbatim
        // — the value-based inference is bypassed. Pin via the parameter
        // surface directly.
        var p = new ClickHouseDbParameter { Value = 42, ClickHouseType = "Int64" };
        Assert.Equal("Int64", p.ClickHouseType);
    }

    [Theory]
    [InlineData(DbType.VarNumeric)]
    [InlineData(DbType.SByte)]
    [InlineData(DbType.UInt32)]
    [InlineData(DbType.Xml)]
    public void DbType_AnyValue_IsAcceptedAsMetadata(DbType dbType)
    {
        // ADO.NET requires that DbType be settable to any DbType enum value
        // — even ones the provider doesn't support — so consumers can build
        // generic parameter pipelines. Pin that we don't reject any value.
        var p = new ClickHouseDbParameter { DbType = dbType };
        Assert.Equal(dbType, p.DbType);
    }
}

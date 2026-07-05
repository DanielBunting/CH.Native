using System.Net;
using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

/// <summary>
/// Covers the value-level inference refinements added to <see cref="ClickHouseTypeMapper"/>:
/// IPv4/IPv6 disambiguation by address family, DBNull -> Nullable(Nothing), and Tuple/ValueTuple
/// inference including the &gt;7-element TRest flattening.
/// </summary>
public class TypeMapperInferenceExtrasTests
{
    [Fact]
    public void InferType_IPv4Address_MapsToIPv4() =>
        Assert.Equal("IPv4", ClickHouseTypeMapper.InferType(IPAddress.Parse("192.168.0.1")));

    [Fact]
    public void InferType_IPv6Address_MapsToIPv6() =>
        Assert.Equal("IPv6", ClickHouseTypeMapper.InferType(IPAddress.Parse("::1")));

    [Fact]
    public void InferTypeFromClrType_IPAddress_DefaultsToIPv6() =>
        Assert.Equal("IPv6", ClickHouseTypeMapper.InferTypeFromClrType(typeof(IPAddress)));

    [Fact]
    public void InferType_DBNull_MapsToNullableNothing() =>
        Assert.Equal("Nullable(Nothing)", ClickHouseTypeMapper.InferType(DBNull.Value));

    [Fact]
    public void InferType_ValueTuple_MapsToTuple() =>
        Assert.Equal("Tuple(Int32, String)", ClickHouseTypeMapper.InferType((1, "a")));

    [Fact]
    public void InferType_NineElementValueTuple_FlattensTRest() =>
        Assert.Equal(
            "Tuple(Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32, Int32)",
            ClickHouseTypeMapper.InferType((1, 2, 3, 4, 5, 6, 7, 8, 9)));

    [Fact]
    public void InferTypeFromClrType_ReferenceTuple_MapsToTuple() =>
        Assert.Equal("Tuple(Int32, UUID)", ClickHouseTypeMapper.InferTypeFromClrType(typeof(Tuple<int, Guid>)));

    private enum SmallEnum { Red = 0, Green = 1, Blue = 2 }

    private enum WideEnum { Lo = -1000, Hi = 1000 }

    [Fact]
    public void InferType_SmallEnum_MapsToEnum8() =>
        Assert.Equal("Enum8('Red' = 0, 'Green' = 1, 'Blue' = 2)", ClickHouseTypeMapper.InferType(SmallEnum.Green));

    [Fact]
    public void InferType_WideEnum_MapsToEnum16()
    {
        // Enum.GetNames orders by unsigned value, so the negative member sorts last.
        Assert.Equal("Enum16('Hi' = 1000, 'Lo' = -1000)", ClickHouseTypeMapper.InferType(WideEnum.Hi));
    }
}

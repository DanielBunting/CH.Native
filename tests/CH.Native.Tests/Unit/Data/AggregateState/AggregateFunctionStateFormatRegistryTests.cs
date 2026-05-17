using System.Buffers;
using CH.Native.Data.AggregateState;
using CH.Native.Data.Types;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.AggregateState;

/// <summary>
/// Direct unit coverage for the three per-row wire-format implementations
/// (<see cref="FixedSizeStateFormat"/>, <see cref="VarUIntStateFormat"/>,
/// <see cref="FlagPlusFixedStateFormat"/>) and the
/// <see cref="AggregateFunctionStateFormatRegistry"/> resolver. Exercises the
/// error branches (wrong-length writes, malformed flag bytes, runaway varuints,
/// unsupported inner types) that the higher-level round-trip tests don't reach.
/// </summary>
public class AggregateFunctionStateFormatRegistryTests
{
    // --- FixedSizeStateFormat ------------------------------------------------

    [Fact]
    public void FixedSize_ReadWrite_RoundTrips_ExpectedBytes()
    {
        var format = new FixedSizeStateFormat(size: 8);
        var input = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };

        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        format.WriteOneState(ref pw, input);

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(buffer.WrittenMemory));
        var output = format.ReadOneState(ref pr);

        Assert.Equal(input, output);
    }

    [Fact]
    public void FixedSize_WriteOneState_WrongLength_ThrowsArgument()
    {
        var format = new FixedSizeStateFormat(size: 8);
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);

        // Wrong size — 3 instead of 8.
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            var local = new ProtocolWriter(buffer);
            format.WriteOneState(ref local, new byte[] { 1, 2, 3 });
        });
        Assert.Contains("8 bytes", ex.Message);
        Assert.Contains("got 3", ex.Message);
    }

    [Fact]
    public void FixedSize_TrySkipOneState_PartialBuffer_ReturnsFalse()
    {
        var format = new FixedSizeStateFormat(size: 8);
        // Only 4 bytes available; format wants 8.
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0, 0, 0, 0 }));
        Assert.False(format.TrySkipOneState(ref pr));
    }

    [Fact]
    public void FixedSize_TrySkipOneState_FullBuffer_ReturnsTrue()
    {
        var format = new FixedSizeStateFormat(size: 4);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 1, 2, 3, 4 }));
        Assert.True(format.TrySkipOneState(ref pr));
        Assert.Equal(0, pr.Remaining);
    }

    // --- VarUIntStateFormat --------------------------------------------------

    [Fact]
    public void VarUInt_ReadOneState_SingleByte_NoContinuationBit()
    {
        var format = VarUIntStateFormat.Instance;
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x01 }));
        Assert.Equal(new byte[] { 0x01 }, format.ReadOneState(ref pr));
    }

    [Fact]
    public void VarUInt_ReadOneState_TwoBytes_WithContinuationBit()
    {
        // 0x80 → continuation set; 0x01 → terminator. Standard 2-byte varuint for 128.
        var format = VarUIntStateFormat.Instance;
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x80, 0x01 }));
        Assert.Equal(new byte[] { 0x80, 0x01 }, format.ReadOneState(ref pr));
    }

    [Fact]
    public void VarUInt_ReadOneState_NineBytes_AtUpperBoundary()
    {
        // 9 bytes: 8 with continuation bit + 1 terminator — the largest legitimate VarUInt.
        var format = VarUIntStateFormat.Instance;
        var bytes = new byte[9];
        for (int i = 0; i < 8; i++) bytes[i] = 0x80;
        bytes[8] = 0x01;

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        Assert.Equal(bytes, format.ReadOneState(ref pr));
    }

    [Fact]
    public void VarUInt_ReadOneState_RunawayMissingTerminator_ThrowsFormat()
    {
        // 11 bytes all with continuation bit set — past the 10-byte safety cap.
        var format = VarUIntStateFormat.Instance;
        var bytes = new byte[11];
        for (int i = 0; i < 11; i++) bytes[i] = 0x80;

        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        var ex = Assert.Throws<FormatException>(() =>
        {
            var local = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            format.ReadOneState(ref local);
        });
        Assert.Contains("runaway", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void VarUInt_WriteOneState_EmptySpan_ThrowsArgument()
    {
        var format = VarUIntStateFormat.Instance;
        var buffer = new ArrayBufferWriter<byte>();
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            format.WriteOneState(ref pw, ReadOnlySpan<byte>.Empty);
        });
        Assert.Contains("1-10 bytes", ex.Message);
    }

    [Fact]
    public void VarUInt_WriteOneState_TooLong_ThrowsArgument()
    {
        var format = VarUIntStateFormat.Instance;
        var buffer = new ArrayBufferWriter<byte>();
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            format.WriteOneState(ref pw, new byte[11]);
        });
        Assert.Contains("1-10 bytes", ex.Message);
        Assert.Contains("got 11", ex.Message);
    }

    [Fact]
    public void VarUInt_TrySkipOneState_PartialBuffer_ReturnsFalse()
    {
        var format = VarUIntStateFormat.Instance;
        // 0x80 = continuation expected, but no following byte.
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x80 }));
        Assert.False(format.TrySkipOneState(ref pr));
    }

    [Fact]
    public void VarUInt_TrySkipOneState_Valid_ReturnsTrue()
    {
        var format = VarUIntStateFormat.Instance;
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x80, 0x01 }));
        Assert.True(format.TrySkipOneState(ref pr));
        Assert.Equal(0, pr.Remaining);
    }

    // --- FlagPlusFixedStateFormat -------------------------------------------

    [Fact]
    public void FlagPlusFixed_Read_FlagZero_ReturnsSingleByte()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x00 }));
        Assert.Equal(new byte[] { 0x00 }, format.ReadOneState(ref pr));
    }

    [Fact]
    public void FlagPlusFixed_Read_FlagOne_ReturnsFullState()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var bytes = new byte[] { 0x01, 0xAA, 0xBB, 0xCC, 0xDD };
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
        Assert.Equal(bytes, format.ReadOneState(ref pr));
    }

    [Fact]
    public void FlagPlusFixed_Read_MalformedFlag_ThrowsFormat()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var bytes = new byte[] { 0x42, 0, 0, 0, 0 };
        var ex = Assert.Throws<FormatException>(() =>
        {
            var pr = new ProtocolReader(new ReadOnlySequence<byte>(bytes));
            format.ReadOneState(ref pr);
        });
        Assert.Contains("0x42", ex.Message);
        Assert.Contains("0x00", ex.Message);
        Assert.Contains("0x01", ex.Message);
    }

    [Fact]
    public void FlagPlusFixed_Write_SingleByteZero_HappyPath()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        format.WriteOneState(ref pw, new byte[] { 0x00 });

        Assert.Equal(new byte[] { 0x00 }, buffer.WrittenMemory.ToArray());
    }

    [Fact]
    public void FlagPlusFixed_Write_SingleByteNonZero_ThrowsArgument()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var buffer = new ArrayBufferWriter<byte>();
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            format.WriteOneState(ref pw, new byte[] { 0x42 });
        });
        Assert.Contains("0x00", ex.Message);
        Assert.Contains("0x42", ex.Message);
    }

    [Fact]
    public void FlagPlusFixed_Write_MultiByteHappyPath()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var input = new byte[] { 0x01, 0xAA, 0xBB, 0xCC, 0xDD };
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        format.WriteOneState(ref pw, input);

        Assert.Equal(input, buffer.WrittenMemory.ToArray());
    }

    [Fact]
    public void FlagPlusFixed_Write_WrongLength_ThrowsArgument()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var buffer = new ArrayBufferWriter<byte>();
        // Expected 5 bytes (1 flag + 4 data) but we pass 3.
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            format.WriteOneState(ref pw, new byte[] { 0x01, 0xAA, 0xBB });
        });
        Assert.Contains("1 or 5 bytes", ex.Message);
        Assert.Contains("got 3", ex.Message);
    }

    [Fact]
    public void FlagPlusFixed_Write_MultiByteBadFlag_ThrowsArgument()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var buffer = new ArrayBufferWriter<byte>();
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            var pw = new ProtocolWriter(buffer);
            format.WriteOneState(ref pw, new byte[] { 0x42, 0xAA, 0xBB, 0xCC, 0xDD });
        });
        Assert.Contains("0x01", ex.Message);
        Assert.Contains("0x42", ex.Message);
    }

    [Fact]
    public void FlagPlusFixed_TrySkip_FlagZero_ShortCircuitsTrue()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x00 }));
        Assert.True(format.TrySkipOneState(ref pr));
        Assert.Equal(0, pr.Remaining);
    }

    [Fact]
    public void FlagPlusFixed_TrySkip_FlagOne_HappyPath()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x01, 1, 2, 3, 4 }));
        Assert.True(format.TrySkipOneState(ref pr));
        Assert.Equal(0, pr.Remaining);
    }

    [Fact]
    public void FlagPlusFixed_TrySkip_EmptyBuffer_ReturnsFalse()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(Array.Empty<byte>()));
        Assert.False(format.TrySkipOneState(ref pr));
    }

    [Fact]
    public void FlagPlusFixed_TrySkip_BadFlag_ReturnsFalse()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x42, 1, 2, 3, 4 }));
        Assert.False(format.TrySkipOneState(ref pr));
    }

    [Fact]
    public void FlagPlusFixed_TrySkip_FlagOneButTruncated_ReturnsFalse()
    {
        var format = new FlagPlusFixedStateFormat(innerSize: 4);
        // Flag says "has value" but only 2 of 4 data bytes provided.
        var pr = new ProtocolReader(new ReadOnlySequence<byte>(new byte[] { 0x01, 1, 2 }));
        Assert.False(format.TrySkipOneState(ref pr));
    }

    // --- AggregateFunctionStateFormatRegistry.Resolve -----------------------

    public static IEnumerable<object[]> SumSizesPerInnerType => new[]
    {
        new object[] { "Int8", 8 },     new object[] { "Int16", 8 },
        new object[] { "Int32", 8 },    new object[] { "Int64", 8 },
        new object[] { "UInt8", 8 },    new object[] { "UInt16", 8 },
        new object[] { "UInt32", 8 },   new object[] { "UInt64", 8 },
        new object[] { "Float32", 8 },  new object[] { "Float64", 8 },
        new object[] { "Int128", 16 },  new object[] { "UInt128", 16 },
        new object[] { "Int256", 32 },  new object[] { "UInt256", 32 },
        new object[] { "Decimal32", 16 }, new object[] { "Decimal64", 16 },
        new object[] { "Decimal128", 16 }, new object[] { "Decimal256", 32 },
    };

    [Theory]
    [MemberData(nameof(SumSizesPerInnerType))]
    public void Resolve_Sum_ReturnsFixedSizeFormat_ForExpectedWidth(string innerBaseName, int expectedSize)
    {
        var inner = new ClickHouseType(innerBaseName);
        var format = AggregateFunctionStateFormatRegistry.Resolve("sum", new[] { inner });

        var fixedSize = Assert.IsType<FixedSizeStateFormat>(format);
        // Verify size indirectly via round-trip (no public size getter).
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        fixedSize.WriteOneState(ref pw, new byte[expectedSize]);
        Assert.Equal(expectedSize, buffer.WrittenCount);
    }

    public static IEnumerable<object[]> SingleValueSizesPerInnerType => new[]
    {
        new object[] { "min", "Bool", 1 },     new object[] { "min", "Int8", 1 },
        new object[] { "max", "UInt8", 1 },
        new object[] { "any", "Int16", 2 },    new object[] { "any", "UInt16", 2 },
        new object[] { "anyLast", "Date", 2 },
        new object[] { "min", "Int32", 4 },    new object[] { "min", "UInt32", 4 },
        new object[] { "min", "Float32", 4 },  new object[] { "min", "DateTime", 4 },
        new object[] { "min", "Date32", 4 },
        new object[] { "max", "Int64", 8 },    new object[] { "max", "UInt64", 8 },
        new object[] { "max", "Float64", 8 },  new object[] { "max", "DateTime64", 8 },
        new object[] { "min", "Int128", 16 },  new object[] { "min", "UInt128", 16 },
        new object[] { "min", "UUID", 16 },
        new object[] { "min", "Int256", 32 },  new object[] { "min", "UInt256", 32 },
    };

    [Theory]
    [MemberData(nameof(SingleValueSizesPerInnerType))]
    public void Resolve_SingleValue_ReturnsFlagPlusFixedFormat_ForExpectedInnerSize(
        string fn, string innerBaseName, int expectedInnerSize)
    {
        var inner = new ClickHouseType(innerBaseName);
        var format = AggregateFunctionStateFormatRegistry.Resolve(fn, new[] { inner });

        var flagPlusFixed = Assert.IsType<FlagPlusFixedStateFormat>(format);
        // Indirect size verification via round-trip with a synthesized flag+data state.
        var stateBytes = new byte[1 + expectedInnerSize];
        stateBytes[0] = 0x01;
        var buffer = new ArrayBufferWriter<byte>();
        var pw = new ProtocolWriter(buffer);
        flagPlusFixed.WriteOneState(ref pw, stateBytes);
        Assert.Equal(1 + expectedInnerSize, buffer.WrittenCount);
    }

    [Fact]
    public void Resolve_Count_ReturnsVarUIntFormat()
    {
        var format = AggregateFunctionStateFormatRegistry.Resolve("count", Array.Empty<ClickHouseType>());
        Assert.IsType<VarUIntStateFormat>(format);
    }

    [Fact]
    public void Resolve_Sum_WrongArgCount_ThrowsFormat()
    {
        var ex = Assert.Throws<FormatException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve("sum", Array.Empty<ClickHouseType>()));
        Assert.Contains("exactly one inner type", ex.Message);
        Assert.Contains("got 0", ex.Message);
    }

    [Fact]
    public void Resolve_SingleValue_WrongArgCount_ThrowsFormat()
    {
        var ex = Assert.Throws<FormatException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve("min",
                new[] { new ClickHouseType("Int32"), new ClickHouseType("Int64") }));
        Assert.Contains("exactly one inner type", ex.Message);
        Assert.Contains("got 2", ex.Message);
    }

    [Fact]
    public void Resolve_Sum_NullableInner_ThrowsNotSupported_WithReason()
    {
        var nullableInt32 = new ClickHouseType("Nullable",
            typeArguments: new[] { new ClickHouseType("Int32") });
        var ex = Assert.Throws<NotSupportedException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve("sum", new[] { nullableInt32 }));
        Assert.Contains("Nullable", ex.Message);
        Assert.Contains("null bitmap", ex.Message);
    }

    [Theory]
    [InlineData("min")]
    [InlineData("max")]
    [InlineData("any")]
    [InlineData("anyLast")]
    public void Resolve_SingleValue_NullableInner_ThrowsNotSupported_WithReason(string fn)
    {
        var nullableInt32 = new ClickHouseType("Nullable",
            typeArguments: new[] { new ClickHouseType("Int32") });
        var ex = Assert.Throws<NotSupportedException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve(fn, new[] { nullableInt32 }));
        Assert.Contains("Nullable", ex.Message);
        Assert.Contains("null bitmap", ex.Message);
    }

    [Fact]
    public void Resolve_Sum_UnsupportedInnerType_ThrowsNotSupported()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve("sum", new[] { new ClickHouseType("String") }));
        Assert.Contains("sum", ex.Message);
        Assert.Contains("String", ex.Message);
        Assert.Contains("tier-1 set", ex.Message);
    }

    [Fact]
    public void Resolve_SingleValue_UnsupportedInnerType_ThrowsNotSupported()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve("min", new[] { new ClickHouseType("String") }));
        Assert.Contains("min", ex.Message);
        Assert.Contains("String", ex.Message);
        Assert.Contains("tier-1 set", ex.Message);
    }

    [Fact]
    public void Resolve_UnknownFunction_ThrowsNotSupported_WithWorkaroundMessage()
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve(
                "quantilesTDigest", new[] { new ClickHouseType("Float64") }));
        Assert.Contains("quantilesTDigest", ex.Message);
        Assert.Contains("finalizeAggregation", ex.Message);
        Assert.Contains("hex(", ex.Message);
        Assert.Contains("Float64", ex.Message);
    }

    [Fact]
    public void Resolve_UnknownFunction_NoArgs_RendersBareSignature()
    {
        // Exercises the `args.Length > 0 ? ... : ...` branch in Unsupported() — the
        // false arm produces "AggregateFunction(funcName)" without the args list.
        var ex = Assert.Throws<NotSupportedException>(() =>
            AggregateFunctionStateFormatRegistry.Resolve("mysteryFn", Array.Empty<ClickHouseType>()));
        Assert.Contains("AggregateFunction(mysteryFn)", ex.Message);
        // No trailing args clause when typeArguments is empty.
        Assert.DoesNotContain("AggregateFunction(mysteryFn,", ex.Message);
    }
}

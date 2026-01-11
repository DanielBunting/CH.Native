using System.Buffers;
using CH.Native.Exceptions;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol.Messages;

public class ServerHelloTests
{
    [Fact]
    public void Read_ModernProtocol_IncludesTimezoneAndDisplayName()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(0); // Message type Hello
        writer.WriteString("ClickHouse");
        writer.WriteVarInt(24); // Version major
        writer.WriteVarInt(1);  // Version minor
        writer.WriteVarInt(54467); // Protocol revision (modern)
        writer.WriteString("UTC");
        writer.WriteString("clickhouse-server");
        writer.WriteVarInt(0); // Version patch (>= 54401)
        writer.WriteVarInt(0); // Password complexity rules count (>= 54461)
        writer.WriteUInt64(0); // Nonce (>= 54462)

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);
        var hello = ServerHello.Read(ref reader);

        Assert.Equal("ClickHouse", hello.ServerName);
        Assert.Equal(24, hello.VersionMajor);
        Assert.Equal(1, hello.VersionMinor);
        Assert.Equal(54467, hello.ProtocolRevision);
        Assert.Equal("UTC", hello.Timezone);
        Assert.Equal("clickhouse-server", hello.DisplayName);
    }

    [Fact]
    public void Read_OldProtocol_NoTimezoneOrDisplayName()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(0); // Message type Hello
        writer.WriteString("ClickHouse");
        writer.WriteVarInt(18); // Old version
        writer.WriteVarInt(0);
        writer.WriteVarInt(54400); // Old protocol (before timezone)

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);
        var hello = ServerHello.Read(ref reader);

        Assert.Equal("ClickHouse", hello.ServerName);
        Assert.Equal(18, hello.VersionMajor);
        Assert.Equal(0, hello.VersionMinor);
        Assert.Equal(54400, hello.ProtocolRevision);
        Assert.Null(hello.Timezone);
        Assert.Null(hello.DisplayName);
    }

    [Fact]
    public void Read_ExactlyAtTimezoneRevision_IncludesTimezone()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(0); // Message type Hello
        writer.WriteString("ClickHouse");
        writer.WriteVarInt(21);
        writer.WriteVarInt(8);
        writer.WriteVarInt(ProtocolVersion.WithTimezone); // Exactly at the boundary (54423)
        writer.WriteString("America/New_York");
        writer.WriteString("my-clickhouse");
        writer.WriteVarInt(0); // Version patch (>= 54401)
        // No password complexity rules or nonce (< 54461)

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);
        var hello = ServerHello.Read(ref reader);

        Assert.Equal(ProtocolVersion.WithTimezone, hello.ProtocolRevision);
        Assert.Equal("America/New_York", hello.Timezone);
        Assert.Equal("my-clickhouse", hello.DisplayName);
    }

    [Fact]
    public void Read_ExceptionMessageType_ThrowsConnectionExceptionWithDetails()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        // Write a complete exception message
        writer.WriteVarInt(2); // Exception message type
        writer.WriteInt32(516); // Error code
        writer.WriteString("DB::Exception"); // Exception name
        writer.WriteString("Authentication failed"); // Error message
        writer.WriteString("stack trace here"); // Stack trace
        writer.WriteByte(0); // No nested exception

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);

        ClickHouseConnectionException? caught = null;
        try
        {
            ServerHello.Read(ref reader);
        }
        catch (ClickHouseConnectionException ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.Contains("516", caught.Message);
        Assert.Contains("Authentication failed", caught.Message);
    }

    [Fact]
    public void Read_DataMessageType_ThrowsConnectionException()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(1); // Data message type

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);

        ClickHouseConnectionException? caught = null;
        try
        {
            ServerHello.Read(ref reader);
        }
        catch (ClickHouseConnectionException ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        Assert.Contains("Expected ServerHello", caught.Message);
    }

    [Fact]
    public void Read_EmptyServerName_ReadsCorrectly()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(0);
        writer.WriteString(""); // Empty server name
        writer.WriteVarInt(24);
        writer.WriteVarInt(1);
        writer.WriteVarInt(54467);
        writer.WriteString("UTC");
        writer.WriteString("");
        writer.WriteVarInt(0); // Version patch (>= 54401)
        writer.WriteVarInt(0); // Password complexity rules count (>= 54461)
        writer.WriteUInt64(0); // Nonce (>= 54462)

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);
        var hello = ServerHello.Read(ref reader);

        Assert.Equal("", hello.ServerName);
        Assert.Equal("", hello.DisplayName);
    }

    [Fact]
    public void Read_ConsumesCorrectBytes()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(0);
        writer.WriteString("ClickHouse");
        writer.WriteVarInt(24);
        writer.WriteVarInt(1);
        writer.WriteVarInt(54467);
        writer.WriteString("UTC");
        writer.WriteString("server");
        writer.WriteVarInt(0); // Version patch (>= 54401)
        writer.WriteVarInt(0); // Password complexity rules count (>= 54461)
        writer.WriteUInt64(0); // Nonce (>= 54462)

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);
        ServerHello.Read(ref reader);

        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void Read_UnicodeTimezone_ReadsCorrectly()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(0);
        writer.WriteString("ClickHouse");
        writer.WriteVarInt(24);
        writer.WriteVarInt(1);
        writer.WriteVarInt(54467);
        writer.WriteString("Europe/Moscow");
        writer.WriteString("test-\u4e2d\u6587"); // Chinese characters
        writer.WriteVarInt(0); // Version patch (>= 54401)
        writer.WriteVarInt(0); // Password complexity rules count (>= 54461)
        writer.WriteUInt64(0); // Nonce (>= 54462)

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);
        var hello = ServerHello.Read(ref reader);

        Assert.Equal("Europe/Moscow", hello.Timezone);
        Assert.Equal("test-\u4e2d\u6587", hello.DisplayName);
    }
}

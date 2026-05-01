using System.Buffers;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol.Messages;

/// <summary>
/// Pre-fix the chunked-packets capability fields the server emits at protocol
/// revision 54470+ were read into <c>_</c> and discarded. CH.Native always
/// advertises <c>"notchunked"</c>, so a server that only accepts
/// <c>"chunked"</c> would silently desync the wire as the client kept
/// emitting un-chunked frames. The fix surfaces the server's declaration on
/// <see cref="ServerHello"/> so the connection can refuse a chunked-only
/// server up-front (validated separately in
/// <c>ClickHouseConnection.PerformHandshakeAsync</c>).
/// </summary>
public class ServerHelloChunkedCapabilityTests
{
    private const int RevWithChunked = 54470;

    [Fact]
    public void Read_ChunkedRevision_CapturesServerDeclaration()
    {
        var bytes = BuildHello(
            protocolRevision: RevWithChunked,
            timezone: "UTC",
            displayName: "clickhouse-server",
            includeVersionPatch: true,
            sendChunked: "notchunked",
            recvChunked: "chunked_optional");

        var seq = new ReadOnlySequence<byte>(bytes);
        var reader = new ProtocolReader(seq);
        var hello = ServerHello.Read(ref reader);

        Assert.Equal("notchunked", hello.ProtoSendChunkedServer);
        Assert.Equal("chunked_optional", hello.ProtoRecvChunkedServer);
    }

    [Fact]
    public void Read_OldRevision_LeavesChunkedFieldsNull()
    {
        var bytes = BuildHello(
            protocolRevision: 54467,
            timezone: "UTC",
            displayName: "clickhouse-server",
            includeVersionPatch: true,
            sendChunked: null,
            recvChunked: null);

        var seq = new ReadOnlySequence<byte>(bytes);
        var reader = new ProtocolReader(seq);
        var hello = ServerHello.Read(ref reader);

        Assert.Null(hello.ProtoSendChunkedServer);
        Assert.Null(hello.ProtoRecvChunkedServer);
    }

    [Fact]
    public void Read_ChunkedOnlyServer_DeclarationIsExposed()
    {
        // The actual handshake-time refusal is in ClickHouseConnection;
        // this test pins that the raw declaration is faithfully captured
        // so callers (or future strict-mode policies) can act on it.
        var bytes = BuildHello(
            protocolRevision: RevWithChunked,
            timezone: "UTC",
            displayName: "clickhouse-server",
            includeVersionPatch: true,
            sendChunked: "chunked",
            recvChunked: "chunked");

        var seq = new ReadOnlySequence<byte>(bytes);
        var reader = new ProtocolReader(seq);
        var hello = ServerHello.Read(ref reader);

        Assert.Equal("chunked", hello.ProtoSendChunkedServer);
        Assert.Equal("chunked", hello.ProtoRecvChunkedServer);
    }

    private static byte[] BuildHello(
        int protocolRevision,
        string? timezone,
        string? displayName,
        bool includeVersionPatch,
        string? sendChunked,
        string? recvChunked)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);

        writer.WriteVarInt(0); // Message type Hello
        writer.WriteString("ClickHouse");
        writer.WriteVarInt(24);
        writer.WriteVarInt(8);
        writer.WriteVarInt((ulong)protocolRevision);

        // 54471 is parallel-replicas; we keep this test below 54471 to skip
        if (protocolRevision >= 54471)
        {
            writer.WriteVarInt(0);
        }

        if (protocolRevision >= 54423 && timezone is not null)
        {
            writer.WriteString(timezone);
            writer.WriteString(displayName ?? "");
        }

        if (includeVersionPatch && protocolRevision >= 54401)
            writer.WriteVarInt(0); // version patch

        if (protocolRevision >= 54470 && sendChunked is not null)
        {
            writer.WriteString(sendChunked);
            writer.WriteString(recvChunked ?? "notchunked");
        }

        if (protocolRevision >= 54461)
            writer.WriteVarInt(0); // password complexity rules count
        if (protocolRevision >= 54462)
            writer.WriteUInt64(0); // nonce

        return buffer.WrittenMemory.ToArray();
    }
}

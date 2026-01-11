using System.Buffers;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using Xunit;

namespace CH.Native.Tests.Unit.Protocol.Messages;

public class ClientHelloTests
{
    [Fact]
    public void Write_ProducesCorrectFormat()
    {
        var hello = new ClientHello
        {
            ClientName = "CH.Native",
            VersionMajor = 1,
            VersionMinor = 0,
            ProtocolRevision = 54467,
            Database = "default",
            Username = "default",
            Password = ""
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        hello.Write(ref writer);

        // Verify message type is 0x00
        Assert.Equal(0x00, buffer.WrittenSpan[0]);

        // Read back and verify structure
        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);

        Assert.Equal(0UL, reader.ReadVarInt()); // Message type
        Assert.Equal("CH.Native", reader.ReadString());
        Assert.Equal(1UL, reader.ReadVarInt()); // Version major
        Assert.Equal(0UL, reader.ReadVarInt()); // Version minor
        Assert.Equal(54467UL, reader.ReadVarInt()); // Protocol revision
        Assert.Equal("default", reader.ReadString());
        Assert.Equal("default", reader.ReadString());
        Assert.Equal("", reader.ReadString());
    }

    [Fact]
    public void Create_ReturnsCorrectDefaults()
    {
        var hello = ClientHello.Create("MyClient", "mydb", "admin", "secret");

        Assert.Equal("MyClient", hello.ClientName);
        Assert.Equal(1, hello.VersionMajor);
        Assert.Equal(0, hello.VersionMinor);
        Assert.Equal(ProtocolVersion.Current, hello.ProtocolRevision);
        Assert.Equal("mydb", hello.Database);
        Assert.Equal("admin", hello.Username);
        Assert.Equal("secret", hello.Password);
    }

    [Fact]
    public void Write_WithUnicodeDatabase_EncodesCorrectly()
    {
        var hello = new ClientHello
        {
            ClientName = "CH.Native",
            VersionMajor = 1,
            VersionMinor = 0,
            ProtocolRevision = 54467,
            Database = "test_\u4e2d\u6587", // Chinese characters
            Username = "default",
            Password = ""
        };

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        hello.Write(ref writer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);

        reader.ReadVarInt(); // Skip message type
        reader.ReadString(); // Skip client name
        reader.ReadVarInt(); // Skip version major
        reader.ReadVarInt(); // Skip version minor
        reader.ReadVarInt(); // Skip protocol revision

        Assert.Equal("test_\u4e2d\u6587", reader.ReadString());
    }

    [Fact]
    public void Write_WithPassword_EncodesPassword()
    {
        var hello = ClientHello.Create("CH.Native", "default", "admin", "my_secret_password");

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        hello.Write(ref writer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);

        reader.ReadVarInt(); // Skip message type
        reader.ReadString(); // Skip client name
        reader.ReadVarInt(); // Skip version major
        reader.ReadVarInt(); // Skip version minor
        reader.ReadVarInt(); // Skip protocol revision
        reader.ReadString(); // Skip database
        reader.ReadString(); // Skip username

        Assert.Equal("my_secret_password", reader.ReadString());
    }

    [Fact]
    public void Write_ConsumesAllBytes()
    {
        var hello = ClientHello.Create("CH.Native", "default", "default", "");

        var buffer = new ArrayBufferWriter<byte>();
        var writer = new ProtocolWriter(buffer);
        hello.Write(ref writer);

        var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
        var reader = new ProtocolReader(sequence);

        reader.ReadVarInt(); // Message type
        reader.ReadString(); // Client name
        reader.ReadVarInt(); // Version major
        reader.ReadVarInt(); // Version minor
        reader.ReadVarInt(); // Protocol revision
        reader.ReadString(); // Database
        reader.ReadString(); // Username
        reader.ReadString(); // Password

        Assert.Equal(0, reader.Remaining);
    }
}

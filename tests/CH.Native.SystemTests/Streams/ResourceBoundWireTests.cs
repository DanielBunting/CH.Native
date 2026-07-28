using System.Buffers;
using System.Text;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Wire-level resource-exhaustion pins (unit-level siblings live in
/// CH.Native.Tests/Unit/Data/ResourceBoundBlockTests). A server-controlled
/// column TYPE NAME is fed to three recursive-descent routines (type parser,
/// reader factory, skipper factory); pathological nesting must surface as a
/// catchable, connection-poisoning failure — never a StackOverflowException,
/// which kills the process uncatchably.
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class ResourceBoundWireTests
{
    private static readonly TimeSpan AntiHang = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task DeeplyNestedTypeName_OverWire_FailsCatchably_NeverStackOverflow()
    {
        // Array( × 4000 — far beyond the parser's depth cap (100), far below
        // any string-length cap. Pre-cap this recursed once per level in three
        // separate routines and crashed the test host.
        const int depth = 4000;
        var type = new StringBuilder(depth * 7 + 16);
        for (int i = 0; i < depth; i++) type.Append("Array(");
        type.Append("Int32");
        type.Append(')', depth);

        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);       // table name
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(1);                  // columns
        w.WriteVarInt(1);                  // rows
        w.WriteString("c");
        w.WriteString(type.ToString());

        await using var mock = new MockClickHouseServer();
        mock.Start();
        await using var conn = new ClickHouseConnection(mock.BuildSettings());
        await conn.OpenAsync();
        await mock.HandshakeCompleted;

        mock.EnqueueBytes(bw.WrittenMemory.Span);
        mock.CompleteOutgoing();

        var query = Task.Run(async () =>
        {
            await foreach (var _ in conn.QueryStreamAsync<int>("SELECT 1")) { }
        });
        var winner = await Task.WhenAny(query, Task.Delay(AntiHang));
        Assert.True(winner == query, "Deeply nested type name must terminate parsing, not hang.");

        // Any catchable failure is acceptable; the process surviving IS the pin.
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => query);
        Assert.False(conn.CanBePooled,
            $"Connection must be poisoned after an unparseable schema (threw {ex.GetType().Name}).");
    }
}

using System.Buffers;
using CH.Native.Connection;
using CH.Native.Data;
using CH.Native.Protocol;
using CH.Native.Protocol.Messages;
using CH.Native.SystemTests.Fixtures;
using Xunit;
using Xunit.Abstractions;

namespace CH.Native.SystemTests.Streams;

/// <summary>
/// Seeded (fully deterministic — no flake) fuzz smoke for the server-message
/// scanner. Feeds mutated variants of valid response byte streams through a
/// <see cref="MockClickHouseServer"/> and asserts the ONLY acceptable outcomes:
/// the query completes, or throws a catchable exception, within the anti-hang
/// budget — never a hang, never a process crash.
///
/// <para>Earned its place: this session found TWO uncatchable
/// StackOverflowExceptions in this parse path (per-message tail recursion,
/// unbounded type-name recursion). A crash here kills the test host, which xUnit
/// reports loudly — exactly the detection we want for the third one.</para>
/// </summary>
[Trait(Categories.Name, Categories.Streams)]
public class ScannerFuzzSmokeTests
{
    private static readonly TimeSpan AntiHang = TimeSpan.FromSeconds(5);
    private const int Seed = 20260721; // fixed: identical corpus every run
    private const int CasesPerCorpus = 24;

    private readonly ITestOutputHelper _output;
    public ScannerFuzzSmokeTests(ITestOutputHelper output) => _output = output;

    /// <summary>Valid byte streams to mutate.</summary>
    private static byte[][] Corpus() => new[]
    {
        BuildEmptyBlockPlusEos(),
        BuildDataBlockPlusEos(),
        BuildProgressPlusEos(),
    };

    private static byte[] BuildEmptyBlockPlusEos()
    {
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);
        w.WriteVarInt(0);
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }

    private static byte[] BuildDataBlockPlusEos()
    {
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(1);           // columns
        w.WriteVarInt(2);           // rows
        w.WriteString("n");
        w.WriteString("Int32");
        w.WriteInt32(7);
        w.WriteInt32(9);
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }

    private static byte[] BuildProgressPlusEos()
    {
        var bw = new ArrayBufferWriter<byte>();
        var w = new ProtocolWriter(bw);
        w.WriteVarInt((ulong)ServerMessageType.Data);
        w.WriteString(string.Empty);
        BlockInfo.Default.Write(ref w);
        w.WriteVarInt(0);
        w.WriteVarInt(0);
        w.WriteVarInt((ulong)ServerMessageType.EndOfStream);
        return bw.WrittenMemory.ToArray();
    }

    private static byte[] Mutate(byte[] source, Random rng)
    {
        var bytes = (byte[])source.Clone();
        switch (rng.Next(4))
        {
            case 0: // flip 1-4 random bytes
                for (int i = 0, n = rng.Next(1, 5); i < n; i++)
                    bytes[rng.Next(bytes.Length)] ^= (byte)(1 << rng.Next(8));
                return bytes;
            case 1: // truncate at a random point
                return bytes[..rng.Next(1, bytes.Length)];
            case 2: // random garbage prefix (desync)
                var prefix = new byte[rng.Next(1, 8)];
                rng.NextBytes(prefix);
                return prefix.Concat(bytes).ToArray();
            default: // splice: duplicate a random slice mid-stream
                var at = rng.Next(bytes.Length);
                var len = rng.Next(1, Math.Min(16, bytes.Length));
                var slice = bytes.Skip(Math.Max(0, at - len)).Take(len).ToArray();
                return bytes.Take(at).Concat(slice).Concat(bytes.Skip(at)).ToArray();
        }
    }

    [Fact]
    public async Task MutatedServerStreams_NeverHangOrCrashTheScanner()
    {
        var rng = new Random(Seed);
        var corpus = Corpus();
        int completed = 0, threw = 0;

        for (int c = 0; c < corpus.Length; c++)
        {
            for (int i = 0; i < CasesPerCorpus; i++)
            {
                var mutated = Mutate(corpus[c], rng);

                await using var mock = new MockClickHouseServer();
                mock.Start();
                await using var conn = new ClickHouseConnection(mock.BuildSettings());
                await conn.OpenAsync();
                await mock.HandshakeCompleted;

                mock.EnqueueBytes(mutated);
                mock.CompleteOutgoing(); // socket closes after the bytes — no waiting-for-more hangs

                var query = Task.Run(async () =>
                {
                    await foreach (var _ in conn.QueryStreamAsync<int>("SELECT 1")) { }
                });
                var winner = await Task.WhenAny(query, Task.Delay(AntiHang));
                Assert.True(winner == query,
                    $"HANG: corpus {c} case {i} (seed {Seed}) did not terminate within {AntiHang}.");

                try { await query; completed++; }
                catch (Exception ex) when (ex is not Xunit.Sdk.XunitException) { threw++; }
                // Any catchable exception is acceptable; a StackOverflow/crash
                // kills the host and fails the run loudly, which is the target
                // detection. No postcondition on reuse — a mutated stream may
                // legitimately poison; the invariant here is termination.
            }
        }

        _output.WriteLine($"fuzz cases: {corpus.Length * CasesPerCorpus} (completed={completed}, threw={threw})");
    }
}

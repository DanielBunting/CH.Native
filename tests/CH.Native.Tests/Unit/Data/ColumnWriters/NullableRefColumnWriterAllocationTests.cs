using System.Buffers;
using CH.Native.Data;
using CH.Native.Data.ColumnWriters;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnWriters;

/// <summary>
/// Allocation guard for the no-null fast path on <see cref="NullableRefColumnWriter{T}"/>.
/// The substitution path was added to support strict inner writers (Bug 3 follow-up);
/// it must not regress the common no-null case by allocating a substituted T[]
/// each flush. The fast path returns immediately after the bitmap walk and
/// passes the original array through to the inner.
/// </summary>
public class NullableRefColumnWriterAllocationTests
{
    [Fact]
    public void WriteColumn_NoNulls_AllocatesLessThanSubstitutionPath()
    {
        // Comparative measurement: the fast path (no nulls, pass-through to
        // inner) should allocate strictly less than the substitution path
        // (one or more nulls present, allocates a fresh T[]). At 1024 rows
        // the substituted string[1024] is ~8 KB on its own, so the gap
        // dwarfs the unrelated per-call noise (ProtocolWriter scratch,
        // ArrayBufferWriter storage). A regression that removed the fast
        // path would make these two numbers converge.
        const int rowCount = 1024;
        var sut = new NullableRefColumnWriter<string>(new StringColumnWriter());

        var noNulls = new string[rowCount];
        for (int i = 0; i < rowCount; i++) noNulls[i] = "x";

        var withOneNull = (string?[])noNulls.Clone();
        withOneNull[0] = null;

        // Warm up both paths to JIT and prime any one-time allocations.
        Run(sut, noNulls);
        Run(sut, withOneNull);

        var fastBefore = GC.GetAllocatedBytesForCurrentThread();
        Run(sut, noNulls);
        var fastAfter = GC.GetAllocatedBytesForCurrentThread();
        var fast = fastAfter - fastBefore;

        var slowBefore = GC.GetAllocatedBytesForCurrentThread();
        Run(sut, withOneNull);
        var slowAfter = GC.GetAllocatedBytesForCurrentThread();
        var slow = slowAfter - slowBefore;

        // The slow path allocates ~8 KB of substituted string[] in addition
        // to whatever the fast path allocates. Demand at least a 4 KB gap to
        // guard against a regression while leaving plenty of headroom for
        // unrelated noise.
        Assert.True(slow > fast + 4096,
            $"Substitution path ({slow}B) should allocate substantially more than the fast path ({fast}B).");
    }

    private static void Run(NullableRefColumnWriter<string> sut, string?[] values)
    {
        var buffer = new ArrayBufferWriter<byte>(values.Length * 2);
        var writer = new ProtocolWriter(buffer);
        sut.WriteColumn(ref writer, values);
    }

    [Fact]
    public void WriteColumn_NoNulls_PassesSameArrayToInner()
    {
        // Verify by behaviour: with no nulls, the wire output of NullableRefColumnWriter
        // with a strict StringColumnWriter inner equals the bitmap bytes followed
        // by exactly what StringColumnWriter would write directly.
        var values = new[] { "x", "y" };

        var nullableBuffer = new ArrayBufferWriter<byte>();
        var nw = new ProtocolWriter(nullableBuffer);
        new NullableRefColumnWriter<string>(new StringColumnWriter())
            .WriteColumn(ref nw, values);

        var bareBuffer = new ArrayBufferWriter<byte>();
        var bw = new ProtocolWriter(bareBuffer);
        new StringColumnWriter().WriteColumn(ref bw, values);

        // 2 bitmap zero-bytes + bare String wire output.
        var nullableSpan = nullableBuffer.WrittenSpan;
        Assert.Equal(2 + bareBuffer.WrittenCount, nullableSpan.Length);
        Assert.Equal(0x00, nullableSpan[0]);
        Assert.Equal(0x00, nullableSpan[1]);
        Assert.True(nullableSpan[2..].SequenceEqual(bareBuffer.WrittenSpan));
    }
}

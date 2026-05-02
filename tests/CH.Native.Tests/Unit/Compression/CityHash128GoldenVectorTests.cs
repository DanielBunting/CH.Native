using CH.Native.Compression;
using Xunit;

namespace CH.Native.Tests.Unit.Compression;

/// <summary>
/// Locks in known-good <see cref="CityHash128"/> digests for a range of input
/// lengths that exercise every internal code path (HashLen0to16, CityMurmur,
/// CityHash128WithSeed main loop, tail loop). End-to-end compression
/// integration tests already prove the implementation matches the bytes
/// ClickHouse expects on the wire — these vectors lock the algorithm against
/// silent drift if anyone refactors the internals (e.g. the audit-flagged tail
/// loop offset arithmetic).
/// </summary>
/// <remarks>
/// Vectors were captured from the current implementation at the time the
/// integration tests were green. If a change here also fails the integration
/// suite, the change broke the wire contract; if these fail but integration
/// passes, the algorithm's output for short inputs has drifted unintentionally.
/// </remarks>
public class CityHash128GoldenVectorTests
{
    public static readonly TheoryData<string, int, ulong, ulong> Vectors = new()
    {
        // length, seed-byte pattern (i & 0xFF), low, high
        { "len-0",     0,    0x3DF09DFC64C09A2BUL, 0x3CB540C392E51E29UL },
        { "len-1",     1,    0xA04B71AB61DE6422UL, 0xF768684937E23970UL },
        { "len-7",     7,    0xAA38DB290CCB2B16UL, 0x684A34C21A5257DAUL },
        { "len-8",     8,    0xC475E6A71EC831AFUL, 0x301D452F75E2AD67UL },
        { "len-15",    15,   0x28DBDE5F7E27E5AEUL, 0x1EA991E69CAB0BF8UL },
        { "len-16",    16,   0x17CEADE677C2F945UL, 0x579ED60675C8FEDCUL },
        { "len-17",    17,   0x8112E830FB310FF1UL, 0xC972AD09C64FD737UL },
        { "len-31",    31,   0x695EA0324B28F5C9UL, 0x4328D2C3AD882B27UL },
        { "len-32",    32,   0xFE71590C670B561DUL, 0x498F5B464F875A30UL },
        { "len-63",    63,   0x087A8E314182487DUL, 0x0DA38C0CC0D2E175UL },
        { "len-64",    64,   0x83D9A0502FD851D0UL, 0x718073343EA63F22UL },
        { "len-127",   127,  0x6E9CEFA0ACCAEEC6UL, 0x56818BBA66E6A718UL },
        { "len-128",   128,  0x7DEF035B54925590UL, 0xDE34765C4ACA9038UL },
        { "len-129",   129,  0x946689818B344E34UL, 0x5BAF696C26FB2021UL },
        { "len-256",   256,  0x327BB07EC0345A9BUL, 0xFB45EF5CB037BD93UL },
        { "len-4096",  4096, 0x15AEE4AB17ED075AUL, 0x94D9FD60B3893768UL },
    };

    [Theory]
    [MemberData(nameof(Vectors))]
    public void Hash_PinnedDigest_MatchesGolden(string label, int length, ulong expectedLow, ulong expectedHigh)
    {
        var data = new byte[length];
        for (int i = 0; i < length; i++) data[i] = (byte)i;

        var (low, high) = CityHash128.Hash(data);

        Assert.True(low == expectedLow && high == expectedHigh,
            $"{label}: expected (0x{expectedLow:X16}UL, 0x{expectedHigh:X16}UL), got (0x{low:X16}UL, 0x{high:X16}UL)");
    }
}

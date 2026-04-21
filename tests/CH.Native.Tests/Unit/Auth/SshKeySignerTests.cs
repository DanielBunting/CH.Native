using System.Text;
using CH.Native.Auth;
using Xunit;

namespace CH.Native.Tests.Unit.Auth;

public class SshKeySignerTests
{
    [Fact]
    public void BuildSignedPayload_ConcatenatesInExactOrder()
    {
        var challenge = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };

        var payload = SshKeySigner.BuildSignedPayload(
            protocolVersion: 54467,
            database: "mydb",
            user: "alice",
            challenge: challenge);

        // Expected: "54467" + "mydb" + "alice" + [0xAA, 0xBB, 0xCC, 0xDD]
        var expected = new List<byte>();
        expected.AddRange(Encoding.UTF8.GetBytes("54467"));
        expected.AddRange(Encoding.UTF8.GetBytes("mydb"));
        expected.AddRange(Encoding.UTF8.GetBytes("alice"));
        expected.AddRange(challenge);

        Assert.Equal(expected, payload);
    }

    [Fact]
    public void BuildSignedPayload_EmptyDatabase_StillConcats()
    {
        var challenge = new byte[] { 0x01 };
        var payload = SshKeySigner.BuildSignedPayload(54467, "", "u", challenge);

        var expected = new List<byte>();
        expected.AddRange(Encoding.UTF8.GetBytes("54467"));
        expected.AddRange(Encoding.UTF8.GetBytes("u"));
        expected.Add(0x01);

        Assert.Equal(expected, payload);
    }

    [Fact]
    public void BuildSignedPayload_UsesInvariantCultureForProtocolVersion()
    {
        var previous = System.Threading.Thread.CurrentThread.CurrentCulture;
        try
        {
            // German uses "." for thousand separator; make sure we don't emit e.g. "54.467"
            System.Threading.Thread.CurrentThread.CurrentCulture =
                new System.Globalization.CultureInfo("de-DE");

            var payload = SshKeySigner.BuildSignedPayload(54467, "", "", Array.Empty<byte>());

            Assert.Equal(Encoding.UTF8.GetBytes("54467"), payload);
        }
        finally
        {
            System.Threading.Thread.CurrentThread.CurrentCulture = previous;
        }
    }
}

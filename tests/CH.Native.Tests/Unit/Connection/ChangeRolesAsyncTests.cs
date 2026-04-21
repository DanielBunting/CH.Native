using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

public class ChangeRolesAsyncTests
{
    [Fact]
    public async Task ChangeRolesAsync_ThrowsIfConnectionNotOpen()
    {
        await using var conn = new ClickHouseConnection("Host=localhost");
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            conn.ChangeRolesAsync(new[] { "analyst" }));
    }

    [Fact]
    public async Task ChangeRolesAsync_ThrowsIfDisposed()
    {
        var conn = new ClickHouseConnection("Host=localhost");
        await conn.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            conn.ChangeRolesAsync(new[] { "analyst" }));
    }
}

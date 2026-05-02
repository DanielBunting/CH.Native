using CH.Native.Ado;
using CH.Native.SystemTests.Fixtures;
using Xunit;

namespace CH.Native.SystemTests.Security;

/// <summary>
/// <c>ChangeDatabase</c> previously interpolated the caller-supplied identifier
/// into a <c>USE `…`</c> statement, escaping only backticks. NUL / newline /
/// over-length names slipped through as part of the identifier and could
/// confuse the server-side parser. The fix validates eagerly at the API
/// boundary before any wire IO.
/// </summary>
[Collection("SingleNode")]
[Trait(Categories.Name, Categories.Security)]
public class ChangeDatabaseInjectionTests
{
    private readonly SingleNodeFixture _fx;

    public ChangeDatabaseInjectionTests(SingleNodeFixture fx) => _fx = fx;

    [Theory]
    [InlineData("db\0name")]
    [InlineData("db\nname")]
    [InlineData("db\rname")]
    [InlineData("db\tname")]
    public async Task ControlCharactersInName_ThrowImmediately(string name)
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        Assert.Throws<ArgumentException>(() => conn.ChangeDatabase(name));
    }

    [Fact]
    public async Task OverLongName_ThrowsImmediately()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        var name = new string('a', 257);
        Assert.Throws<ArgumentException>(() => conn.ChangeDatabase(name));
    }

    [Fact]
    public async Task EmptyName_ThrowsImmediately()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        Assert.Throws<ArgumentException>(() => conn.ChangeDatabase("   "));
    }

    [Fact]
    public async Task ValidName_DoesNotThrow_DefaultExists()
    {
        await using var conn = new ClickHouseDbConnection(_fx.ConnectionString);
        await conn.OpenAsync();
        // 'default' is the system default database — always present.
        conn.ChangeDatabase("default");
        Assert.Equal("default", conn.Database);
    }
}

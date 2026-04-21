using CH.Native.Ado;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseDbCommandRolesTests
{
    [Fact]
    public void Roles_DefaultsToEmpty_LazyInit()
    {
        // Matches ClickHouse.Driver's surface: accessing Roles never throws and
        // returns an empty mutable list. Untouched Roles means "inherit connection
        // default" — backing field stays null to distinguish that from an empty list.
        var cmd = new ClickHouseDbCommand();
        Assert.NotNull(cmd.Roles);
        Assert.Empty(cmd.Roles);
    }

    [Fact]
    public void Roles_SupportsCollectionInitializer()
    {
        var cmd = new ClickHouseDbCommand { Roles = { "analyst", "admin_role" } };
        Assert.Equal(new[] { "analyst", "admin_role" }, cmd.Roles);
    }

    [Fact]
    public void Roles_Add_AppendsToList()
    {
        var cmd = new ClickHouseDbCommand();
        cmd.Roles.Add("analyst");
        cmd.Roles.Add("admin_role");
        Assert.Equal(new[] { "analyst", "admin_role" }, cmd.Roles);
    }

    [Fact]
    public void Roles_Clear_ExpressesInheritIntent()
    {
        // Parity with ClickHouse.Driver: empty list == "inherit connection default"
        // (their IList<string> shape can't distinguish null from empty). For an
        // explicit SET ROLE NONE, use ChangeRolesAsync(Array.Empty<string>()).
        var cmd = new ClickHouseDbCommand { Roles = { "analyst" } };
        cmd.Roles.Clear();
        Assert.Empty(cmd.Roles);
    }
}

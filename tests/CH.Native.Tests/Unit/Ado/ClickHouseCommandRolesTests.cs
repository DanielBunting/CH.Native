using CH.Native.Ado;
using CH.Native.Connection;
using CH.Native.Commands;
using CH.Native.Results;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

public class ClickHouseCommandRolesTests
{
    [Fact]
    public void Roles_DefaultsToEmpty_LazyInit()
    {
        // Matches ClickHouse.Driver's surface: accessing Roles never throws and
        // returns an empty mutable list. Untouched Roles means "inherit connection
        // default" — backing field stays null to distinguish that from an empty list.
        var cmd = new ClickHouseCommand();
        Assert.NotNull(cmd.Roles);
        Assert.Empty(cmd.Roles);
    }

    [Fact]
    public void Roles_SupportsCollectionInitializer()
    {
        var cmd = new ClickHouseCommand { Roles = { "analyst", "admin_role" } };
        Assert.Equal(new[] { "analyst", "admin_role" }, cmd.Roles);
    }

    [Fact]
    public void Roles_Add_AppendsToList()
    {
        var cmd = new ClickHouseCommand();
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
        var cmd = new ClickHouseCommand { Roles = { "analyst" } };
        cmd.Roles.Clear();
        Assert.Empty(cmd.Roles);
    }
}

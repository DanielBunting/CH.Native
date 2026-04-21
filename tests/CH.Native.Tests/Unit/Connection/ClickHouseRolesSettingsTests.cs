using CH.Native.Connection;
using Xunit;

namespace CH.Native.Tests.Unit.Connection;

public class ClickHouseRolesSettingsTests
{
    [Fact]
    public void Default_Roles_IsNull()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .Build();

        Assert.Null(settings.Roles);
    }

    [Fact]
    public void WithRoles_Params_SetsList()
    {
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithRoles("analyst", "admin_role")
            .Build();

        Assert.NotNull(settings.Roles);
        Assert.Equal(new[] { "analyst", "admin_role" }, settings.Roles);
    }

    [Fact]
    public void WithRoles_Empty_ExplicitlyEmpty_NotNull()
    {
        // Passing an empty list = "SET ROLE NONE" intent. Must NOT collapse to null.
        var settings = ClickHouseConnectionSettings.CreateBuilder()
            .WithHost("localhost")
            .WithRoles(Array.Empty<string>())
            .Build();

        Assert.NotNull(settings.Roles);
        Assert.Empty(settings.Roles!);
    }

    [Fact]
    public void WithRoles_Null_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            ClickHouseConnectionSettings.CreateBuilder().WithRoles((string[])null!));
    }

    [Fact]
    public void WithRoles_ContainsNullEntry_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.CreateBuilder().WithRoles("a", null!, "b"));
    }

    [Fact]
    public void WithRoles_ContainsWhitespace_Throws()
    {
        Assert.Throws<ArgumentException>(() =>
            ClickHouseConnectionSettings.CreateBuilder().WithRoles("a", "   "));
    }

    [Fact]
    public void Parse_RolesKey_ParsesCommaSeparated()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Roles=analyst,admin_role");

        Assert.Equal(new[] { "analyst", "admin_role" }, settings.Roles);
    }

    [Fact]
    public void Parse_RolesKey_SingleRole()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Roles=analyst");

        Assert.Equal(new[] { "analyst" }, settings.Roles);
    }

    [Fact]
    public void Parse_RoleAlias_MapsToRoles()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Role=analyst");

        Assert.Equal(new[] { "analyst" }, settings.Roles);
    }

    [Fact]
    public void Parse_EmptyRolesValue_ExplicitEmptyList()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Roles=");

        Assert.NotNull(settings.Roles);
        Assert.Empty(settings.Roles!);
    }

    [Fact]
    public void Parse_RolesWithWhitespace_IsTrimmed()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost;Roles= a , b ,c ");

        Assert.Equal(new[] { "a", "b", "c" }, settings.Roles);
    }

    [Fact]
    public void Parse_RolesOmitted_RolesIsNull()
    {
        var settings = ClickHouseConnectionSettings.Parse("Host=localhost");

        Assert.Null(settings.Roles);
    }
}

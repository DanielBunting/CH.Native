using CH.Native.Commands;
using CH.Native.Parameters;
using Xunit;

namespace CH.Native.Tests.Unit.Parameters;

public class SqlParameterRewriterTests
{
    #region Basic SQL Rewriting

    [Fact]
    public void Rewrite_NoParameters_ReturnsSameSql()
    {
        var parameters = new ClickHouseParameterCollection();
        var result = SqlParameterRewriter.Rewrite("SELECT 1", parameters);
        Assert.Equal("SELECT 1", result);
    }

    [Fact]
    public void Rewrite_SingleIntParameter_RewritesToClickHouseSyntax()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("userId", 42);

        var result = SqlParameterRewriter.Rewrite(
            "SELECT * FROM users WHERE id = @userId",
            parameters);

        Assert.Equal("SELECT * FROM users WHERE id = {userId:Int32}", result);
    }

    [Fact]
    public void Rewrite_SingleStringParameter_RewritesToClickHouseSyntax()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("name", "John");

        var result = SqlParameterRewriter.Rewrite(
            "SELECT * FROM users WHERE name = @name",
            parameters);

        Assert.Equal("SELECT * FROM users WHERE name = {name:String}", result);
    }

    [Fact]
    public void Rewrite_MultipleParameters_RewritesAll()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("minAge", 18);
        parameters.Add("maxAge", 65);
        parameters.Add("country", "USA");

        var result = SqlParameterRewriter.Rewrite(
            "SELECT * FROM users WHERE age >= @minAge AND age <= @maxAge AND country = @country",
            parameters);

        Assert.Equal(
            "SELECT * FROM users WHERE age >= {minAge:Int32} AND age <= {maxAge:Int32} AND country = {country:String}",
            result);
    }

    [Fact]
    public void Rewrite_SameParameterMultipleTimes_RewritesAll()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("value", 10);

        var result = SqlParameterRewriter.Rewrite(
            "SELECT @value, @value * 2, @value + @value",
            parameters);

        Assert.Equal("SELECT {value:Int32}, {value:Int32} * 2, {value:Int32} + {value:Int32}", result);
    }

    [Fact]
    public void Rewrite_ParameterWithExplicitType_UsesExplicitType()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("value", 123, "Nullable(Int32)");

        var result = SqlParameterRewriter.Rewrite(
            "SELECT @value",
            parameters);

        Assert.Equal("SELECT {value:Nullable(Int32)}", result);
    }

    #endregion

    #region Parameter Name Edge Cases

    [Fact]
    public void Rewrite_ParameterWithUnderscore_RewritesCorrectly()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("user_id", 42);

        var result = SqlParameterRewriter.Rewrite(
            "SELECT * FROM users WHERE id = @user_id",
            parameters);

        Assert.Equal("SELECT * FROM users WHERE id = {user_id:Int32}", result);
    }

    [Fact]
    public void Rewrite_ParameterStartingWithUnderscore_RewritesCorrectly()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("_private", 42);

        var result = SqlParameterRewriter.Rewrite(
            "SELECT @_private",
            parameters);

        Assert.Equal("SELECT {_private:Int32}", result);
    }

    [Fact]
    public void Rewrite_ParameterWithNumbers_RewritesCorrectly()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("param123", 42);

        var result = SqlParameterRewriter.Rewrite(
            "SELECT @param123",
            parameters);

        Assert.Equal("SELECT {param123:Int32}", result);
    }

    [Fact]
    public void Rewrite_DoubleAtSign_NotReplacedAsParameter()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("name", "John");

        // @@name should not be replaced (it's a ClickHouse system variable pattern)
        var result = SqlParameterRewriter.Rewrite(
            "SELECT @@version, @name",
            parameters);

        Assert.Equal("SELECT @@version, {name:String}", result);
    }

    [Fact]
    public void Rewrite_ParameterNameWithAtPrefixInCollection_StripsPrefix()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("@name", "John"); // Note: @ prefix provided

        var result = SqlParameterRewriter.Rewrite(
            "SELECT * FROM users WHERE name = @name",
            parameters);

        Assert.Equal("SELECT * FROM users WHERE name = {name:String}", result);
    }

    #endregion

    #region Error Handling

    [Fact]
    public void Rewrite_MissingParameter_ThrowsArgumentException()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("userId", 42);

        var ex = Assert.Throws<ArgumentException>(() =>
            SqlParameterRewriter.Rewrite(
                "SELECT * FROM users WHERE id = @userId AND name = @name",
                parameters));

        Assert.Contains("@name", ex.Message);
        Assert.Contains("not provided", ex.Message);
    }

    #endregion

    #region BuildParameterSettings

    [Fact]
    public void BuildParameterSettings_SingleParameter_ReturnsSetting()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("userId", 42);

        var settings = SqlParameterRewriter.BuildParameterSettings(parameters);

        Assert.Single(settings);
        // Parameter names are raw (no prefix), values are quoted for Field dump format
        Assert.Equal("'42'", settings["userId"]);
    }

    [Fact]
    public void BuildParameterSettings_StringParameter_ReturnsEscapedSetting()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("name", "O'Brien");

        var settings = SqlParameterRewriter.BuildParameterSettings(parameters);

        Assert.Single(settings);
        // String values are quoted and escaped
        Assert.Equal(@"'O\'Brien'", settings["name"]);
    }

    [Fact]
    public void BuildParameterSettings_MultipleParameters_ReturnsAllSettings()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("userId", 42);
        parameters.Add("name", "John");
        parameters.Add("active", true);

        var settings = SqlParameterRewriter.BuildParameterSettings(parameters);

        Assert.Equal(3, settings.Count);
        // All values are quoted for Field dump format
        Assert.Equal("'42'", settings["userId"]);
        Assert.Equal("'John'", settings["name"]);
        Assert.Equal("'1'", settings["active"]);
    }

    #endregion

    #region Process (Combined Rewrite and BuildSettings)

    [Fact]
    public void Process_ReturnsRewrittenSqlAndSettings()
    {
        var parameters = new ClickHouseParameterCollection();
        parameters.Add("minAge", 18);
        parameters.Add("country", "USA");

        var (sql, settings) = SqlParameterRewriter.Process(
            "SELECT * FROM users WHERE age >= @minAge AND country = @country",
            parameters);

        Assert.Equal(
            "SELECT * FROM users WHERE age >= {minAge:Int32} AND country = {country:String}",
            sql);
        // Parameter names are raw (no prefix), values are quoted for Field dump format
        Assert.Equal("'18'", settings["minAge"]);
        Assert.Equal("'USA'", settings["country"]);
    }

    #endregion
}

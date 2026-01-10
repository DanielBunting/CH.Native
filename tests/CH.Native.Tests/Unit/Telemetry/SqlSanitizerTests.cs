using CH.Native.Telemetry;
using Xunit;

namespace CH.Native.Tests.Unit.Telemetry;

public class SqlSanitizerTests
{
    [Theory]
    [InlineData("SELECT 'secret'", "SELECT ?")]
    [InlineData("SELECT 'hello world'", "SELECT ?")]
    [InlineData("WHERE name = 'Alice'", "WHERE name = ?")]
    [InlineData("SELECT 'it''s a test'", "SELECT ?")]
    [InlineData("SELECT ''", "SELECT ?")]
    public void Sanitize_ReplacesStringLiterals(string input, string expected)
    {
        var result = SqlSanitizer.Sanitize(input);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("SELECT 123", "SELECT ?")]
    [InlineData("WHERE id = 456", "WHERE id = ?")]
    [InlineData("LIMIT 100", "LIMIT ?")]
    [InlineData("SELECT 3.14", "SELECT ?")]
    [InlineData("SELECT -42", "SELECT ?")]
    [InlineData("SELECT 1e10", "SELECT ?")]
    [InlineData("SELECT 2.5E-3", "SELECT ?")]
    [InlineData("OFFSET 0", "OFFSET ?")]
    public void Sanitize_ReplacesNumericLiterals(string input, string expected)
    {
        var result = SqlSanitizer.Sanitize(input);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("SELECT * FROM table1", "SELECT * FROM table1")]
    [InlineData("SELECT id, name FROM users", "SELECT id, name FROM users")]
    [InlineData("SELECT col1 FROM db.table", "SELECT col1 FROM db.table")]
    [InlineData("SELECT count(*) FROM events", "SELECT count(*) FROM events")]
    public void Sanitize_PreservesIdentifiers(string input, string expected)
    {
        var result = SqlSanitizer.Sanitize(input);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Sanitize_CombinedLiterals()
    {
        var sql = "SELECT * FROM users WHERE name = 'Alice' AND age = 30";
        var expected = "SELECT * FROM users WHERE name = ? AND age = ?";
        Assert.Equal(expected, SqlSanitizer.Sanitize(sql));
    }

    [Fact]
    public void Sanitize_MultipleLiterals()
    {
        var sql = "INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)";
        var expected = "INSERT INTO users (name, email, age) VALUES (?, ?, ?)";
        Assert.Equal(expected, SqlSanitizer.Sanitize(sql));
    }

    [Fact]
    public void Sanitize_PreservesKeywords()
    {
        var sql = "SELECT 1 WHERE TRUE AND FALSE";
        // Keywords like TRUE/FALSE should be preserved as they're identifiers
        var result = SqlSanitizer.Sanitize(sql);
        Assert.Contains("WHERE", result);
        Assert.Contains("AND", result);
    }

    [Theory]
    [InlineData("", "")]
    [InlineData(null, null)]
    public void Sanitize_HandlesEmptyAndNull(string? input, string? expected)
    {
        var result = SqlSanitizer.Sanitize(input!);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Sanitize_PreservesTableAndColumnNames()
    {
        var sql = "SELECT column1, column2 FROM schema1.table1 WHERE id = 1";
        var result = SqlSanitizer.Sanitize(sql);
        Assert.Contains("column1", result);
        Assert.Contains("column2", result);
        Assert.Contains("schema1", result);
        Assert.Contains("table1", result);
    }
}

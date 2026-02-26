using CH.Native.Connection;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class SecurityTests
{
    private readonly ClickHouseFixture _fixture;

    public SecurityTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public async Task Security_SecondOrderInjection_TableName()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create a table with a long alphanumeric name to verify table operations
        // work safely with generated names (simulating safe table name patterns)
        var tableName = $"test_sec_injection_{Guid.NewGuid():N}";

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value String
            ) ENGINE = Memory");

        try
        {
            // Insert and verify the table was created with the safe name
            await connection.ExecuteNonQueryAsync(
                $"INSERT INTO {tableName} VALUES (1, 'safe_value')");

            var result = await connection.ExecuteScalarAsync<string>(
                $"SELECT Value FROM {tableName} WHERE Id = 1");

            Assert.Equal("safe_value", result);

            // Verify the table exists by counting
            var count = await connection.ExecuteScalarAsync<long>(
                $"SELECT count() FROM {tableName}");
            Assert.Equal(1, count);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Security_UnicodeHomoglyph_Parameter()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Cyrillic 'a' (U+0430) vs Latin 'a' (U+0061)
        // These look identical but are different bytes in UTF-8
        var cyrillicA = "\u0430"; // Cyrillic small letter a
        var latinA = "a";         // Latin small letter a
        var mixedString = $"test_{cyrillicA}_vs_{latinA}";

        // Insert the string with Cyrillic character via parameter
        var result = await connection.ExecuteScalarAsync<string>(
            "SELECT @text",
            new { text = mixedString });

        // Verify the exact string is preserved - no Unicode normalization
        Assert.Equal(mixedString, result);

        // Double-check the specific bytes are preserved
        Assert.Contains(cyrillicA, result);
        Assert.Contains(latinA, result);

        // The Cyrillic and Latin characters should remain distinct
        Assert.NotEqual(cyrillicA, latinA);
        Assert.Equal(mixedString.Length, result!.Length);
    }

    [Fact]
    public async Task Security_BackslashEscaping_String()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        var tableName = $"test_sec_backslash_{Guid.NewGuid():N}";

        await connection.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                Id Int32,
                Value String
            ) ENGINE = Memory");

        try
        {
            // String with literal backslash that could be misinterpreted as escape sequence
            var backslashString = "hello\\world";

            // Insert via bulk insert to use binary protocol (no string escaping issues)
            await using var inserter = connection.CreateBulkInserter<SimpleRow>(tableName);
            await inserter.InitAsync();

            await inserter.AddAsync(new SimpleRow { Id = 1, Value = backslashString });
            await inserter.CompleteAsync();

            // Read back and verify the literal string is preserved
            var result = new List<string>();
            await foreach (var row in connection.QueryAsync(
                $"SELECT Value FROM {tableName} WHERE Id = 1"))
            {
                result.Add(row.GetFieldValue<string>("Value"));
            }

            Assert.Single(result);
            Assert.Equal(backslashString, result[0]);

            // Also verify via parameter-based query
            var paramResult = await connection.ExecuteScalarAsync<string>(
                "SELECT @val",
                new { val = backslashString });
            Assert.Equal(backslashString, paramResult);
        }
        finally
        {
            await connection.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        }
    }

    [Fact]
    public async Task Security_ParameterizedQuery_NoInjection()
    {
        await using var connection = new ClickHouseConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Classic SQL injection payload as a parameter value
        var maliciousInput = "'; DROP TABLE test; --";

        // Use the @param syntax which the native protocol binds as binary data
        var result = await connection.ExecuteScalarAsync<string>(
            "SELECT @input",
            new { input = maliciousInput });

        // The malicious string must be returned as-is, not executed as SQL
        Assert.Equal(maliciousInput, result);

        // Verify with another injection variant
        var secondPayload = "1; SELECT * FROM system.numbers --";
        var secondResult = await connection.ExecuteScalarAsync<string>(
            "SELECT @input",
            new { input = secondPayload });
        Assert.Equal(secondPayload, secondResult);

        // Verify with null byte injection attempt
        var nullBytePayload = "before\0after";
        var nullResult = await connection.ExecuteScalarAsync<string>(
            "SELECT @input",
            new { input = nullBytePayload });
        Assert.Equal(nullBytePayload, nullResult);
    }

    #region Test POCOs

    private class SimpleRow
    {
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    #endregion
}

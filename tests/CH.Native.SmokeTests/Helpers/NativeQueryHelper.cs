using CH.Native.Connection;

namespace CH.Native.SmokeTests.Helpers;

public static class NativeQueryHelper
{
    public static async Task<List<object?[]>> QueryAsync(string connectionString, string sql)
    {
        var results = new List<object?[]>();

        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        await foreach (var row in connection.QueryAsync(sql))
        {
            var values = new object?[row.FieldCount];
            for (int i = 0; i < row.FieldCount; i++)
            {
                values[i] = row[i];
            }
            results.Add(values);
        }

        return results;
    }

    public static async Task ExecuteNonQueryAsync(string connectionString, string sql)
    {
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();
        await connection.ExecuteNonQueryAsync(sql);
    }

    public static async Task<T> ExecuteScalarAsync<T>(string connectionString, string sql)
    {
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();
        return await connection.ExecuteScalarAsync<T>(sql)!;
    }
}

using DriverConnection = ClickHouse.Driver.ADO.ClickHouseConnection;

namespace CH.Native.SmokeTests.Helpers;

public static class DriverQueryHelper
{
    public static async Task<List<object?[]>> QueryAsync(string connectionString, string sql)
    {
        var results = new List<object?[]>();

        using var connection = new DriverConnection(connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        using var reader = await cmd.ExecuteReaderAsync();

        int fieldCount = reader.FieldCount;
        while (await reader.ReadAsync())
        {
            var values = new object?[fieldCount];
            for (int i = 0; i < fieldCount; i++)
            {
                var val = reader.GetValue(i);
                values[i] = val is DBNull ? null : val;
            }
            results.Add(values);
        }

        return results;
    }

    public static async Task ExecuteNonQueryAsync(string connectionString, string sql)
    {
        using var connection = new DriverConnection(connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    public static async Task<object?> ExecuteScalarAsync(string connectionString, string sql)
    {
        using var connection = new DriverConnection(connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        var result = await cmd.ExecuteScalarAsync();
        return result is DBNull ? null : result;
    }
}

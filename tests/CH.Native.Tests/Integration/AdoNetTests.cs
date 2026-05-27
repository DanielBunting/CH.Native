using System.Data;
using CH.Native.Ado;
using CH.Native.Tests.Fixtures;
using Xunit;

namespace CH.Native.Tests.Integration;

[Collection("ClickHouse")]
public class AdoNetTests
{
    private readonly ClickHouseFixture _fixture;

    public AdoNetTests(ClickHouseFixture fixture)
    {
        _fixture = fixture;
    }

    #region Connection Tests

    [Fact]
    public async Task OpenAsync_ValidConnection_OpensSuccessfully()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);

        await connection.OpenAsync();

        Assert.Equal(ConnectionState.Open, connection.State);
    }

    [Fact]
    public async Task OpenAsync_SetsServerVersion()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);

        await connection.OpenAsync();

        Assert.NotEmpty(connection.ServerVersion);
        Assert.Contains(".", connection.ServerVersion); // e.g., "24.1"
    }

    [Fact]
    public async Task OpenAsync_SetsDataSource()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);

        await connection.OpenAsync();

        Assert.Contains(_fixture.Host, connection.DataSource);
    }

    [Fact]
    public void Open_StateTransitions_CorrectlyManaged()
    {
        using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);

        Assert.Equal(ConnectionState.Closed, connection.State);

        connection.Open();
        Assert.Equal(ConnectionState.Open, connection.State);

        connection.Close();
        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void ConnectionString_SetWhileOpen_ThrowsInvalidOperationException()
    {
        using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        connection.Open();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            connection.ConnectionString = "Host=other;Port=9000");

        Assert.Contains("open", ex.Message.ToLower());
    }

    [Fact]
    public async Task CloseAsync_CanBeCalledMultipleTimes()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        await connection.CloseAsync();
        await connection.CloseAsync(); // Should not throw

        Assert.Equal(ConnectionState.Closed, connection.State);
    }

    [Fact]
    public void BeginTransaction_ThrowsNotSupportedException()
    {
        using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        connection.Open();

        var ex = Assert.Throws<NotSupportedException>(() =>
            connection.BeginTransaction());

        Assert.Contains("transaction", ex.Message.ToLower());
    }

    [Fact]
    public async Task ChangeDatabase_SwitchesToNewDatabase()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // Create a test database
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "CREATE DATABASE IF NOT EXISTS test_adonet_db";
            await cmd.ExecuteNonQueryAsync();
        }

        connection.ChangeDatabase("test_adonet_db");

        Assert.Equal("test_adonet_db", connection.Database);

        // Verify we can query from the new database context
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT currentDatabase()";
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal("test_adonet_db", result);
        }

        // Cleanup
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "DROP DATABASE IF EXISTS test_adonet_db";
            await cmd.ExecuteNonQueryAsync();
        }
    }

    #endregion

    #region Command Tests

    [Fact]
    public async Task ExecuteNonQuery_CreateTable_Succeeds()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE IF NOT EXISTS test_adonet_nonquery (
                id UInt32,
                name String
            ) ENGINE = Memory";

        var result = await cmd.ExecuteNonQueryAsync();

        // DDL returns 0 affected rows
        Assert.Equal(0, result);

        // Cleanup
        cmd.CommandText = "DROP TABLE IF EXISTS test_adonet_nonquery";
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task ExecuteScalar_ReturnsFirstColumnFirstRow()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 42 as result";

        var result = await cmd.ExecuteScalarAsync();

        Assert.Equal(42, Convert.ToInt32(result));
    }

    [Fact]
    public async Task ExecuteScalar_WithString_ReturnsString()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 'hello' as greeting";

        var result = await cmd.ExecuteScalarAsync();

        Assert.Equal("hello", result);
    }

    [Fact]
    public async Task ExecuteReader_ReturnsMultipleRows()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT number FROM numbers(5)";

        await using var reader = await cmd.ExecuteReaderAsync();

        var values = new List<ulong>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetFieldValue<ulong>(0));
        }

        Assert.Equal(new ulong[] { 0, 1, 2, 3, 4 }, values);
    }

    [Fact]
    public void CommandType_StoredProcedure_ThrowsNotSupportedException()
    {
        using var cmd = new ClickHouseDbCommand();

        var ex = Assert.Throws<NotSupportedException>(() =>
            cmd.CommandType = CommandType.StoredProcedure);

        Assert.Contains("Text", ex.Message);
    }

    [Fact]
    public async Task CommandTimeout_CancelsLongRunningQuery()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        // Use sleep(2) which is within ClickHouse's 3 second limit
        // but longer than our 1 second timeout
        cmd.CommandText = "SELECT sleep(2)";
        cmd.CommandTimeout = 1; // 1 second timeout

        // Should throw either OperationCanceledException or TaskCanceledException
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await cmd.ExecuteScalarAsync());

        Assert.True(ex is OperationCanceledException or TaskCanceledException);
    }

    #endregion

    #region Parameter Tests

    [Fact]
    public async Task Parameters_IntParameter_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT @value as result";
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "value", Value = 42 });

        var result = await cmd.ExecuteScalarAsync();

        Assert.Equal(42, Convert.ToInt32(result));
    }

    [Fact]
    public async Task Parameters_StringParameter_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT @name as result";
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "name", Value = "test" });

        var result = await cmd.ExecuteScalarAsync();

        Assert.Equal("test", result);
    }

    [Fact]
    public async Task Parameters_MultipleParameters_WorkCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT @a + @b as result";
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "a", Value = 10 });
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "b", Value = 20 });

        var result = await cmd.ExecuteScalarAsync();

        Assert.Equal(30, Convert.ToInt32(result));
    }

    [Fact]
    public async Task Parameters_WithExplicitClickHouseType_UsesSpecifiedType()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT @value as result";
        cmd.Parameters.Add(new ClickHouseDbParameter
        {
            ParameterName = "value",
            Value = 123,
            ClickHouseType = "UInt64"
        });

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        var typeName = reader.GetDataTypeName(0);

        // The type should be UInt64 as specified
        Assert.Equal("UInt64", typeName);
    }

    #endregion

    #region DataReader Tests

    [Fact]
    public async Task DataReader_GetFieldCount_ReturnsCorrectCount()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1 as a, 2 as b, 3 as c";

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(3, reader.FieldCount);
    }

    [Fact]
    public async Task DataReader_GetName_ReturnsColumnNames()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1 as first_col, 2 as second_col";

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal("first_col", reader.GetName(0));
        Assert.Equal("second_col", reader.GetName(1));
    }

    [Fact]
    public async Task DataReader_GetOrdinal_ReturnsCorrectIndex()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1 as alpha, 2 as beta";

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(0, reader.GetOrdinal("alpha"));
        Assert.Equal(1, reader.GetOrdinal("beta"));
    }

    [Fact]
    public async Task DataReader_TypeGetters_WorkCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"SELECT
            toInt32(42) as int_val,
            toInt64(9999999999) as long_val,
            toFloat64(3.14) as double_val,
            'hello' as string_val,
            true as bool_val";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.Equal(42, reader.GetInt32(0));
        Assert.Equal(9999999999L, reader.GetInt64(1));
        Assert.Equal(3.14, reader.GetDouble(2), 0.001);
        Assert.Equal("hello", reader.GetString(3));
        Assert.True(reader.GetBoolean(4));
    }

    [Fact]
    public async Task DataReader_GetFieldValue_WorksForGenericTypes()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT toUInt64(12345) as value";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var value = reader.GetFieldValue<ulong>(0);
        Assert.Equal(12345UL, value);
    }

    [Fact]
    public async Task DataReader_IsDBNull_DetectsNullValues()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        // Use CAST to proper nullable type to avoid 'Nothing' type
        cmd.CommandText = "SELECT CAST(NULL AS Nullable(Int32)) as null_val, CAST(42 AS Nullable(Int32)) as not_null_val";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.True(reader.IsDBNull(0));
        Assert.False(reader.IsDBNull(1));
    }

    [Fact]
    public async Task DataReader_GetSchemaTable_ReturnsColumnMetadata()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT toInt32(1) as id, 'test' as name, toNullable('nullable') as nullable_col";

        await using var reader = await cmd.ExecuteReaderAsync();
        var schema = reader.GetSchemaTable();

        Assert.NotNull(schema);
        Assert.Equal(3, schema.Rows.Count);

        var firstRow = schema.Rows[0];
        Assert.Equal("id", firstRow["ColumnName"]);
        Assert.Equal(0, firstRow["ColumnOrdinal"]);
        Assert.False((bool)firstRow["AllowDBNull"]);

        var thirdRow = schema.Rows[2];
        Assert.Equal("nullable_col", thirdRow["ColumnName"]);
        Assert.True((bool)thirdRow["AllowDBNull"]);
    }

    [Fact]
    public async Task DataReader_RecordsAffected_ReturnsMinusOne()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.Equal(-1, reader.RecordsAffected);
    }

    [Fact]
    public async Task DataReader_NextResult_ReturnsFalse()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.False(reader.NextResult());
        Assert.False(await reader.NextResultAsync());
    }

    [Fact]
    public async Task DataReader_Indexer_ByOrdinalAndName()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 42 as value";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.Equal(42, Convert.ToInt32(reader[0]));
        Assert.Equal(42, Convert.ToInt32(reader["value"]));
    }

    #endregion

    #region Provider Factory Tests

    [Fact]
    public void ProviderFactory_CreateConnection_ReturnsClickHouseDbConnection()
    {
        var factory = ClickHouseProviderFactory.Instance;

        using var connection = factory.CreateConnection();

        Assert.IsType<ClickHouseDbConnection>(connection);
    }

    [Fact]
    public void ProviderFactory_CreateCommand_ReturnsClickHouseDbCommand()
    {
        var factory = ClickHouseProviderFactory.Instance;

        using var command = factory.CreateCommand();

        Assert.IsType<ClickHouseDbCommand>(command);
    }

    [Fact]
    public void ProviderFactory_CreateParameter_ReturnsClickHouseDbParameter()
    {
        var factory = ClickHouseProviderFactory.Instance;

        var parameter = factory.CreateParameter();

        Assert.IsType<ClickHouseDbParameter>(parameter);
    }

    [Fact]
    public void ProviderFactory_CanCreateDataAdapter_ReturnsFalse()
    {
        var factory = ClickHouseProviderFactory.Instance;

        Assert.False(factory.CanCreateDataAdapter);
    }

    #endregion

    #region Empty Result Tests

    [Fact]
    public async Task ExecuteReader_EmptyResult_HasRowsReturnsFalse()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT number FROM numbers(0)"; // Empty result

        await using var reader = await cmd.ExecuteReaderAsync();

        Assert.False(reader.HasRows);
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task ExecuteReader_EmptyResult_SchemaStillAvailable()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT toInt32(1) as id, 'test' as name FROM numbers(0)";

        await using var reader = await cmd.ExecuteReaderAsync();

        // Schema should be available even with no rows
        Assert.Equal(2, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("name", reader.GetName(1));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task ExecuteScalar_EmptyResult_ReturnsNull()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT number FROM numbers(0)";

        var result = await cmd.ExecuteScalarAsync();

        Assert.Null(result);
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public async Task ExecuteNonQuery_InvalidSql_ThrowsException()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELEC invalid syntax";

        await Assert.ThrowsAsync<CH.Native.Exceptions.ClickHouseServerException>(
            async () => await cmd.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task ExecuteReader_MissingParameter_ThrowsException()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        // Reference @missing_param but only provide @existing_param
        cmd.CommandText = "SELECT @missing_param, @existing_param as value";
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "existing_param", Value = 1 });
        // Not adding @missing_param

        // Exception thrown during parameter rewriting (before query is sent)
        await Assert.ThrowsAsync<ArgumentException>(
            async () => await cmd.ExecuteReaderAsync());
    }

    [Fact]
    public async Task ExecuteScalar_NonExistentTable_ThrowsException()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM non_existent_table_xyz_12345";

        // Server error thrown during query execution
        await Assert.ThrowsAsync<CH.Native.Exceptions.ClickHouseServerException>(
            async () => await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public void ExecuteNonQuery_ConnectionNotOpen_ThrowsInvalidOperationException()
    {
        using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 1";

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    [Fact]
    public void ExecuteNonQuery_ConnectionNotSet_ThrowsInvalidOperationException()
    {
        using var cmd = new ClickHouseDbCommand();
        cmd.CommandText = "SELECT 1";

        Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
    }

    #endregion

    #region Connection and Command Reuse Tests

    [Fact]
    public async Task Connection_MultipleCommands_ExecuteSequentially()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        // First command
        using (var cmd1 = connection.CreateCommand())
        {
            cmd1.CommandText = "SELECT 1 as value";
            var result1 = await cmd1.ExecuteScalarAsync();
            Assert.Equal(1, Convert.ToInt32(result1));
        }

        // Second command on same connection
        using (var cmd2 = connection.CreateCommand())
        {
            cmd2.CommandText = "SELECT 2 as value";
            var result2 = await cmd2.ExecuteScalarAsync();
            Assert.Equal(2, Convert.ToInt32(result2));
        }

        // Third command
        using (var cmd3 = connection.CreateCommand())
        {
            cmd3.CommandText = "SELECT 3 as value";
            var result3 = await cmd3.ExecuteScalarAsync();
            Assert.Equal(3, Convert.ToInt32(result3));
        }
    }

    [Fact]
    public async Task Command_Reuse_ExecuteMultipleTimes()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT @value as result";
        var param = new ClickHouseDbParameter { ParameterName = "value", Value = 0 };
        cmd.Parameters.Add(param);

        // Execute multiple times with different parameter values
        for (int i = 1; i <= 5; i++)
        {
            param.Value = i;
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(i, Convert.ToInt32(result));
        }
    }

    [Fact]
    public async Task Command_ChangeCommandText_ExecuteDifferentQueries()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();

        cmd.CommandText = "SELECT 'first' as value";
        var result1 = await cmd.ExecuteScalarAsync();
        Assert.Equal("first", result1);

        cmd.CommandText = "SELECT 'second' as value";
        var result2 = await cmd.ExecuteScalarAsync();
        Assert.Equal("second", result2);
    }

    [Fact]
    public async Task Connection_ReopenAfterClose_Succeeds()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);

        // First open/close cycle
        await connection.OpenAsync();
        Assert.Equal(ConnectionState.Open, connection.State);
        await connection.CloseAsync();
        Assert.Equal(ConnectionState.Closed, connection.State);

        // Second open/close cycle
        await connection.OpenAsync();
        Assert.Equal(ConnectionState.Open, connection.State);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT 42";
        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(42, Convert.ToInt32(result));
    }

    #endregion

    #region Reader Edge Cases

    [Fact]
    public async Task DataReader_DisposeBeforeReadingAllRows_Succeeds()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT number FROM numbers(1000)";

        // Read only first few rows then dispose
        await using (var reader = await cmd.ExecuteReaderAsync())
        {
            Assert.True(await reader.ReadAsync());
            Assert.True(await reader.ReadAsync());
            // Dispose without reading remaining rows
        }

        // Connection should still be usable
        using var cmd2 = connection.CreateCommand();
        cmd2.CommandText = "SELECT 'still works'";
        var result = await cmd2.ExecuteScalarAsync();
        Assert.Equal("still works", result);
    }

    [Fact]
    public async Task DataReader_GetValues_ReturnsAllColumns()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT toInt32(1) as a, toInt32(2) as b, toInt32(3) as c";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        var values = new object[3];
        var count = reader.GetValues(values);

        Assert.Equal(3, count);
        Assert.Equal(1, Convert.ToInt32(values[0]));
        Assert.Equal(2, Convert.ToInt32(values[1]));
        Assert.Equal(3, Convert.ToInt32(values[2]));
    }

    [Fact]
    public async Task DataReader_GetValues_PartialArray_ReturnsSubset()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT toInt32(1) as a, toInt32(2) as b, toInt32(3) as c";

        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        // Array smaller than field count
        var values = new object[2];
        var count = reader.GetValues(values);

        Assert.Equal(2, count);
        Assert.Equal(1, Convert.ToInt32(values[0]));
        Assert.Equal(2, Convert.ToInt32(values[1]));
    }

    [Fact]
    public void DataReader_SyncRead_Works()
    {
        using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        connection.Open(); // Sync open

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT number FROM numbers(3)";

        using var reader = cmd.ExecuteReader(); // Sync execute

        var values = new List<ulong>();
        while (reader.Read()) // Sync read
        {
            values.Add(reader.GetFieldValue<ulong>(0));
        }

        Assert.Equal(new ulong[] { 0, 1, 2 }, values);
    }

    #endregion

    #region Large Result Streaming Test

    [Fact]
    public async Task ExecuteReader_LargeResultSet_StreamsWithoutOOM()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        // 100k rows - should stream without loading all into memory
        cmd.CommandText = "SELECT number, toString(number) as str FROM numbers(100000)";

        await using var reader = await cmd.ExecuteReaderAsync();

        long rowCount = 0;
        long sum = 0;
        while (await reader.ReadAsync())
        {
            sum += (long)reader.GetFieldValue<ulong>(0);
            rowCount++;
        }

        Assert.Equal(100000, rowCount);
        // Sum of 0..99999 = n*(n-1)/2 = 100000*99999/2 = 4999950000
        Assert.Equal(4999950000L, sum);
    }

    #endregion

    #region Array Parameter Tests (Direct ADO.NET)

    [Fact]
    public async Task ExecuteScalar_WithArrayParameter_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT length(@arr)";
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "arr", Value = new int[] { 1, 2, 3, 4, 5 } });

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(5UL, result);
    }

    [Fact]
    public async Task ExecuteScalar_WithArrayParameterHasAny_FiltersCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT count() FROM numbers(10) WHERE hasAny([number], @ids)";
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "ids", Value = new ulong[] { 2, 4, 6, 8 } });

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal(4UL, result);
    }

    [Fact]
    public async Task ExecuteScalar_WithStringArrayParameter_WorksCorrectly()
    {
        await using var connection = new ClickHouseDbConnection(_fixture.ConnectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT arrayStringConcat(@names, ', ')";
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "names", Value = new string[] { "Alice", "Bob", "Charlie" } });

        var result = await cmd.ExecuteScalarAsync();
        Assert.Equal("Alice, Bob, Charlie", result);
    }

    #endregion
}

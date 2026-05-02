using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

/// <summary>
/// White-box test for the retry-safety classifier. The actual data-integrity
/// guard depends on this returning <c>false</c> for every write verb, so the
/// matrix below is intentionally exhaustive against the ClickHouse statement
/// vocabulary the library can reasonably encounter.
/// </summary>
public class SqlRetrySafetyTests
{
    [Theory]
    [InlineData("SELECT 1")]
    [InlineData("select 1")]
    [InlineData("  SELECT 1")]
    [InlineData("\nSELECT 1")]
    [InlineData("-- comment\nSELECT 1")]
    [InlineData("/* block */ SELECT 1")]
    [InlineData("/* multi\nline */ SELECT 1")]
    [InlineData("SHOW DATABASES")]
    [InlineData("DESCRIBE my_table")]
    [InlineData("DESC my_table")]
    [InlineData("EXPLAIN SELECT * FROM t")]
    [InlineData("EXISTS my_table")]
    [InlineData("CHECK TABLE my_table")]
    public void ReadStatements_AreRetrySafe(string sql)
    {
        Assert.True(SqlRetrySafety.IsRetrySafe(sql), $"Expected retry-safe: {sql}");
    }

    [Theory]
    [InlineData("INSERT INTO t VALUES (1)")]
    [InlineData("insert into t values (1)")]
    [InlineData("  INSERT INTO t VALUES (1)")]
    [InlineData("/* comment */ INSERT INTO t VALUES (1)")]
    [InlineData("ALTER TABLE t ADD COLUMN c Int32")]
    [InlineData("OPTIMIZE TABLE t FINAL")]
    [InlineData("KILL QUERY WHERE query_id = '...'")]
    [InlineData("CREATE TABLE t (c Int32) ENGINE = MergeTree")]
    [InlineData("DROP TABLE t")]
    [InlineData("RENAME TABLE a TO b")]
    [InlineData("TRUNCATE TABLE t")]
    [InlineData("ATTACH TABLE t")]
    [InlineData("DETACH TABLE t")]
    [InlineData("EXCHANGE TABLES a AND b")]
    [InlineData("SET allow_experimental_object_type = 1")]
    [InlineData("GRANT SELECT ON db.* TO user")]
    [InlineData("REVOKE SELECT ON db.* FROM user")]
    [InlineData("USE db")]
    [InlineData("SYSTEM RELOAD CONFIG")]
    [InlineData("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x")]
    public void WriteAndDdlStatements_AreNotRetrySafe(string sql)
    {
        Assert.False(SqlRetrySafety.IsRetrySafe(sql), $"Expected NOT retry-safe: {sql}");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\n\n")]
    [InlineData("-- only a comment")]
    [InlineData("/* only block */")]
    [InlineData(";")]
    [InlineData("123")]
    public void EmptyOrUnparsable_AreNotRetrySafe(string? sql)
    {
        // Defaulting to "not retry-safe" is the safe failure mode — better to
        // surface a transient error to the caller than to silently retry an
        // unrecognized statement.
        Assert.False(SqlRetrySafety.IsRetrySafe(sql));
    }

    [Theory]
    [InlineData("WITH cte AS (SELECT 1) SELECT * FROM cte")]
    [InlineData("with cte as (select 1) select * from cte")]
    [InlineData("WITH 1 AS one SELECT one")]
    [InlineData("WITH /* comment */ cte AS (SELECT 1) SELECT * FROM cte")]
    [InlineData("WITH cte AS (SELECT 'INSERT INTO foo' as msg) SELECT * FROM cte")]
    public void WithPrefixedReadOnlyCte_IsRetrySafe(string sql)
    {
        // Read-only CTEs (WITH … SELECT, no write verb anywhere outside string
        // literals or comments) are retry-safe. The classifier scans for any
        // standalone write verb in the comment-stripped, literal-aware token
        // stream — string content like `'INSERT INTO foo'` does NOT poison
        // the classification.
        Assert.True(SqlRetrySafety.IsRetrySafe(sql),
            $"Read-only CTE should be retry-safe: {sql}");
    }

    [Theory]
    [InlineData("WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte")]
    [InlineData("WITH cte AS (SELECT 1) UPDATE t SET v = 1 WHERE id IN (SELECT id FROM cte)")]
    [InlineData("with cte as (select 1) insert into t values (1)")]
    [InlineData("WITH cte AS (SELECT 1) ALTER TABLE t ADD COLUMN x Int32")]
    public void WithPrefixedWriteCte_IsNotRetrySafe(string sql)
    {
        // Mirror: a CTE that wraps a write verb (anywhere) must NOT be
        // retried — that's the data-integrity guard the whole classifier
        // exists for.
        Assert.False(SqlRetrySafety.IsRetrySafe(sql),
            $"CTE wrapping a write should be unsafe: {sql}");
    }

    [Theory]
    [InlineData("-- WITH cte AS (...) ; \nSELECT 1")]
    [InlineData("/* WITH cte AS (...) */ SELECT 1")]
    [InlineData("/* outer */-- inner\nSELECT 1")]
    public void CommentsContainingVerbs_DoNotPoisonClassification(string sql)
    {
        // The verb extractor strips comments before reading the first identifier,
        // so a hostile-looking comment containing "INSERT" or "WITH" must not
        // cause a real read to be classified unsafe.
        Assert.True(SqlRetrySafety.IsRetrySafe(sql),
            $"Comment-stripping should ignore verbs in comment text: {sql}");
    }

    [Theory]
    [InlineData("\t\rSELECT 1")]
    [InlineData("\t  -- c\n\t/* d */\tSELECT 1")]
    public void MixedWhitespaceAndComments_AreSkipped(string sql)
    {
        Assert.True(SqlRetrySafety.IsRetrySafe(sql));
    }

    [Fact]
    public void UnterminatedBlockComment_DoesNotInfiniteLoop_AndIsNotRetrySafe()
    {
        // Defensive: an unterminated /* ... block must not hang and must not
        // be classified safe (no verb identifier was ever found).
        const string sql = "/* never closed";
        Assert.False(SqlRetrySafety.IsRetrySafe(sql));
    }
}

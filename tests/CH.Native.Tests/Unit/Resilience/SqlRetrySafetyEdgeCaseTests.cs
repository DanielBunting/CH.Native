using CH.Native.Resilience;
using Xunit;

namespace CH.Native.Tests.Unit.Resilience;

/// <summary>
/// Probes adversarial / edge-case inputs for the retry classifier.
/// F3 fixed the canonical CTE case; these are inputs we haven't audited yet
/// and are likely to surface in real workloads:
///
/// <list type="bullet">
/// <item><description><c>EXPLAIN INSERT</c> — read-only despite the wrapped INSERT verb.</description></item>
/// <item><description><c>SHOW CREATE TABLE</c> — read-only with no CREATE side effect.</description></item>
/// <item><description><c>SETTINGS x = 1; SELECT</c> — settings-prefixed read, written
///     outside CTE pattern but with a leading SET-like token.</description></item>
/// <item><description>Mixed-comment + WITH + SELECT.</description></item>
/// </list>
/// </summary>
public class SqlRetrySafetyEdgeCaseTests
{
    [Theory]
    [InlineData("EXPLAIN SELECT 1")]
    [InlineData("EXPLAIN SYNTAX SELECT 1")]
    [InlineData("EXPLAIN PIPELINE SELECT 1")]
    public void ExplainOfSelect_IsRetrySafe(string sql)
    {
        Assert.True(SqlRetrySafety.IsRetrySafe(sql), $"Expected retry-safe: {sql}");
    }

    [Theory]
    [InlineData("EXPLAIN INSERT INTO t VALUES (1)")]
    [InlineData("EXPLAIN PIPELINE INSERT INTO t SELECT * FROM s")]
    public void ExplainOfInsert_DocumentedBehaviour(string sql)
    {
        // OBSERVE: EXPLAIN INSERT is read-only (it doesn't actually INSERT),
        // but the classifier today only sees the leading EXPLAIN verb. The
        // ContainsWriteVerb scan added in F3 only fires for WITH-prefixed
        // statements — so EXPLAIN INSERT is currently retry-safe (because
        // EXPLAIN is in the allow-list and there's no WITH-scan).
        //
        // Actually correct! EXPLAIN never executes the inner write, so
        // retry is safe. Pin this as INTENDED behaviour.
        Assert.True(SqlRetrySafety.IsRetrySafe(sql),
            $"EXPLAIN INSERT is read-only at execution time: {sql}");
    }

    [Theory]
    [InlineData("SHOW CREATE TABLE my_table")]
    [InlineData("show create database my_db")]
    public void ShowCreate_IsRetrySafe(string sql)
    {
        // SHOW is in the allow-list; the trailing CREATE is just keyword
        // text, not a write verb at execution time.
        Assert.True(SqlRetrySafety.IsRetrySafe(sql), $"Expected retry-safe: {sql}");
    }

    [Theory]
    [InlineData("/*INSERT comment*/ /*ALTER comment*/ SELECT 1")]
    [InlineData("-- INSERT INTO t (x) VALUES (1)\nSELECT 1")]
    public void CommentsContainingMultipleWriteVerbs_DontPoisonClassification(string sql)
    {
        // Comments are stripped before verb extraction; multiple write
        // verbs in comments must not affect the read classification.
        Assert.True(SqlRetrySafety.IsRetrySafe(sql), $"Comments must be ignored: {sql}");
    }

    [Theory]
    [InlineData("WITH x AS (SELECT 1) SELECT 'INSERT INTO foo' as msg")]
    [InlineData("WITH x AS (SELECT 1) SELECT 'UPDATE t SET x=1' as msg")]
    public void StringLiteralContainingWriteVerb_InsideCte_IsStillRetrySafe(string sql)
    {
        // F3 added a literal-aware token walker; verify it correctly
        // ignores write-verb tokens that live inside string literals.
        Assert.True(SqlRetrySafety.IsRetrySafe(sql),
            $"String-literal verb tokens must not poison classification: {sql}");
    }

    [Theory]
    [InlineData("WITH x AS (SELECT 1) SELECT * FROM x; INSERT INTO y VALUES (1)")]
    public void SemicolonChainedStatements_AreNotRetrySafe(string sql)
    {
        // The classifier inspects the leading verb; chained statements
        // after a semicolon are NOT in the leading-verb context. Today's
        // behavior: the WITH-scan walks the entire string (across the
        // semicolon) and finds the INSERT — correctly flags as unsafe.
        // Pin this as the correct behaviour.
        Assert.False(SqlRetrySafety.IsRetrySafe(sql), $"Chained INSERT must mark as unsafe: {sql}");
    }
}

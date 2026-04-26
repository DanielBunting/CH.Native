using CH.Native.Connection;
using Xunit;

namespace CH.Native.SystemTests.Linq;

/// <summary>
/// Helpers used by the LINQ system tests to compare LINQ-translated SQL against a
/// hand-written raw-SQL oracle running on the same connection / table.
/// </summary>
internal static class LinqAssertions
{
    /// <summary>
    /// Executes a single-scalar oracle query.
    /// </summary>
    public static async Task<T?> ExecuteScalarAsync<T>(
        ClickHouseConnection conn, string rawSql, CancellationToken ct = default)
    {
        return await conn.ExecuteScalarAsync<T>(rawSql, cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Asserts that two row sequences contain the same elements, regardless of order,
    /// using the supplied projection as the equality key. Both sides are sorted by
    /// <paramref name="orderKey"/> before comparison so the test is order-stable.
    /// </summary>
    public static void AssertSetsEqual<T>(
        IEnumerable<T> expected,
        IEnumerable<T> actual,
        Func<T, IComparable> orderKey)
    {
        var e = expected.OrderBy(orderKey).ToList();
        var a = actual.OrderBy(orderKey).ToList();

        Assert.Equal(e.Count, a.Count);
        for (int i = 0; i < e.Count; i++)
        {
            Assert.Equal(e[i], a[i]);
        }
    }
}

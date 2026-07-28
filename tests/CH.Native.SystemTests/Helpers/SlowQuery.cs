namespace CH.Native.SystemTests.Helpers;

/// <summary>
/// Builds queries that occupy the server for a known WALL-CLOCK duration.
/// <para>
/// The obvious way to write a "slow query" is a big scan such as
/// <c>SELECT count() FROM numbers(10000000000)</c>, but its duration is
/// throughput-bound: it depends entirely on how many cores the server has.
/// Tests that race a scan against a fixed deadline (a cancellation token, a
/// <c>CommandTimeout</c>, a <c>Task.Delay</c>) silently stop testing anything
/// once CI hardware gets fast enough to finish the scan first — the assertion
/// fails not because the client regressed, but because the precondition the
/// test needed was never established. Measured on ClickHouse 24.8, a 10B-row
/// count ranges from ~2.5s to well under 0.5s depending purely on the host.
/// </para>
/// <para>
/// <c>sleepEachRow</c> is bound by wall clock instead, so the duration holds on
/// any hardware. <c>max_threads=1</c> keeps the rows serial (otherwise the sleeps
/// run concurrently and the total collapses), and <c>max_block_size=1</c> puts a
/// block boundary after every row so the server observes cancellation within one
/// step rather than at the end.
/// </para>
/// </summary>
internal static class SlowQuery
{
    /// <summary>
    /// Granularity of a single sleep. Also the server's worst-case latency in
    /// noticing a cancellation, since it is checked at block boundaries.
    /// </summary>
    private const double StepSeconds = 0.1;

    /// <summary>
    /// A query that keeps the server busy for at least <paramref name="duration"/>.
    /// Returns a single row, so it is safe for <c>ExecuteScalar</c> callers.
    /// </summary>
    public static string LastingAtLeast(TimeSpan duration)
    {
        var rows = (long)Math.Ceiling(duration.TotalSeconds / StepSeconds);
        if (rows < 1) rows = 1;

        return $"SELECT count() FROM numbers({rows}) WHERE NOT ignore(sleepEachRow({StepSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture)})) " +
               "SETTINGS max_block_size = 1, max_threads = 1";
    }

    /// <summary>
    /// A query long enough that no test deadline will outrun it — for cases that
    /// need a query merely to be "still running" (kill, cancel, timeout, contention).
    /// </summary>
    public static string Indefinite() => LastingAtLeast(TimeSpan.FromMinutes(5));
}

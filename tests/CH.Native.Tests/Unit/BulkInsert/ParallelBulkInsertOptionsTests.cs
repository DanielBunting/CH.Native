using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Pins the <see cref="ParallelBulkInsertOptions"/> contract: defaults, the
/// pool-size validation that guards against deadlock, channel-capacity defaulting,
/// and the deliberate absence of a deduplication token on the worker options the
/// fan-out builds. The no-token guard is a regression test — re-adding a token to
/// the parallel path is unsafe under work-stealing (see the type remarks).
/// </summary>
public class ParallelBulkInsertOptionsTests
{
    [Fact]
    public void Defaults_AreSensible()
    {
        var options = new ParallelBulkInsertOptions();
        Assert.Equal(4, options.DegreeOfParallelism);
        Assert.Equal(10_000, options.BatchSize);
        Assert.Null(options.ChannelCapacity);
        Assert.Null(options.Roles);
        Assert.Null(options.UseSchemaCache);
        Assert.Null(options.QueryId);
    }

    [Fact]
    public void Type_HasNoDeduplicationTokenMember()
    {
        // The parallel path must never expose a dedup token — its identity is
        // tied to (token, block-sequence), which work-stealing fan-out makes
        // unsound. This reflection guard fails loudly if one is ever added.
        var member = typeof(ParallelBulkInsertOptions).GetMember("DeduplicationToken");
        Assert.Empty(member);
    }

    [Fact]
    public void ResolveChannelCapacity_DefaultsToDegreeTimesBatch()
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = 4, BatchSize = 10_000 };
        Assert.Equal(40_000, options.ResolveChannelCapacity());
    }

    [Fact]
    public void ResolveChannelCapacity_HonoursExplicitValue()
    {
        var options = new ParallelBulkInsertOptions { ChannelCapacity = 123 };
        Assert.Equal(123, options.ResolveChannelCapacity());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_DegreeBelowOne_Throws(int degree)
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = degree };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate(maxPoolSize: 100));
    }

    [Fact]
    public void Validate_DegreeExceedsMaxPoolSize_Throws()
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = 12 };
        var ex = Assert.Throws<ArgumentException>(() => options.Validate(maxPoolSize: 8));
        Assert.Contains("MaxPoolSize", ex.Message);
    }

    [Fact]
    public void Validate_DegreeEqualToMaxPoolSize_Ok()
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = 8 };
        options.Validate(maxPoolSize: 8); // does not throw
    }

    [Fact]
    public void Validate_BatchSizeBelowOne_Throws()
    {
        var options = new ParallelBulkInsertOptions { BatchSize = 0 };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate(maxPoolSize: 100));
    }

    [Fact]
    public void Validate_ExplicitChannelCapacityBelowOne_Throws()
    {
        var options = new ParallelBulkInsertOptions { ChannelCapacity = 0 };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate(maxPoolSize: 100));
    }

    [Fact]
    public void BuildWorkerOptions_NeverSetsDeduplicationToken()
    {
        var worker = new ParallelBulkInsertOptions().BuildWorkerOptions(0);
        Assert.Null(worker.DeduplicationToken);
    }

    [Fact]
    public void BuildWorkerOptions_PropagatesBatchAndRolesAndSchemaCache()
    {
        var roles = new List<string> { "analyst" };
        var options = new ParallelBulkInsertOptions
        {
            BatchSize = 5_000,
            Roles = roles,
            UseSchemaCache = true,
        };

        var worker = options.BuildWorkerOptions(0);
        Assert.Equal(5_000, worker.BatchSize);
        Assert.Same(roles, worker.Roles);
        Assert.True(worker.UseSchemaCache);
    }

    [Fact]
    public void BuildWorkerOptions_SuffixesQueryIdPerWorker()
    {
        var options = new ParallelBulkInsertOptions { QueryId = "import-42" };
        Assert.Equal("import-42-p0", options.BuildWorkerOptions(0).QueryId);
        Assert.Equal("import-42-p3", options.BuildWorkerOptions(3).QueryId);
    }

    [Fact]
    public void BuildWorkerOptions_NullQueryId_StaysNull()
    {
        Assert.Null(new ParallelBulkInsertOptions().BuildWorkerOptions(0).QueryId);
    }
}

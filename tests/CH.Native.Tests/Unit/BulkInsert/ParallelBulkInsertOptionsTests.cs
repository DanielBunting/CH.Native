using CH.Native.BulkInsert;
using Xunit;

namespace CH.Native.Tests.Unit.BulkInsert;

/// <summary>
/// Pins the <see cref="ParallelBulkInsertOptions"/> contract: defaults, the
/// pool-size resolution/validation that guards against deadlock without making
/// the default unusable on small pools, overflow-safe channel-capacity defaulting,
/// and the deliberate absence of a deduplication token on the worker options the
/// fan-out builds.
/// </summary>
public class ParallelBulkInsertOptionsTests
{
    [Fact]
    public void Defaults_AreSensible()
    {
        var options = new ParallelBulkInsertOptions();
        // null DegreeOfParallelism => resolved per-pool (min(4, MaxPoolSize)).
        Assert.Null(options.DegreeOfParallelism);
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

    // --- Resolve: degree-of-parallelism ---

    [Fact]
    public void Resolve_DefaultOptions_OnSmallPool_DoesNotThrowAndClamps()
    {
        // The default (unset) DegreeOfParallelism must not throw just because
        // MaxPoolSize is below the nominal default of 4.
        var options = new ParallelBulkInsertOptions();
        Assert.Equal(2, options.Resolve(maxPoolSize: 2));
    }

    [Fact]
    public void Resolve_DefaultOptions_OnLargePool_UsesNominalDefault()
    {
        Assert.Equal(4, new ParallelBulkInsertOptions().Resolve(maxPoolSize: 100));
    }

    [Fact]
    public void Resolve_ExplicitDegree_ReturnedAsIs()
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = 3 };
        Assert.Equal(3, options.Resolve(maxPoolSize: 100));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Resolve_ExplicitDegreeBelowOne_Throws(int degree)
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = degree };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Resolve(maxPoolSize: 100));
    }

    [Fact]
    public void Resolve_ExplicitDegreeExceedsMaxPoolSize_Throws()
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = 12 };
        var ex = Assert.Throws<ArgumentException>(() => options.Resolve(maxPoolSize: 8));
        Assert.Contains("MaxPoolSize", ex.Message);
    }

    [Fact]
    public void Resolve_ExplicitDegreeEqualToMaxPoolSize_Ok()
    {
        var options = new ParallelBulkInsertOptions { DegreeOfParallelism = 8 };
        Assert.Equal(8, options.Resolve(maxPoolSize: 8));
    }

    [Fact]
    public void Resolve_BatchSizeBelowOne_Throws()
    {
        var options = new ParallelBulkInsertOptions { BatchSize = 0 };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Resolve(maxPoolSize: 100));
    }

    [Fact]
    public void Resolve_ExplicitChannelCapacityBelowOne_Throws()
    {
        var options = new ParallelBulkInsertOptions { ChannelCapacity = 0 };
        Assert.Throws<ArgumentOutOfRangeException>(() => options.Resolve(maxPoolSize: 100));
    }

    // --- ResolveChannelCapacity ---

    [Fact]
    public void ResolveChannelCapacity_DefaultsToDegreeTimesBatch()
    {
        var options = new ParallelBulkInsertOptions { BatchSize = 10_000 };
        Assert.Equal(40_000, options.ResolveChannelCapacity(degreeOfParallelism: 4));
    }

    [Fact]
    public void ResolveChannelCapacity_HonoursExplicitValue()
    {
        var options = new ParallelBulkInsertOptions { ChannelCapacity = 123 };
        Assert.Equal(123, options.ResolveChannelCapacity(degreeOfParallelism: 8));
    }

    [Fact]
    public void ResolveChannelCapacity_LargeValues_ClampToInt32MaxValue()
    {
        // degree*batch can exceed Int32 without either value being individually
        // invalid; the result must clamp, not overflow.
        var options = new ParallelBulkInsertOptions { BatchSize = 300_000_000 };
        Assert.Equal(int.MaxValue, options.ResolveChannelCapacity(degreeOfParallelism: 8));
    }

    // --- BuildWorkerOptions ---

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

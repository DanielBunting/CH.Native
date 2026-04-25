using Xunit;

// System tests share heavyweight Docker resources (cluster, version-matrix cache,
// toxiproxy) and the allocation-budget tests rely on a process-wide GC counter;
// running tests across collections in parallel makes both flaky. Force the entire
// assembly to run sequentially.
[assembly: CollectionBehavior(DisableTestParallelization = true)]

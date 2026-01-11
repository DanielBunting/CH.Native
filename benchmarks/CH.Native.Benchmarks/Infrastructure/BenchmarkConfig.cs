using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;
using BenchmarkDotNet.Reports;

namespace CH.Native.Benchmarks.Infrastructure;

/// <summary>
/// Custom BenchmarkDotNet configuration for protocol comparison benchmarks.
/// </summary>
public class ProtocolComparisonConfig : ManualConfig
{
    public ProtocolComparisonConfig()
    {
        // Job configuration - moderate iteration count for network benchmarks
        AddJob(Job.Default
            .WithWarmupCount(3)
            .WithIterationCount(10)
            .WithInvocationCount(1)      // Network operations are slow, 1 per iteration
            .WithUnrollFactor(1));

        // Diagnosers
        AddDiagnoser(MemoryDiagnoser.Default);

        // Columns
        AddColumn(StatisticColumn.Mean);
        AddColumn(StatisticColumn.StdDev);
        AddColumn(StatisticColumn.Median);
        AddColumn(StatisticColumn.P95);
        AddColumn(RankColumn.Arabic);
        AddColumn(new TagColumn("Protocol", name =>
            name.Contains("Native") ? "Native TCP" : "HTTP"));

        // Exporters
        AddExporter(MarkdownExporter.GitHub);
        AddExporter(JsonExporter.Full);
        AddExporter(HtmlExporter.Default);

        // Logger
        AddLogger(ConsoleLogger.Default);

        // Summary style
        WithSummaryStyle(SummaryStyle.Default.WithRatioStyle(RatioStyle.Trend));
    }
}

/// <summary>
/// Lighter config for quick iterations during development.
/// </summary>
public class QuickComparisonConfig : ManualConfig
{
    public QuickComparisonConfig()
    {
        AddJob(Job.Default
            .WithWarmupCount(1)
            .WithIterationCount(3)
            .WithInvocationCount(1)
            .WithUnrollFactor(1));

        AddDiagnoser(MemoryDiagnoser.Default);
        AddLogger(ConsoleLogger.Default);
    }
}

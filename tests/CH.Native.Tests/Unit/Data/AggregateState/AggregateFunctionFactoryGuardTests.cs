using System.Reflection;
using CH.Native.Data;
using CH.Native.Data.Types;
using Xunit;

namespace CH.Native.Tests.Unit.Data.AggregateState;

/// <summary>
/// Defensive-guard coverage for the three factory arms that handle
/// <c>AggregateFunction(...)</c>. The guards check
/// <c>type.AggregateFunctionName is null</c> and throw — a state the public
/// <see cref="ClickHouseTypeParser"/> cannot produce, but the internal 7-arg
/// ctor of <see cref="ClickHouseType"/> can. Tests drive the private
/// <c>CreateReaderForType</c> / <c>CreateWriterForType</c> /
/// <c>CreateSkipperForType</c> entry points via reflection — the public
/// <c>CreateReader(string)</c> family re-parses the type name and so cannot
/// trigger the guard.
///
/// The reflection is intentional: these are dead-by-design guards documenting
/// an internal invariant, and the tests lock that invariant down so refactors
/// don't silently remove it.
/// </summary>
public class AggregateFunctionFactoryGuardTests
{
    private static ClickHouseType MalformedAggregateFunctionType()
    {
        // Internal 7-arg ctor — sets BaseName = "AggregateFunction" but leaves
        // AggregateFunctionName = null, mimicking what a buggy parser would emit.
        return new ClickHouseType(
            baseName: "AggregateFunction",
            typeArguments: new[] { new ClickHouseType("Int32") },
            parameters: null,
            originalTypeName: "AggregateFunction(_, Int32)",
            fieldNames: null,
            aggregateFunctionName: null,
            aggregateFunctionParameters: null);
    }

    private static Exception? UnwrapTargetInvocation(Exception ex) =>
        ex is TargetInvocationException tie ? tie.InnerException : ex;

    [Fact]
    public void ReaderFactory_AggregateFunctionWithNullName_ThrowsFormatException_DefensiveGuard()
    {
        var factory = new ColumnReaderFactory(ColumnReaderRegistry.Default);
        var method = typeof(ColumnReaderFactory).GetMethod(
            "CreateReaderForType", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var raw = Assert.Throws<TargetInvocationException>(() =>
            method!.Invoke(factory, new object[] { MalformedAggregateFunctionType() }));
        var inner = UnwrapTargetInvocation(raw);

        Assert.IsType<FormatException>(inner);
        Assert.Contains("missing function name", inner!.Message);
    }

    [Fact]
    public void WriterFactory_AggregateFunctionWithNullName_ThrowsFormatException_DefensiveGuard()
    {
        var factory = new ColumnWriterFactory(ColumnWriterRegistry.Default);
        var method = typeof(ColumnWriterFactory).GetMethod(
            "CreateWriterForType", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var raw = Assert.Throws<TargetInvocationException>(() =>
            method!.Invoke(factory, new object[] { MalformedAggregateFunctionType() }));
        var inner = UnwrapTargetInvocation(raw);

        Assert.IsType<FormatException>(inner);
        Assert.Contains("missing function name", inner!.Message);
    }

    [Fact]
    public void SkipperFactory_AggregateFunctionWithNullName_ThrowsFormatException_DefensiveGuard()
    {
        var factory = new ColumnSkipperFactory(ColumnSkipperRegistry.Default);
        var method = typeof(ColumnSkipperFactory).GetMethod(
            "CreateSkipperForType", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        var raw = Assert.Throws<TargetInvocationException>(() =>
            method!.Invoke(factory, new object[] { MalformedAggregateFunctionType() }));
        var inner = UnwrapTargetInvocation(raw);

        Assert.IsType<FormatException>(inner);
        Assert.Contains("missing function name", inner!.Message);
    }
}

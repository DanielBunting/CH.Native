using System.Data;
using System.Reflection;
using CH.Native.Ado;
using CH.Native.Commands;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

/// <summary>
/// Pins the ADO/native parameter-mapping contract for <see cref="DBNull"/>.
///
/// <para>
/// ADO.NET callers commonly use <c>DBNull.Value</c> to represent SQL NULL —
/// it's the documented idiom in <see cref="DbParameter.Value"/>. The native
/// layer expects plain <c>null</c>, so we translate at the ADO boundary.
/// Pre-fix: <c>DBNull.Value</c> reached <c>ClickHouseTypeMapper.InferType</c>,
/// which threw <c>NotSupportedException</c> with a generic "type DBNull is
/// not supported" message — leading users to the wrong fix (replacing
/// <c>DBNull.Value</c> with literal-null at every call site) instead of the
/// right one (relying on the library to handle it).
/// </para>
/// </summary>
public class ClickHouseDbCommandDBNullTests
{
    [Fact]
    public void BuildNativeParameters_DBNullValue_IsConvertedToNull_NoTypeInferenceThrow()
    {
        var cmd = new ClickHouseDbCommand();
        var p = (ClickHouseDbParameter)cmd.CreateParameter();
        p.ParameterName = "p1";
        p.Value = DBNull.Value;
        p.ClickHouseType = "Nullable(String)"; // explicit type — bypasses inference
        cmd.Parameters.Add(p);

        var native = InvokeBuildNativeParameters(cmd);
        Assert.Equal(1, native.Count);
        Assert.Null(native[0].Value);
    }

    [Fact]
    public void BuildNativeParameters_DBNullValue_WithoutExplicitType_DoesNotThrow()
    {
        // Without ClickHouseType set, the production code calls InferType only
        // on demand. The DBNull → null translation must happen at the ADO
        // boundary BEFORE any type inference, so even without an explicit type
        // the build step itself succeeds.
        var cmd = new ClickHouseDbCommand();
        var p = (ClickHouseDbParameter)cmd.CreateParameter();
        p.ParameterName = "p1";
        p.Value = DBNull.Value;
        cmd.Parameters.Add(p);

        var native = InvokeBuildNativeParameters(cmd);
        Assert.Null(native[0].Value);
    }

    [Fact]
    public void BuildNativeParameters_PlainNull_PassesThroughUnchanged()
    {
        var cmd = new ClickHouseDbCommand();
        var p = (ClickHouseDbParameter)cmd.CreateParameter();
        p.ParameterName = "p1";
        p.Value = null;
        p.ClickHouseType = "Nullable(String)";
        cmd.Parameters.Add(p);

        var native = InvokeBuildNativeParameters(cmd);
        Assert.Null(native[0].Value);
    }

    [Fact]
    public void BuildNativeParameters_RegularValue_PassesThroughUnchanged()
    {
        var cmd = new ClickHouseDbCommand();
        var p = (ClickHouseDbParameter)cmd.CreateParameter();
        p.ParameterName = "p1";
        p.Value = 42;
        cmd.Parameters.Add(p);

        var native = InvokeBuildNativeParameters(cmd);
        Assert.Equal(42, native[0].Value);
    }

    private static ClickHouseParameterCollection InvokeBuildNativeParameters(ClickHouseDbCommand cmd)
    {
        var method = typeof(ClickHouseDbCommand).GetMethod(
            "BuildNativeParameters",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("BuildNativeParameters not found");
        return (ClickHouseParameterCollection)method.Invoke(cmd, null)!;
    }
}

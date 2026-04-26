using System.Reflection;
using CH.Native.BulkInsert;

namespace CH.Native.SystemTests.BulkInsertFailures.Helpers;

/// <summary>
/// Reflection accessor for <see cref="BulkInserter{T}"/> private state. Tests use this
/// to pin contract behaviour the public API does not expose (buffer survival on
/// failure, <c>_completeStarted</c> after cancellation, etc.). Add no production
/// surface for this — these are pin-the-contract assertions, not runtime APIs.
/// </summary>
/// <remarks>
/// Field handles must be looked up against the *closed* type (<c>inserter.GetType()</c>)
/// rather than the open generic <c>typeof(BulkInserter&lt;&gt;)</c>: the latter returns
/// fields whose declaring type contains generic parameters, and the runtime refuses
/// to <c>GetValue</c> against such fields ("Late bound operations cannot be performed
/// on fields with types for which Type.ContainsGenericParameters is true").
/// </remarks>
internal static class InserterStateInspector
{
    private static FieldInfo Field<T>(BulkInserter<T> inserter, string name) where T : class
    {
        return inserter.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException(
                $"BulkInserter<{typeof(T).Name}>.{name} field moved or renamed.");
    }

    public static bool Completed<T>(BulkInserter<T> inserter) where T : class =>
        (bool)Field(inserter, "_completed").GetValue(inserter)!;

    public static bool CompleteStarted<T>(BulkInserter<T> inserter) where T : class =>
        (bool)Field(inserter, "_completeStarted").GetValue(inserter)!;

    public static bool Initialized<T>(BulkInserter<T> inserter) where T : class =>
        (bool)Field(inserter, "_initialized").GetValue(inserter)!;

    public static bool Disposed<T>(BulkInserter<T> inserter) where T : class =>
        (bool)Field(inserter, "_disposed").GetValue(inserter)!;
}

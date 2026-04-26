using CH.Native.Data.Types;

namespace CH.Native.Data;

/// <summary>
/// Centralised semantic check enforcing that <c>Nullable(T)</c> wraps only the inner
/// types ClickHouse itself accepts at schema-creation time.
/// </summary>
/// <remarks>
/// ClickHouse rejects all container types inside <c>Nullable</c> with
/// <c>ILLEGAL_TYPE_OF_ARGUMENT</c> (code 43, "Nested type X cannot be inside Nullable
/// type"). The factories used to silently construct broken wrapper compositions for
/// these — wire bytes that no real schema can ever consume. Failing fast here matches
/// the server and keeps the writer/reader/skipper paths free of unreachable corners.
///
/// Probed against ClickHouse 26.2.15.4. The list mirrors the server's restriction;
/// see <see cref="CH.Native.Tests.Integration.BulkInsertCompositeTypeTests"/> for the
/// schema-level guard tests that cross-check this behaviour.
/// </remarks>
internal static class NullableInnerValidator
{
    public static void EnsureAllowedInsideNullable(ClickHouseType inner)
    {
        if (!IsForbidden(inner, out var reason))
            return;

        throw new FormatException(
            $"Nullable({inner.OriginalTypeName}) is not allowed: nested type {inner.OriginalTypeName} cannot be inside Nullable type. {reason}");
    }

    private static bool IsForbidden(ClickHouseType inner, out string reason)
    {
        if (inner.IsArray)         { reason = "Use Array(Nullable(...)) instead — empty array is the conventional NULL sentinel for collection columns."; return true; }
        if (inner.IsMap)           { reason = "Maps cannot be NULL; use an empty map as the sentinel value."; return true; }
        if (inner.IsTuple)         { reason = "Tuples cannot be NULL; mark individual fields as Nullable inside the Tuple."; return true; }
        if (inner.IsLowCardinality){ reason = "Use LowCardinality(Nullable(...)) instead — the bitmap lives inside the dictionary in ClickHouse's layout."; return true; }
        if (inner.IsNested)        { reason = "Nested expands to parallel arrays; nullability must live on the inner column types."; return true; }
        // NOTE: Nullable(JSON) IS accepted by ClickHouse 26.2 (server treats the JSON
        // column as a self-describing container that absorbs the bitmap upstream).
        // Do not reject it here.
        if (inner.IsNullable)      { reason = "Nullable cannot wrap Nullable — flatten to a single Nullable layer."; return true; }
        if (inner.IsDynamic)       { reason = "Dynamic already represents NULL via its discriminator."; return true; }
        if (inner.IsVariant)       { reason = "Variant already represents NULL via its discriminator."; return true; }

        reason = string.Empty;
        return false;
    }
}

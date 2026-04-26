using CH.Native.Data.Types;

namespace CH.Native.Data;

/// <summary>
/// Centralised semantic check enforcing that <c>LowCardinality(T)</c> wraps only the
/// inner types ClickHouse itself accepts at schema-creation time.
/// </summary>
/// <remarks>
/// ClickHouse rejects every container type inside <c>LowCardinality</c> with
/// <c>ILLEGAL_TYPE_OF_ARGUMENT</c> (code 43). Allowed forms are scalar types
/// (Int*, Float*, FixedString, String, Date*, Decimal*, Enum*, IPv4/6, UUID) and the
/// canonical wrapper <c>LowCardinality(Nullable(&lt;scalar&gt;))</c>. The factories
/// previously only blocked Dynamic and Variant inners; this validator covers the rest.
///
/// Probed against ClickHouse 26.2.15.4. The list mirrors the server's restriction;
/// see <see cref="CH.Native.Tests.Integration.BulkInsertCompositeTypeTests"/> for the
/// schema-level guard tests that cross-check this behaviour.
/// </remarks>
internal static class LowCardinalityInnerValidator
{
    public static void EnsureAllowedInsideLowCardinality(ClickHouseType inner)
    {
        if (!IsForbidden(inner, out var reason))
            return;

        throw new FormatException(
            $"LowCardinality({inner.OriginalTypeName}) is not allowed: nested type {inner.OriginalTypeName} cannot be inside LowCardinality. {reason}");
    }

    private static bool IsForbidden(ClickHouseType inner, out string reason)
    {
        // The canonical LowCardinality(Nullable(scalar)) form is allowed; the factory
        // strips one Nullable layer when building the dictionary. Peek through it so
        // we still validate the underlying base type against the same forbidden list.
        var typeToCheck = inner.IsNullable && inner.TypeArguments.Count == 1
            ? inner.TypeArguments[0]
            : inner;

        if (typeToCheck.IsArray)          { reason = "Use Array(LowCardinality(...)) instead — LowCardinality lives inside the element type, not around the array."; return true; }
        if (typeToCheck.IsMap)            { reason = "Maps store variable-width key/value blobs that don't compress through a dictionary."; return true; }
        if (typeToCheck.IsTuple)          { reason = "Apply LowCardinality to individual fields inside the Tuple instead."; return true; }
        if (typeToCheck.IsNested)         { reason = "Nested expands to parallel arrays; LowCardinality must live on the inner column types."; return true; }
        if (typeToCheck.IsLowCardinality) { reason = "LowCardinality cannot wrap LowCardinality — flatten to a single dictionary layer."; return true; }
        if (typeToCheck.IsDynamic)        { reason = "Dynamic stores variable per-row types and is not dictionary-friendly."; return true; }
        if (typeToCheck.IsVariant)        { reason = "Variant arms have different types and cannot share a single dictionary."; return true; }
        if (typeToCheck.BaseName == "JSON") { reason = "JSON is a self-describing container; LowCardinality compresses fixed-width values."; return true; }
        // Reject Nullable here too: by this point we've already peeked through one
        // Nullable layer, so any remaining Nullable means LowCardinality(Nullable(Nullable(...))).
        if (typeToCheck.IsNullable)       { reason = "LowCardinality(Nullable(Nullable(...))) is not allowed — flatten to a single Nullable layer."; return true; }

        reason = string.Empty;
        return false;
    }
}

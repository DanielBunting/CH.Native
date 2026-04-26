using CH.Native.Data.Types;

namespace CH.Native.Data;

/// <summary>
/// Centralised semantic check enforcing that <c>Variant(...)</c> arms are types
/// ClickHouse itself accepts at schema-creation time.
/// </summary>
/// <remarks>
/// ClickHouse 26.2 rejects four arm shapes inside <c>Variant</c>:
/// <list type="bullet">
/// <item><c>Variant</c> (nested Variants are not allowed)</item>
/// <item><c>Nullable(...)</c> (Variant has its own NULL discriminator)</item>
/// <item><c>LowCardinality(Nullable(...))</c> (the dictionary's null entry would conflict
///   with the Variant discriminator)</item>
/// <item><c>Dynamic</c> (Dynamic and Variant overlap conceptually)</item>
/// </list>
/// Other compositions including <c>Array</c>, <c>Map</c>, <c>Tuple</c>, <c>Nested</c>,
/// <c>JSON</c>, and <c>LowCardinality(&lt;scalar&gt;)</c> are accepted by the server.
///
/// The previous inline checks rejected <em>all</em> <c>LowCardinality</c> arms — too
/// strict, blocking legitimate <c>Variant(LowCardinality(String), Int32)</c> schemas.
/// This validator matches the server's actual behaviour as probed.
///
/// Probed against ClickHouse 26.2.15.4. See
/// <see cref="CH.Native.Tests.Integration.BulkInsertCompositeTypeTests"/> for the
/// schema-level guard tests that cross-check this behaviour.
/// </remarks>
internal static class VariantArmValidator
{
    public static void EnsureAllowedAsVariantArm(ClickHouseType arm)
    {
        if (!IsForbidden(arm, out var reason))
            return;

        throw new FormatException(
            $"Variant arm '{arm.OriginalTypeName}' is not allowed: {reason}");
    }

    private static bool IsForbidden(ClickHouseType arm, out string reason)
    {
        if (arm.IsVariant)
        {
            reason = "nested Variant types are not allowed; flatten the arms into a single Variant.";
            return true;
        }

        if (arm.IsNullable)
        {
            reason = "Variant has its own NULL discriminator — drop the Nullable wrapper.";
            return true;
        }

        if (arm.IsLowCardinality
            && arm.TypeArguments.Count == 1
            && arm.TypeArguments[0].IsNullable)
        {
            reason = "LowCardinality(Nullable(...)) arms are rejected by ClickHouse — use LowCardinality(<scalar>) or wrap differently.";
            return true;
        }

        if (arm.IsDynamic)
        {
            reason = "Dynamic and Variant overlap conceptually; pick one.";
            return true;
        }

        reason = string.Empty;
        return false;
    }
}

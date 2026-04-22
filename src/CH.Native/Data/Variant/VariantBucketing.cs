using System.Buffers;

namespace CH.Native.Data.Variant;

/// <summary>
/// Shared helpers for the Variant / Dynamic / JSON-v3 bucketing algorithm.
/// </summary>
/// <remarks>
/// ClickHouse's Variant encoding groups all non-NULL rows by discriminator into per-arm
/// sub-columns, then reads each arm's values back into row order using the discriminator
/// stream. These helpers implement the counting and stitching steps so that the Variant,
/// Dynamic and JSON readers can share the logic.
/// </remarks>
internal static class VariantBucketing
{
    /// <summary>
    /// Counts rows per discriminator arm. Discriminator value <see cref="ClickHouseVariant.NullDiscriminator"/>
    /// is ignored (it contributes nothing to any arm).
    /// </summary>
    /// <param name="discriminators">The row discriminator stream.</param>
    /// <param name="armCount">Number of declared arms; values outside [0, armCount) except 255 are rejected.</param>
    /// <returns>An int[armCount] of per-arm counts. Caller owns the array (not pooled).</returns>
    public static int[] CountPerArm(ReadOnlySpan<byte> discriminators, int armCount)
    {
        var counts = new int[armCount];
        for (int i = 0; i < discriminators.Length; i++)
        {
            var disc = discriminators[i];
            if (disc == ClickHouseVariant.NullDiscriminator)
                continue;
            if (disc >= armCount)
                throw new InvalidOperationException(
                    $"Discriminator {disc} out of range for Variant with {armCount} arms at row {i}.");
            counts[disc]++;
        }
        return counts;
    }

    /// <summary>
    /// Rents and fills a cursor array — one running index per arm, all initialised to zero.
    /// Used by stitching to pull values in row order from the per-arm packed columns.
    /// </summary>
    /// <param name="armCount">Number of arms (excluding the implicit NULL arm).</param>
    public static int[] RentCursors(int armCount)
    {
        var cursors = ArrayPool<int>.Shared.Rent(armCount);
        Array.Clear(cursors, 0, armCount);
        return cursors;
    }
}

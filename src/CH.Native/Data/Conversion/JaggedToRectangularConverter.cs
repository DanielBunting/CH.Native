using CH.Native.Exceptions;

namespace CH.Native.Data.Conversion;

/// <summary>
/// Converts jagged arrays returned by the column readers (<c>T[][]</c>,
/// <c>T[][][]</c>, …) into rectangular form (<c>T[,]</c>, <c>T[,,]</c>, …)
/// at the read boundary. Validates that all inner arrays have a uniform
/// length and throws <see cref="ClickHouseTypeConversionException"/> on
/// mismatch with the offending row index in the message.
/// </summary>
public static class JaggedToRectangularConverter
{
    /// <summary>
    /// Converts a jagged 2D array to a rectangular 2D array. All inner
    /// arrays must have the same length.
    /// </summary>
    public static T[,] ToRectangular2D<T>(T[][] jagged)
    {
        ArgumentNullException.ThrowIfNull(jagged);

        int outer = jagged.Length;
        if (outer == 0)
            return new T[0, 0];

        int inner = jagged[0]?.Length ?? 0;
        for (int i = 1; i < outer; i++)
        {
            int actual = jagged[i]?.Length ?? 0;
            if (actual != inner)
                throw new ClickHouseTypeConversionException(
                    $"Cannot convert jagged array to rectangular T[,]: row {i} has length {actual}, " +
                    $"expected {inner} (uniform with row 0).",
                    rowIndex: i, expectedLength: inner, actualLength: actual);
        }

        var result = new T[outer, inner];
        for (int i = 0; i < outer; i++)
        {
            var row = jagged[i];
            for (int j = 0; j < inner; j++)
                result[i, j] = row[j];
        }
        return result;
    }

    /// <summary>
    /// Converts a jagged 3D array to a rectangular 3D array. All sub-arrays
    /// at each level must have uniform lengths.
    /// </summary>
    public static T[,,] ToRectangular3D<T>(T[][][] jagged)
    {
        ArgumentNullException.ThrowIfNull(jagged);

        int d0 = jagged.Length;
        if (d0 == 0)
            return new T[0, 0, 0];

        int d1 = jagged[0]?.Length ?? 0;
        int d2 = d1 > 0 ? (jagged[0][0]?.Length ?? 0) : 0;

        for (int i = 0; i < d0; i++)
        {
            var plane = jagged[i] ?? Array.Empty<T[]>();
            if (plane.Length != d1)
                throw new ClickHouseTypeConversionException(
                    $"Cannot convert jagged array to rectangular T[,,]: outer index {i} has length {plane.Length}, " +
                    $"expected {d1} (uniform with index 0).",
                    rowIndex: i, expectedLength: d1, actualLength: plane.Length);
            for (int j = 0; j < d1; j++)
            {
                int actual = plane[j]?.Length ?? 0;
                if (actual != d2)
                    throw new ClickHouseTypeConversionException(
                        $"Cannot convert jagged array to rectangular T[,,]: inner [{i},{j}] has length {actual}, " +
                        $"expected {d2} (uniform with [0,0]).",
                        rowIndex: i, expectedLength: d2, actualLength: actual);
            }
        }

        var result = new T[d0, d1, d2];
        for (int i = 0; i < d0; i++)
        for (int j = 0; j < d1; j++)
        {
            var row = jagged[i][j];
            for (int k = 0; k < d2; k++)
                result[i, j, k] = row[k];
        }
        return result;
    }

    /// <summary>
    /// Reflection-based conversion for arbitrary-rank rectangular targets.
    /// </summary>
    /// <param name="jagged">A jagged array whose nesting depth matches the target rank.</param>
    /// <param name="targetType">A multidim array type (e.g. <c>typeof(int[,,])</c>).</param>
    public static Array ToRectangular(Array jagged, Type targetType)
    {
        ArgumentNullException.ThrowIfNull(jagged);
        ArgumentNullException.ThrowIfNull(targetType);
        if (!targetType.IsArray || targetType.GetArrayRank() < 2)
            throw new ArgumentException(
                $"Target type must be a rectangular array (rank >= 2), got '{targetType.FullName}'.",
                nameof(targetType));

        int rank = targetType.GetArrayRank();
        var scalarType = targetType.GetElementType()!;

        var lengths = new int[rank];
        MeasureUniform(jagged, lengths, depth: 0, rank, outerIndex: -1);

        var result = Array.CreateInstance(scalarType, lengths);
        if (lengths[0] == 0)
            return result;

        var indices = new int[rank];
        Copy(jagged, result, indices, depth: 0, rank);
        return result;
    }

    private static void MeasureUniform(object? node, int[] lengths, int depth, int rank, int outerIndex)
    {
        if (depth == rank)
            return;

        if (node is not Array arr)
            throw new ClickHouseTypeConversionException(
                $"Expected nested array at depth {depth} but found '{(node?.GetType().FullName ?? "null")}'.");

        if (depth == 0)
        {
            lengths[depth] = arr.Length;
        }
        else if (arr.Length != lengths[depth])
        {
            throw new ClickHouseTypeConversionException(
                $"Cannot convert jagged array to rectangular: outer index {outerIndex} has inner length {arr.Length} at depth {depth}, expected {lengths[depth]} (uniform with index 0).",
                rowIndex: outerIndex, expectedLength: lengths[depth], actualLength: arr.Length);
        }

        if (depth < rank - 1 && arr.Length > 0)
        {
            // Probe child 0 to seed the next dimension's expected length.
            var first = arr.GetValue(0);
            if (first is Array firstArr)
            {
                lengths[depth + 1] = firstArr.Length;
            }
            else if (depth + 1 < rank)
            {
                throw new ClickHouseTypeConversionException(
                    $"Expected nested array at depth {depth + 1} but found '{(first?.GetType().FullName ?? "null")}'.");
            }

            for (int i = 0; i < arr.Length; i++)
            {
                // First level of recursion captures dim-0 index for ragged-error reporting.
                int passedIndex = depth == 0 ? i : outerIndex;
                MeasureUniform(arr.GetValue(i), lengths, depth + 1, rank, passedIndex);
            }
        }
    }

    private static void Copy(object? node, Array result, int[] indices, int depth, int rank)
    {
        if (depth == rank - 1)
        {
            var arr = (Array)node!;
            for (int i = 0; i < arr.Length; i++)
            {
                indices[depth] = i;
                result.SetValue(arr.GetValue(i), indices);
            }
            return;
        }

        var outer = (Array)node!;
        for (int i = 0; i < outer.Length; i++)
        {
            indices[depth] = i;
            Copy(outer.GetValue(i), result, indices, depth + 1, rank);
        }
    }
}

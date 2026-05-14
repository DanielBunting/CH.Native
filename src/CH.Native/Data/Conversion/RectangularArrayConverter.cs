namespace CH.Native.Data.Conversion;

/// <summary>
/// Converts C# rectangular multidimensional arrays (<c>T[,]</c>, <c>T[,,]</c>, …)
/// to jagged form (<c>T[][]</c>, <c>T[][][]</c>, …) at the library boundary.
/// </summary>
/// <remarks>
/// ClickHouse has no native rectangular type; on the wire a rectangular array is
/// the same <c>Array(Array(T))</c> as a jagged one. Converting at the POCO /
/// parameter boundary keeps the existing column-writer pipeline unchanged and
/// localizes the rectangular handling in a single place.
/// </remarks>
public static class RectangularArrayConverter
{
    /// <summary>
    /// Converts a rank-2 rectangular array to jagged form.
    /// </summary>
    public static T[][] To2DJagged<T>(T[,] rect)
    {
        ArgumentNullException.ThrowIfNull(rect);

        int outer = rect.GetLength(0);
        int inner = rect.GetLength(1);
        var result = new T[outer][];
        for (int i = 0; i < outer; i++)
        {
            var row = new T[inner];
            for (int j = 0; j < inner; j++)
                row[j] = rect[i, j];
            result[i] = row;
        }
        return result;
    }

    /// <summary>
    /// Converts a rank-3 rectangular array to jagged form.
    /// </summary>
    public static T[][][] To3DJagged<T>(T[,,] rect)
    {
        ArgumentNullException.ThrowIfNull(rect);

        int d0 = rect.GetLength(0);
        int d1 = rect.GetLength(1);
        int d2 = rect.GetLength(2);
        var result = new T[d0][][];
        for (int i = 0; i < d0; i++)
        {
            var plane = new T[d1][];
            for (int j = 0; j < d1; j++)
            {
                var row = new T[d2];
                for (int k = 0; k < d2; k++)
                    row[k] = rect[i, j, k];
                plane[j] = row;
            }
            result[i] = plane;
        }
        return result;
    }

    /// <summary>
    /// Converts an arbitrary-rank rectangular array to jagged form via reflection.
    /// </summary>
    /// <remarks>
    /// For ranks 2 and 3, prefer the typed <see cref="To2DJagged{T}"/> /
    /// <see cref="To3DJagged{T}"/> overloads — they avoid boxing on every cell.
    /// </remarks>
    public static Array ToJagged(Array rect)
    {
        ArgumentNullException.ThrowIfNull(rect);

        int rank = rect.Rank;
        if (rank == 1)
            return rect;

        var scalarType = rect.GetType().GetElementType()!;
        var indices = new int[rank];
        return BuildJagged(rect, scalarType, depth: 0, indices);
    }

    private static Array BuildJagged(Array rect, Type scalarType, int depth, int[] indices)
    {
        int rank = rect.Rank;
        int len = rect.GetLength(depth);

        if (depth == rank - 1)
        {
            var leaf = Array.CreateInstance(scalarType, len);
            for (int i = 0; i < len; i++)
            {
                indices[depth] = i;
                leaf.SetValue(rect.GetValue(indices), i);
            }
            return leaf;
        }

        var innerType = scalarType;
        for (int i = 0; i < rank - depth - 1; i++)
            innerType = innerType.MakeArrayType();

        var result = Array.CreateInstance(innerType, len);
        for (int i = 0; i < len; i++)
        {
            indices[depth] = i;
            result.SetValue(BuildJagged(rect, scalarType, depth + 1, indices), i);
        }
        return result;
    }
}

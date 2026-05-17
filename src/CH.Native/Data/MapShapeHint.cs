namespace CH.Native.Data;

/// <summary>
/// Carries the per-column Map-shape decision from a typed query call site
/// (driven by the row type's properties) down to the column reader factory.
/// Columns without a hint fall back to the supplied <see cref="Fallback"/>,
/// which is <see cref="MapShape.Dictionary"/> by default.
/// </summary>
/// <remarks>
/// Per-column hints apply to the <strong>top-level</strong> column only; nested
/// Maps inside Array/Tuple/Nullable wrappers fall back to <see cref="Fallback"/>.
/// </remarks>
internal sealed class MapShapeHint
{
    /// <summary>
    /// The default hint: no per-column overrides, fallback to
    /// <see cref="MapShape.Dictionary"/>. Used when no typed call site has
    /// injected a hint for the current operation.
    /// </summary>
    public static readonly MapShapeHint Default = new(perColumn: null, MapShape.Dictionary);

    /// <summary>
    /// Convenience hint that forces every column to <see cref="MapShape.Entries"/>.
    /// Used by scalar typed call sites where <c>T = ClickHouseMap&lt;,&gt;</c>
    /// or an entries-shaped collection.
    /// </summary>
    public static readonly MapShapeHint AllEntries = new(perColumn: null, MapShape.Entries);

    private readonly IReadOnlyDictionary<string, MapShape>? _perColumn;

    public MapShapeHint(IReadOnlyDictionary<string, MapShape>? perColumn, MapShape fallback = MapShape.Dictionary)
    {
        _perColumn = perColumn;
        Fallback = fallback == MapShape.Default ? MapShape.Dictionary : fallback;
    }

    /// <summary>
    /// Gets the shape used for columns that have no per-column hint (or whose
    /// per-column hint is <see cref="MapShape.Default"/>).
    /// </summary>
    public MapShape Fallback { get; }

    /// <summary>
    /// Resolves the shape for a specific top-level column. Falls back to
    /// <see cref="Fallback"/> when no per-column override is recorded or the
    /// override is <see cref="MapShape.Default"/>.
    /// </summary>
    public MapShape Resolve(string columnName)
    {
        if (_perColumn is not null
            && _perColumn.TryGetValue(columnName, out var shape)
            && shape != MapShape.Default)
        {
            return shape;
        }

        return Fallback;
    }
}

using System.Collections;

namespace CH.Native.Data;

/// <summary>
/// A lossless materialisation of a ClickHouse <c>Map(K, V)</c> column value.
/// </summary>
/// <remarks>
/// <para>
/// ClickHouse explicitly permits a <c>Map(K, V)</c> to contain duplicate keys
/// (per the ClickHouse documentation: <em>"maps are not unique in ClickHouse,
/// i.e. a map can contain two elements with the same key"</em>). The default
/// CH.Native materialisation as <see cref="Dictionary{TKey, TValue}"/> collapses
/// duplicates last-wins. <see cref="ClickHouseMap{TKey, TValue}"/> preserves every
/// entry in wire order, so callers who need fidelity can opt in by declaring
/// this type instead.
/// </para>
/// <para>
/// The type exposes both <see cref="IReadOnlyList{T}"/> (entries in order) and
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> (key lookups) views. Lookups
/// via <see cref="this[TKey]"/> / <see cref="TryGetValue"/> / <see cref="ContainsKey"/>
/// are <strong>first-wins</strong> linear scans — predictable and matches how
/// ClickHouse's own <c>m[k]</c> behaves in documented cases. Use
/// <see cref="ToDictionary"/> for an explicit last-wins collapse or
/// <see cref="ToLookup"/> to group duplicates.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The key type. Must be non-null per ClickHouse semantics.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class ClickHouseMap<TKey, TValue>
    : IReadOnlyList<KeyValuePair<TKey, TValue>>,
      IReadOnlyDictionary<TKey, TValue>
    where TKey : notnull
{
    private readonly KeyValuePair<TKey, TValue>[] _entries;
    // 0 = unknown, 1 = no duplicates, 2 = duplicates. Lazy because the HashSet
    // probe is the most expensive thing the ctor would do, and most readers
    // never touch HasDuplicateKeys.
    private int _hasDuplicateKeys;

    /// <summary>
    /// Initializes a new <see cref="ClickHouseMap{TKey, TValue}"/> wrapping the supplied entries array.
    /// The array is not copied; callers must not mutate it after construction.
    /// </summary>
    /// <param name="entries">The entries in wire order, including any duplicate keys.</param>
    public ClickHouseMap(KeyValuePair<TKey, TValue>[] entries)
    {
        _entries = entries ?? throw new ArgumentNullException(nameof(entries));
    }

    /// <summary>
    /// Initializes a new <see cref="ClickHouseMap{TKey, TValue}"/> by materialising the source enumerable
    /// exactly once into an internal array.
    /// </summary>
    /// <param name="entries">The entries in wire order, including any duplicate keys.</param>
    public ClickHouseMap(IEnumerable<KeyValuePair<TKey, TValue>> entries)
    {
        if (entries is null) throw new ArgumentNullException(nameof(entries));
        _entries = entries as KeyValuePair<TKey, TValue>[] ?? entries.ToArray();
    }

    /// <summary>
    /// Gets the number of entries, counting duplicate-key entries separately.
    /// </summary>
    public int Count => _entries.Length;

    /// <summary>
    /// Gets the entry at the specified position in wire order.
    /// </summary>
    public KeyValuePair<TKey, TValue> this[int index] => _entries[index];

    /// <summary>
    /// Gets the value associated with the <em>first</em> entry whose key equals <paramref name="key"/>.
    /// Throws <see cref="KeyNotFoundException"/> if no such entry exists.
    /// </summary>
    public TValue this[TKey key]
    {
        get
        {
            if (TryGetValue(key, out var value)) return value;
            throw new KeyNotFoundException($"The key '{key}' was not present in the map.");
        }
    }

    /// <summary>
    /// Returns the keys in wire order, including duplicates.
    /// </summary>
    public IEnumerable<TKey> Keys
    {
        get
        {
            foreach (var entry in _entries) yield return entry.Key;
        }
    }

    /// <summary>
    /// Returns the values in wire order, paired with their keys including duplicates.
    /// </summary>
    public IEnumerable<TValue> Values
    {
        get
        {
            foreach (var entry in _entries) yield return entry.Value;
        }
    }

    /// <summary>
    /// True when the map contains at least two entries sharing the same key.
    /// Computed lazily on first access and cached.
    /// </summary>
    public bool HasDuplicateKeys
    {
        get
        {
            var cached = _hasDuplicateKeys;
            if (cached != 0) return cached == 2;
            var hasDupes = ComputeHasDuplicateKeys(_entries);
            _hasDuplicateKeys = hasDupes ? 2 : 1;
            return hasDupes;
        }
    }

    /// <summary>
    /// Returns true if any entry's key equals <paramref name="key"/>.
    /// </summary>
    public bool ContainsKey(TKey key)
    {
        foreach (var entry in _entries)
        {
            if (EqualityComparer<TKey>.Default.Equals(entry.Key, key)) return true;
        }
        return false;
    }

    /// <summary>
    /// Returns the value of the <em>first</em> entry whose key equals <paramref name="key"/>.
    /// </summary>
    public bool TryGetValue(TKey key, out TValue value)
    {
        foreach (var entry in _entries)
        {
            if (EqualityComparer<TKey>.Default.Equals(entry.Key, key))
            {
                value = entry.Value;
                return true;
            }
        }
        value = default!;
        return false;
    }

    /// <summary>
    /// Returns a span view over the backing array. Mutating the span mutates this map.
    /// </summary>
    public ReadOnlySpan<KeyValuePair<TKey, TValue>> AsSpan() => _entries;

    /// <summary>
    /// Collapses duplicate-key entries into a <see cref="Dictionary{TKey, TValue}"/>
    /// using last-wins semantics — matching what the default <c>Map(K, V)</c>
    /// reader would have produced.
    /// </summary>
    public Dictionary<TKey, TValue> ToDictionary()
    {
        var dict = new Dictionary<TKey, TValue>(_entries.Length);
        foreach (var entry in _entries)
        {
            dict[entry.Key] = entry.Value;
        }
        return dict;
    }

    /// <summary>
    /// Groups entries by key, preserving all values per key in wire order.
    /// </summary>
    public ILookup<TKey, TValue> ToLookup()
        => _entries.ToLookup(e => e.Key, e => e.Value);

    /// <inheritdoc />
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        foreach (var entry in _entries) yield return entry;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static bool ComputeHasDuplicateKeys(KeyValuePair<TKey, TValue>[] entries)
    {
        if (entries.Length < 2) return false;

        var seen = new HashSet<TKey>(entries.Length);
        foreach (var entry in entries)
        {
            if (!seen.Add(entry.Key)) return true;
        }
        return false;
    }
}

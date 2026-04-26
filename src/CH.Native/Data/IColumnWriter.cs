using CH.Native.Protocol;

namespace CH.Native.Data;

/// <summary>
/// Interface for writing column data to the ClickHouse protocol.
/// </summary>
public interface IColumnWriter
{
    /// <summary>
    /// Gets the ClickHouse type name this writer handles.
    /// </summary>
    string TypeName { get; }

    /// <summary>
    /// Gets the CLR type that this writer accepts.
    /// </summary>
    Type ClrType { get; }

    /// <summary>
    /// Writes any column-level state prefix (e.g. LowCardinality's
    /// KeysSerializationVersion) that ClickHouse expects before the main data phase.
    /// Most writers have nothing to emit; composite writers recursively invoke their
    /// inner writers' prefixes so nested state (e.g. Array(LowCardinality(T))) emits
    /// its version before the outer composite's structural bytes.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    void WritePrefix(ref ProtocolWriter writer) { }

    /// <summary>
    /// Writes column data for the specified values.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="values">The column values to write.</param>
    void WriteColumn(ref ProtocolWriter writer, object?[] values);

    /// <summary>
    /// Writes a single value.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="value">The value to write.</param>
    void WriteValue(ref ProtocolWriter writer, object? value);
}

/// <summary>
/// Generic interface for strongly-typed column writing.
/// </summary>
/// <typeparam name="T">The CLR type to write.</typeparam>
public interface IColumnWriter<T> : IColumnWriter
{
    /// <summary>
    /// Writes column data for the specified typed values.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="values">The typed values to write.</param>
    void WriteColumn(ref ProtocolWriter writer, T[] values);

    /// <summary>
    /// Writes a single typed value.
    /// </summary>
    /// <param name="writer">The protocol writer.</param>
    /// <param name="value">The typed value to write.</param>
    void WriteValue(ref ProtocolWriter writer, T value);

    /// <summary>
    /// Placeholder value the Nullable wrapper writes under a "row is null"
    /// bitmap byte. ClickHouse's wire format requires bytes for every row
    /// (the bitmap is read separately), so wrapped writers need a benign
    /// value to emit for null slots — empty string for String, zero-padded
    /// buffer for FixedString, empty array for Array, etc. Default: throw,
    /// so writers that don't override (value-type writers, structural
    /// composites) signal that they don't participate in Nullable-wrap
    /// substitution and should be wrapped by a different mechanism (the
    /// struct-T <see cref="ColumnWriters.NullableColumnWriter{T}"/> uses
    /// <c>default(T)</c> directly).
    /// </summary>
    T NullPlaceholder => throw new NotSupportedException(
        $"{GetType().Name} does not declare a Nullable-bitmap placeholder. " +
        $"Reference-type writers used inside Nullable(T) must override NullPlaceholder.");
}

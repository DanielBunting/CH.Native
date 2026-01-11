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
}

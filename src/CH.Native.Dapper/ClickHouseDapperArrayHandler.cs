using System.Data;
using Dapper;

namespace CH.Native.Dapper;

/// <summary>
/// Dapper type handler that binds a CLR array to an ADO.NET parameter verbatim,
/// preventing Dapper's default list-expansion behaviour from rewriting the SQL
/// into a tuple literal.
/// </summary>
/// <remarks>
/// <para>
/// Without this handler, <c>new { ids = new[] { 1, 2, 3 } }</c> is expanded by
/// Dapper into individual parameters and the SQL is rewritten into
/// <c>... hasAny([x], (_CAST(1,'Int32'), _CAST(2,'Int32'), _CAST(3,'Int32')))</c>
/// — which ClickHouse rejects because the second argument is a
/// <c>Tuple(Int32, ...)</c>, not an <c>Array(Int32)</c>.
/// </para>
/// <para>
/// With the handler registered, Dapper calls <see cref="SetValue"/> with the
/// raw array and we set it directly on the <see cref="IDbDataParameter"/>. The
/// CH.Native ADO.NET layer already knows how to serialise arrays (see
/// <c>ClickHouseTypeMapper.InferTypeFromClrType</c> and
/// <c>ParameterSerializer.SerializeArray</c>) so the value reaches the wire as
/// a proper <c>Array(T)</c>.
/// </para>
/// </remarks>
public sealed class ClickHouseDapperArrayHandler<T> : SqlMapper.TypeHandler<T[]>
{
    /// <inheritdoc />
    public override void SetValue(IDbDataParameter parameter, T[]? value)
    {
        // Hand the array straight to the ADO.NET parameter. The CH.Native
        // parameter collection infers the ClickHouse Array(T) type from the CLR
        // array type at send time.
        parameter.Value = (object?)value ?? DBNull.Value;
    }

    /// <inheritdoc />
    public override T[]? Parse(object? value)
    {
        // Parse is invoked when Dapper maps a row value back to a typed column
        // or field. For array parameters on the way out (parameters only),
        // Parse is not called; we only need SetValue. If the library ever grows
        // support for reading arrays into typed POCOs via Dapper, implement the
        // row-materialisation path here.
        return value is T[] typed ? typed : null;
    }
}

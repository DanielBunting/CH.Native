using System.Buffers;
using CH.Native.Data;
using CH.Native.Exceptions;
using CH.Native.Mapping;
using Xunit;

namespace CH.Native.Tests.Unit.Mapping;

public class ReflectionTypedRowMapperTests
{
    private class Rect2DRow
    {
        public int Id { get; set; }
        public int[,] Grid { get; set; } = new int[0, 0];
    }

    private class Rect3DRow
    {
        public int Id { get; set; }
        public int[,,] Cube { get; set; } = new int[0, 0, 0];
    }

    private class RectString2DRow
    {
        public int Id { get; set; }
        public string[,] Tags { get; set; } = new string[0, 0];
    }

    [Fact]
    public void MapRow_RectangularInt2D_ConvertsJaggedToRect()
    {
        // Column reader materialized Array(Array(Int32)) as int[][] — the typed
        // fast-path Setter must route through JaggedToRectangularConverter
        // before the property assignment.
        var idColumn = new TypedColumn<int>(new[] { 7 });
        var gridColumn = new TypedColumn<int[][]>(new[]
        {
            new[] { new[] { 1, 2, 3 }, new[] { 4, 5, 6 } }
        });

        var mapper = TypedRowMapperFactory.GetMapper<Rect2DRow>(new[] { "Id", "Grid" });
        var row = mapper.MapRow(new ITypedColumn[] { idColumn, gridColumn }, 0);

        Assert.Equal(7, row.Id);
        Assert.Equal(2, row.Grid.GetLength(0));
        Assert.Equal(3, row.Grid.GetLength(1));
        Assert.Equal(1, row.Grid[0, 0]);
        Assert.Equal(6, row.Grid[1, 2]);
    }

    [Fact]
    public void MapRow_RectangularInt2D_RaggedSource_Throws()
    {
        var idColumn = new TypedColumn<int>(new[] { 1 });
        var gridColumn = new TypedColumn<int[][]>(new[]
        {
            new[] { new[] { 1, 2 }, new[] { 3 } } // ragged
        });

        var mapper = TypedRowMapperFactory.GetMapper<Rect2DRow>(new[] { "Id", "Grid" });

        Assert.Throws<ClickHouseTypeConversionException>(
            () => mapper.MapRow(new ITypedColumn[] { idColumn, gridColumn }, 0));
    }

    [Fact]
    public void MapRow_RectangularInt3D_ConvertsCorrectly()
    {
        var idColumn = new TypedColumn<int>(new[] { 2 });
        var cubeColumn = new TypedColumn<int[][][]>(new[]
        {
            new[]
            {
                new[] { new[] { 1, 2 }, new[] { 3, 4 } },
                new[] { new[] { 5, 6 }, new[] { 7, 8 } },
            }
        });

        var mapper = TypedRowMapperFactory.GetMapper<Rect3DRow>(new[] { "Id", "Cube" });
        var row = mapper.MapRow(new ITypedColumn[] { idColumn, cubeColumn }, 0);

        Assert.Equal(2, row.Id);
        Assert.Equal(2, row.Cube.GetLength(0));
        Assert.Equal(2, row.Cube.GetLength(1));
        Assert.Equal(2, row.Cube.GetLength(2));
        Assert.Equal(1, row.Cube[0, 0, 0]);
        Assert.Equal(8, row.Cube[1, 1, 1]);
    }

    [Fact]
    public void MapRow_RectangularString2D_ConvertsJaggedToRect()
    {
        // Reference-typed element to exercise the non-int branch of the
        // expression-tree Convert in the rectangular-property setter.
        var idColumn = new TypedColumn<int>(new[] { 5 });
        var tagsColumn = new TypedColumn<string[][]>(new[]
        {
            new[] { new[] { "a", "b" }, new[] { "c", "d" } }
        });

        var mapper = TypedRowMapperFactory.GetMapper<RectString2DRow>(new[] { "Id", "Tags" });
        var row = mapper.MapRow(new ITypedColumn[] { idColumn, tagsColumn }, 0);

        Assert.Equal(5, row.Id);
        Assert.Equal(2, row.Tags.GetLength(0));
        Assert.Equal(2, row.Tags.GetLength(1));
        Assert.Equal("a", row.Tags[0, 0]);
        Assert.Equal("d", row.Tags[1, 1]);
    }

    // ── scalar targets ──────────────────────────────────────────────────────
    // The mapper's scalar branch binds T to the row's FIRST column instead of
    // running per-property setters. It exists because a scalar T has no writable
    // properties: without it the setter array is empty, MapRow hands back
    // `new T()`, and every row silently decodes to default(T). The tests below
    // pin the parts the query-level tests don't reach — the type-recognition
    // legs that aren't enums (DateOnly, TimeOnly), and the three conversion
    // outcomes inside the built map: exact type, enum widening, ChangeType.

    private enum Status
    {
        Unknown = 0,
        Active = 1,
        Retired = 2,
    }

    private sealed class Poco
    {
        public int Id { get; set; }
    }

    // Hands back fresh arrays and ignores returns, so TypedColumn.Dispose never
    // trips ArrayPool.Shared's foreign-buffer guard.
    private sealed class NoReturnPool<T> : ArrayPool<T>
    {
        public static readonly NoReturnPool<T> Instance = new();
        public override T[] Rent(int minimumLength) => new T[minimumLength];
        public override void Return(T[] array, bool clearArray = false) { }
    }

    private static ITypedColumn[] OneColumn<TCol>(params TCol[] values)
        => new ITypedColumn[] { new TypedColumn<TCol>(values, values.Length, NoReturnPool<TCol>.Instance) };

    // ---- IsScalarTarget: the non-enum recognition legs ----------------------

    [Fact]
    public void DateOnlyScalar_BindsFirstColumn()
    {
        // DateOnly is NOT in IsDirectlyMappable, so it only reaches the scalar
        // path via the explicit DateOnly leg. Miss that leg and a
        // QueryStreamAsync<DateOnly> silently yields DateOnly.MinValue per row.
        var mapper = new ReflectionTypedRowMapper<DateOnly>(new[] { "d" });
        var columns = OneColumn(new DateOnly(2026, 7, 28), new DateOnly(1999, 1, 2));

        Assert.Equal(new DateOnly(2026, 7, 28), mapper.MapRow(columns, 0));
        Assert.Equal(new DateOnly(1999, 1, 2), mapper.MapRow(columns, 1));
    }

    [Fact]
    public void TimeOnlyScalar_BindsFirstColumn()
    {
        var mapper = new ReflectionTypedRowMapper<TimeOnly>(new[] { "t" });
        var columns = OneColumn(new TimeOnly(13, 45, 30));

        Assert.Equal(new TimeOnly(13, 45, 30), mapper.MapRow(columns, 0));
    }

    [Fact]
    public void NullableDateOnlyScalar_UnwrapsBeforeRecognition()
    {
        // The Nullable unwrap happens before the DateOnly check; without it
        // DateOnly? falls through to the POCO path.
        var mapper = new ReflectionTypedRowMapper<DateOnly?>(new[] { "d" });
        var columns = OneColumn<DateOnly?>(new DateOnly(2026, 7, 28));

        Assert.Equal(new DateOnly(2026, 7, 28), mapper.MapRow(columns, 0));
    }

    [Fact]
    public void PocoTarget_StillUsesPropertySetters()
    {
        // Negative control: a POCO must fail every scalar-recognition leg and keep
        // the setter path. If IsScalarTarget ever went true here, Id would be
        // bound from column 0 as if the POCO were the column's value.
        var mapper = new ReflectionTypedRowMapper<Poco>(new[] { "Id" });
        var columns = OneColumn(7, 9);

        Assert.Equal(7, mapper.MapRow(columns, 0).Id);
        Assert.Equal(9, mapper.MapRow(columns, 1).Id);
    }

    // ---- BuildScalarMap: the conversion outcomes ---------------------------

    [Fact]
    public void ScalarMap_NoColumns_Throws()
    {
        var mapper = new ReflectionTypedRowMapper<long>(new[] { "n" });

        var ex = Assert.Throws<InvalidOperationException>(
            () => mapper.MapRow(Array.Empty<ITypedColumn>(), 0));

        Assert.Contains("Int64", ex.Message);
    }

    [Fact]
    public void ScalarMap_ExactTypeMatch_SkipsConversion()
    {
        var mapper = new ReflectionTypedRowMapper<long>(new[] { "n" });

        Assert.Equal(77L, mapper.MapRow(OneColumn(77L), 0));
    }

    [Fact]
    public void ScalarMap_SqlNull_YieldsDefault()
    {
        // Matches the property-setter path's null handling: a NULL maps to
        // default(T), not an exception.
        var mapper = new ReflectionTypedRowMapper<long?>(new[] { "n" });

        // Explicit array — a bare `null` binds to the params array itself, not a
        // one-element array holding null.
        Assert.Null(mapper.MapRow(OneColumn(new long?[] { null }), 0));
    }

    [Fact]
    public void ScalarMap_EnumFromUnderlyingInteger_Converts()
    {
        // The column's element type need not match T: an enum arrives as its
        // numeric representation, so the map must route through Enum.ToObject.
        // A direct TypedColumn<Status> cast would throw here.
        var mapper = new ReflectionTypedRowMapper<Status>(new[] { "s" });
        var columns = OneColumn(2, 1);

        Assert.Equal(Status.Retired, mapper.MapRow(columns, 0));
        Assert.Equal(Status.Active, mapper.MapRow(columns, 1));
    }

    [Fact]
    public void ScalarMap_NullableEnumFromUnderlyingInteger_Converts()
    {
        var mapper = new ReflectionTypedRowMapper<Status?>(new[] { "s" });

        Assert.Equal(Status.Retired, mapper.MapRow(OneColumn(2), 0));
    }

    [Fact]
    public void ScalarMap_WideningNumeric_UsesChangeType()
    {
        // UInt8/Int32 column read as a wider CLR target — the documented reason
        // the map boxes through GetValue instead of casting.
        var mapper = new ReflectionTypedRowMapper<long>(new[] { "n" });

        Assert.Equal(42L, mapper.MapRow(OneColumn(42), 0));
    }

    [Fact]
    public void ScalarMap_ReadsFirstColumnOnly()
    {
        // "Scalar binds ordinal 0" — a second column must be ignored, not
        // merged or preferred.
        var mapper = new ReflectionTypedRowMapper<int>(new[] { "a", "b" });
        var columns = new ITypedColumn[]
        {
            new TypedColumn<int>(new[] { 1 }, 1, NoReturnPool<int>.Instance),
            new TypedColumn<int>(new[] { 2 }, 1, NoReturnPool<int>.Instance),
        };

        Assert.Equal(1, mapper.MapRow(columns, 0));
    }
}

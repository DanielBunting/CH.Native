using System.Collections;
using CH.Native.Commands;
using Xunit;

namespace CH.Native.Tests.Unit.Commands;

public class ClickHouseParameterCollectionTests
{
    private static ClickHouseParameterCollection Make(params string[] names)
    {
        var c = new ClickHouseParameterCollection();
        foreach (var n in names)
            c.Add(n, 1);
        return c;
    }

    [Fact]
    public void Count_And_IsReadOnly()
    {
        var c = Make("a", "b");
        Assert.Equal(2, c.Count);
        Assert.False(c.IsReadOnly);
    }

    [Fact]
    public void Add_NameValue_And_NameValueType()
    {
        var c = new ClickHouseParameterCollection();
        var p1 = c.Add("a", 1);
        var p2 = c.Add("b", 2, "Int32");
        Assert.Equal("a", p1.ParameterName);
        Assert.Equal("Int32", p2.ClickHouseType);
        Assert.Equal(2, c.Count);
    }

    [Fact]
    public void Add_Null_Throws() =>
        Assert.Throws<ArgumentNullException>(() => new ClickHouseParameterCollection().Add((ClickHouseParameter)null!));

    [Fact]
    public void Add_DuplicateName_Throws()
    {
        var c = Make("a");
        Assert.Throws<ArgumentException>(() => c.Add("a", 2));
    }

    [Fact]
    public void Indexer_Int_GetSet_UpdatesLookup()
    {
        var c = Make("a", "b");
        Assert.Equal("a", c[0].ParameterName);
        c[0] = new ClickHouseParameter("c", 9);
        Assert.Equal("c", c[0].ParameterName);
        Assert.False(c.Contains("a"));   // old name removed from lookup
        Assert.True(c.Contains("c"));    // new name added
    }

    [Fact]
    public void Indexer_Name_Get_TrimsAt_And_Throws()
    {
        var c = Make("a");
        Assert.Equal("a", c["@a"].ParameterName);   // '@' trimmed, case-insensitive
        Assert.Equal("a", c["A"].ParameterName);
        Assert.Throws<KeyNotFoundException>(() => c["missing"]);
    }

    [Fact]
    public void Contains_ByName_And_ByItem()
    {
        var c = Make("a");
        var item = c[0];
        Assert.True(c.Contains("@a"));
        Assert.False(c.Contains("x"));
        Assert.True(c.Contains(item));
        Assert.False(c.Contains(new ClickHouseParameter("z", 0)));
    }

    [Fact]
    public void IndexOf_ByName_And_ByItem()
    {
        var c = Make("a", "b");
        Assert.Equal(1, c.IndexOf("@b"));
        Assert.Equal(-1, c.IndexOf("missing"));
        Assert.Equal(0, c.IndexOf(c[0]));
    }

    [Fact]
    public void Insert_RebuildsLookup_And_RejectsDuplicate()
    {
        var c = Make("a", "b");
        c.Insert(0, new ClickHouseParameter("c", 3));
        Assert.Equal("c", c[0].ParameterName);
        Assert.Equal(1, c.IndexOf("a"));   // lookup rebuilt with shifted index
        Assert.Throws<ArgumentException>(() => c.Insert(0, new ClickHouseParameter("a", 0)));
        Assert.Throws<ArgumentNullException>(() => c.Insert(0, null!));
    }

    [Fact]
    public void RemoveAt_And_Remove_ByName_ByItem()
    {
        var c = Make("a", "b", "c");
        c.RemoveAt(0);
        Assert.Equal("b", c[0].ParameterName);
        Assert.Equal(0, c.IndexOf("b"));   // lookup rebuilt

        Assert.True(c.Remove("@c"));
        Assert.False(c.Remove("missing"));

        var item = c[0];
        Assert.True(c.Remove(item));
        Assert.False(c.Remove(new ClickHouseParameter("z", 0)));
    }

    [Fact]
    public void Clear_EmptiesCollectionAndLookup()
    {
        var c = Make("a", "b");
        c.Clear();
        Assert.Equal(0, c.Count);
        Assert.False(c.Contains("a"));
    }

    [Fact]
    public void CopyTo_CopiesParameters()
    {
        var c = Make("a", "b");
        var arr = new ClickHouseParameter[3];
        c.CopyTo(arr, 1);
        Assert.Null(arr[0]);
        Assert.Equal("a", arr[1].ParameterName);
        Assert.Equal("b", arr[2].ParameterName);
    }

    [Fact]
    public void Enumerators_Generic_And_NonGeneric()
    {
        var c = Make("a", "b");
        var generic = 0;
        foreach (var _ in c) generic++;          // IEnumerator<ClickHouseParameter>
        var nonGeneric = 0;
        foreach (var _ in (IEnumerable)c) nonGeneric++;   // IEnumerable.GetEnumerator
        Assert.Equal(2, generic);
        Assert.Equal(2, nonGeneric);
    }
}

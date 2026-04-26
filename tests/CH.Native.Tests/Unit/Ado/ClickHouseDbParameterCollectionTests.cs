using CH.Native.Ado;
using Xunit;

namespace CH.Native.Tests.Unit.Ado;

/// <summary>
/// Audit finding #22: The audit guessed that adding <c>@id</c> and then
/// <c>@ID</c> would silently overwrite the first because the lookup uses
/// <see cref="StringComparer.OrdinalIgnoreCase"/>. These tests document
/// the actual behaviour: duplicates (case-insensitive, with or without
/// the <c>@</c> prefix) throw <see cref="ArgumentException"/> from the
/// <c>ValidateNoDuplicate</c> guard, matching standard ADO.NET semantics.
/// </summary>
public class ClickHouseDbParameterCollectionTests
{
    [Fact]
    public void Add_DifferentNames_BothStored()
    {
        var coll = new ClickHouseDbParameterCollection();
        coll.Add("a", 1);
        coll.Add("b", 2);

        Assert.Equal(2, coll.Count);
        Assert.True(coll.Contains("a"));
        Assert.True(coll.Contains("b"));
    }

    [Fact]
    public void Add_SameNameTwice_Throws()
    {
        var coll = new ClickHouseDbParameterCollection();
        coll.Add("id", 1);
        Assert.Throws<ArgumentException>(() => coll.Add("id", 2));
    }

    [Fact]
    public void Add_DifferentCaseSameName_ThrowsRatherThanOverwrites()
    {
        var coll = new ClickHouseDbParameterCollection();
        coll.Add("id", 1);

        // The audit suspected silent overwrite. Confirm we throw instead.
        Assert.Throws<ArgumentException>(() => coll.Add("ID", 2));

        // First parameter must remain untouched.
        Assert.Equal(1, coll.Count);
        Assert.Equal(1, coll["id"].Value);
    }

    [Fact]
    public void Add_AtPrefixedAliasOfExistingName_ThrowsRatherThanOverwrites()
    {
        var coll = new ClickHouseDbParameterCollection();
        coll.Add("id", 1);

        // Normalisation strips a leading '@', so '@id' and 'id' are the same key.
        Assert.Throws<ArgumentException>(() => coll.Add("@id", 2));
        Assert.Throws<ArgumentException>(() => coll.Add("@ID", 2));

        Assert.Equal(1, coll.Count);
        Assert.Equal(1, coll["@id"].Value);
        Assert.Equal(1, coll["@ID"].Value);
    }

    [Fact]
    public void Lookup_IsCaseInsensitiveAndAtPrefixInsensitive()
    {
        var coll = new ClickHouseDbParameterCollection();
        coll.Add("UserId", 7);

        Assert.True(coll.Contains("userid"));
        Assert.True(coll.Contains("@UserId"));
        Assert.True(coll.Contains("@USERID"));
        Assert.Equal(7, coll["@userid"].Value);
    }

    [Fact]
    public void RemoveAt_ByDifferentCaseName_RemovesParameter()
    {
        var coll = new ClickHouseDbParameterCollection();
        coll.Add("UserId", 7);
        coll.Add("name", "x");

        coll.RemoveAt("USERID");

        Assert.Equal(1, coll.Count);
        Assert.False(coll.Contains("UserId"));
        Assert.True(coll.Contains("name"));
    }

    [Fact]
    public void SetParameter_RenamingToExistingNameInDifferentCase_Throws()
    {
        var coll = new ClickHouseDbParameterCollection();
        coll.Add("a", 1);
        coll.Add("b", 2);

        var renamed = new ClickHouseDbParameter { ParameterName = "A", Value = 99 };

        // Position 1 is "b"; renaming it to "A" should collide with position 0.
        Assert.Throws<ArgumentException>(() => coll[1] = renamed);
    }
}

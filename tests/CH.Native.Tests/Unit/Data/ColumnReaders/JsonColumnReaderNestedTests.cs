using System.Buffers;
using System.Text;
using System.Text.Json;
using CH.Native.Data.ColumnReaders;
using CH.Native.Protocol;
using Xunit;

namespace CH.Native.Tests.Unit.Data.ColumnReaders;

/// <summary>
/// Unit tests for JsonColumnReader handling of nested JSON structures.
/// </summary>
public class JsonColumnReaderNestedTests
{
    private static byte[] CreateJsonProtocolBytes(string json)
    {
        // JSON columns have: version (UInt64) + length-prefixed string
        var jsonBytes = Encoding.UTF8.GetBytes(json);
        var result = new byte[8 + GetVarIntLength(jsonBytes.Length) + jsonBytes.Length];
        var span = result.AsSpan();

        // Version 1 (string serialization)
        BitConverter.TryWriteBytes(span, 1UL);
        var offset = 8;

        // Write varint length
        offset += WriteVarInt(span.Slice(offset), jsonBytes.Length);

        // Write JSON string
        jsonBytes.CopyTo(span.Slice(offset));

        return result;
    }

    private static int GetVarIntLength(int value)
    {
        int length = 0;
        do { length++; value >>= 7; } while (value > 0);
        return length;
    }

    private static int WriteVarInt(Span<byte> span, int value)
    {
        int written = 0;
        do
        {
            byte b = (byte)(value & 0x7F);
            value >>= 7;
            if (value > 0) b |= 0x80;
            span[written++] = b;
        } while (value > 0);
        return written;
    }

    [Fact]
    public void ReadTypedColumn_DeeplyNestedObject_ParsesCorrectly()
    {
        var json = @"{""a"":{""b"":{""c"":{""d"":{""value"":42}}}}}";
        var bytes = CreateJsonProtocolBytes(json);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var columnReader = new JsonColumnReader();
        var result = columnReader.ReadTypedColumn(ref reader, 1);

        Assert.Equal(1, result.Count);
        using var doc = result[0];

        var value = doc.RootElement
            .GetProperty("a")
            .GetProperty("b")
            .GetProperty("c")
            .GetProperty("d")
            .GetProperty("value")
            .GetInt32();

        Assert.Equal(42, value);
    }

    [Theory]
    [InlineData(@"{""a"":1}", 1)]
    [InlineData(@"{""a"":{""b"":2}}", 2)]
    [InlineData(@"{""a"":{""b"":{""c"":3}}}", 3)]
    [InlineData(@"{""a"":{""b"":{""c"":{""d"":4}}}}", 4)]
    public void ReadTypedColumn_VariousNestingDepths_ParsesCorrectly(string json, int expectedValue)
    {
        var bytes = CreateJsonProtocolBytes(json);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var columnReader = new JsonColumnReader();
        var result = columnReader.ReadTypedColumn(ref reader, 1);

        using var doc = result[0];

        // Navigate to the deepest value
        var element = doc.RootElement;
        while (element.ValueKind == JsonValueKind.Object)
        {
            var prop = element.EnumerateObject().First();
            if (prop.Value.ValueKind == JsonValueKind.Number)
            {
                Assert.Equal(expectedValue, prop.Value.GetInt32());
                return;
            }
            element = prop.Value;
        }

        Assert.Fail("Could not find nested value");
    }

    [Fact]
    public void ReadTypedColumn_NestedArrays_ParsesCorrectly()
    {
        var json = @"{""matrix"":[[1,2],[3,4]],""items"":[{""name"":""A""},{""name"":""B""}]}";
        var bytes = CreateJsonProtocolBytes(json);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var columnReader = new JsonColumnReader();
        var result = columnReader.ReadTypedColumn(ref reader, 1);

        using var doc = result[0];

        // Check nested array
        var matrix = doc.RootElement.GetProperty("matrix");
        Assert.Equal(2, matrix.GetArrayLength());
        Assert.Equal(1, matrix[0][0].GetInt32());
        Assert.Equal(4, matrix[1][1].GetInt32());

        // Check array of objects
        var items = doc.RootElement.GetProperty("items");
        Assert.Equal("A", items[0].GetProperty("name").GetString());
        Assert.Equal("B", items[1].GetProperty("name").GetString());
    }

    [Fact]
    public void ReadTypedColumn_MixedNestedStructure_ParsesCorrectly()
    {
        var json = @"{
            ""user"":{""name"":""Alice"",""profile"":{""address"":{""city"":""NYC""}}},
            ""orders"":[{""id"":1,""items"":[{""product"":""Widget"",""price"":29.99}]}],
            ""tags"":[""vip"",""premium""]
        }";
        var bytes = CreateJsonProtocolBytes(json);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var columnReader = new JsonColumnReader();
        var result = columnReader.ReadTypedColumn(ref reader, 1);

        using var doc = result[0];

        // Nested object traversal
        Assert.Equal("NYC", doc.RootElement
            .GetProperty("user")
            .GetProperty("profile")
            .GetProperty("address")
            .GetProperty("city")
            .GetString());

        // Array with nested object
        Assert.Equal("Widget", doc.RootElement
            .GetProperty("orders")[0]
            .GetProperty("items")[0]
            .GetProperty("product")
            .GetString());

        // Simple array
        Assert.Equal("vip", doc.RootElement.GetProperty("tags")[0].GetString());
    }

    [Fact]
    public void ReadTypedColumn_EmptyNestedObjects_ParsesCorrectly()
    {
        var json = @"{""user"":{""profile"":{}},""items"":[]}";
        var bytes = CreateJsonProtocolBytes(json);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var columnReader = new JsonColumnReader();
        var result = columnReader.ReadTypedColumn(ref reader, 1);

        using var doc = result[0];

        var profile = doc.RootElement.GetProperty("user").GetProperty("profile");
        Assert.Equal(JsonValueKind.Object, profile.ValueKind);
        Assert.Equal(0, profile.EnumerateObject().Count());

        var items = doc.RootElement.GetProperty("items");
        Assert.Equal(JsonValueKind.Array, items.ValueKind);
        Assert.Equal(0, items.GetArrayLength());
    }

    [Fact]
    public void ReadTypedColumn_NullNestedValues_ParsesCorrectly()
    {
        var json = @"{""user"":{""profile"":null,""name"":""Alice""},""data"":null}";
        var bytes = CreateJsonProtocolBytes(json);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var columnReader = new JsonColumnReader();
        var result = columnReader.ReadTypedColumn(ref reader, 1);

        using var doc = result[0];

        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("user").GetProperty("profile").ValueKind);
        Assert.Equal("Alice", doc.RootElement.GetProperty("user").GetProperty("name").GetString());
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("data").ValueKind);
    }

    [Fact]
    public void ReadTypedColumn_LargeNestedDocument_ParsesCorrectly()
    {
        // Create a reasonably complex nested document
        var json = @"{
            ""metadata"":{""version"":""1.0"",""generated"":""2024-01-15""},
            ""data"":{
                ""users"":[
                    {""id"":1,""name"":""Alice"",""roles"":[""admin"",""user""]},
                    {""id"":2,""name"":""Bob"",""roles"":[""user""]}
                ],
                ""config"":{
                    ""features"":{""darkMode"":true,""notifications"":{""email"":true,""sms"":false}},
                    ""limits"":{""maxUsers"":100,""maxStorage"":1024}
                }
            }
        }";
        var bytes = CreateJsonProtocolBytes(json);
        var reader = new ProtocolReader(new ReadOnlySequence<byte>(bytes));

        var columnReader = new JsonColumnReader();
        var result = columnReader.ReadTypedColumn(ref reader, 1);

        using var doc = result[0];

        // Deep traversal
        Assert.True(doc.RootElement
            .GetProperty("data")
            .GetProperty("config")
            .GetProperty("features")
            .GetProperty("notifications")
            .GetProperty("email")
            .GetBoolean());

        // Array element access
        Assert.Equal("admin", doc.RootElement
            .GetProperty("data")
            .GetProperty("users")[0]
            .GetProperty("roles")[0]
            .GetString());
    }
}

using System.Collections;
using System.Globalization;
using System.Net;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using ClickHouse.Driver.Numerics;
using NativeClickHouseDecimal = CH.Native.Numerics.ClickHouseDecimal;
using Xunit;

namespace CH.Native.SmokeTests.Helpers;

// Compares what came back from a native SELECT against a pre-write "source of truth":
// either the original POCO values (for write→read roundtrip tests) or another
// reader's output (for read-only cross-client tests). Parameters are named
// postRead / preWritten because the first argument is what the DB just handed
// back to us and the second is the ground truth we expected.
public static class ResultComparer
{
    public static void AssertResultsEqual(
        List<object?[]> postReadResults,
        List<object?[]> preWrittenResults,
        string? context = null)
    {
        var prefix = context != null ? $"[{context}] " : "";

        Assert.Equal(preWrittenResults.Count, postReadResults.Count);

        for (int row = 0; row < postReadResults.Count; row++)
        {
            Assert.Equal(preWrittenResults[row].Length, postReadResults[row].Length);

            for (int col = 0; col < postReadResults[row].Length; col++)
            {
                var postRead = postReadResults[row][col];
                var preWritten = preWrittenResults[row][col];

                AssertValuesEqual(postRead, preWritten, $"{prefix}Row {row}, Col {col}");
            }
        }
    }

    public static void AssertValuesEqual(object? postRead, object? preWritten, string location)
    {
        // Both null
        if (postRead is null && preWritten is null)
            return;

        // One null, other not
        if (postRead is null || preWritten is null)
        {
            Assert.Fail($"{location}: PostRead={postRead ?? "null"}, PreWritten={preWritten ?? "null"}");
            return;
        }

        // Same type, direct comparison
        if (postRead.GetType() == preWritten.GetType())
        {
            if (postRead is double prd && preWritten is double pwd)
            {
                AssertDoublesEqual(prd, pwd, location);
                return;
            }

            if (postRead is float prf && preWritten is float pwf)
            {
                AssertFloatsEqual(prf, pwf, location);
                return;
            }

            if (postRead is byte[] prb && preWritten is byte[] pwb)
            {
                Assert.Equal(pwb, prb);
                return;
            }

            if (postRead is IList postList && preWritten is IList preList)
            {
                AssertListsEqual(postList, preList, location);
                return;
            }

            Assert.Equal(preWritten, postRead);
            return;
        }

        // Cross-type normalization

        // BigInteger conversions (Int128/UInt128/Int256/UInt256)
        if (TryNormalizeToBigInteger(postRead, out var postBi) &&
            TryNormalizeToBigInteger(preWritten, out var preBi))
        {
            Assert.Equal(preBi, postBi);
            return;
        }

        // IPAddress normalization
        if (TryNormalizeToIPAddress(postRead, out var postIp) &&
            TryNormalizeToIPAddress(preWritten, out var preIp))
        {
            Assert.Equal(preIp, postIp);
            return;
        }

        // DateOnly vs DateTime
        if (postRead is DateOnly postDateOnly && preWritten is DateTime preDt)
        {
            Assert.Equal(DateOnly.FromDateTime(preDt), postDateOnly);
            return;
        }
        if (postRead is DateTime postDt2 && preWritten is DateOnly preDateOnly2)
        {
            Assert.Equal(preDateOnly2, DateOnly.FromDateTime(postDt2));
            return;
        }

        // DateTimeOffset vs DateTime (timezone-aware columns)
        if (postRead is DateTimeOffset postDto && preWritten is DateTime preDtUtc)
        {
            Assert.Equal(preDtUtc, postDto.UtcDateTime);
            return;
        }
        if (postRead is DateTime postDtUtc && preWritten is DateTimeOffset preDto)
        {
            Assert.Equal(preDto.UtcDateTime, postDtUtc);
            return;
        }

        // Guid vs string
        if (postRead is Guid postGuid && preWritten is string preGuidStr)
        {
            Assert.Equal(Guid.Parse(preGuidStr), postGuid);
            return;
        }
        if (postRead is string postGuidStr && preWritten is Guid preGuid)
        {
            Assert.Equal(preGuid, Guid.Parse(postGuidStr));
            return;
        }

        // byte[] (FixedString) vs string
        if (postRead is byte[] postBytes && preWritten is string preStr)
        {
            var postStr = Encoding.UTF8.GetString(postBytes).TrimEnd('\0');
            Assert.Equal(preStr.TrimEnd('\0'), postStr);
            return;
        }
        if (postRead is string postStr2 && preWritten is byte[] preBytes)
        {
            var preStr2 = Encoding.UTF8.GetString(preBytes).TrimEnd('\0');
            Assert.Equal(preStr2, postStr2);
            return;
        }

        // Float special values as string
        if (postRead is double postDouble && preWritten is string preFloatStr)
        {
            AssertDoubleMatchesString(postDouble, preFloatStr, location);
            return;
        }
        if (postRead is string postFloatStr && preWritten is double preDouble)
        {
            AssertDoubleMatchesString(preDouble, postFloatStr, location);
            return;
        }

        // DateTime with different precision
        if (postRead is DateTime postDt && preWritten is DateTime preDt2b)
        {
            Assert.Equal(preDt2b, postDt);
            return;
        }

        // Decimal with potentially different scale
        if (postRead is decimal postDec && preWritten is decimal preDec)
        {
            Assert.Equal(preDec, postDec);
            return;
        }

        // ClickHouseDecimal cross-type: CH.Native.Numerics.ClickHouseDecimal vs ClickHouse.Driver.Numerics.ClickHouseDecimal
        // Both are BigInteger-backed with identical ToString formatting — compare by string representation.
        if (postRead is NativeClickHouseDecimal postChd && preWritten is ClickHouseDecimal preChd)
        {
            Assert.Equal(
                preChd.ToString(CultureInfo.InvariantCulture),
                postChd.ToString(null, CultureInfo.InvariantCulture));
            return;
        }

        // CH.Native.Numerics.ClickHouseDecimal vs System.Decimal — reader returns
        // ClickHouseDecimal for wide Decimal types (128/256) even when the value fits
        // in a CLR decimal. Compare numerically; ClickHouseDecimal preserves scale so
        // ToString differs from decimal's trailing-zero-stripped format.
        if (postRead is NativeClickHouseDecimal postChd2 && preWritten is decimal prePlain)
        {
            Assert.True(postChd2.CompareTo(prePlain) == 0,
                $"{location}: ClickHouseDecimal {postChd2} != decimal {prePlain}");
            return;
        }
        if (postRead is decimal postPlain && preWritten is NativeClickHouseDecimal preChd2)
        {
            Assert.True(preChd2.CompareTo(postPlain) == 0,
                $"{location}: decimal {postPlain} != ClickHouseDecimal {preChd2}");
            return;
        }

        // ITuple (read-back) vs IList (pre-written as object[]) — surface when a POCO
        // represents a Tuple column as object[].
        if (postRead is ITuple postT && preWritten is IList preL && preWritten is not ITuple)
        {
            Assert.Equal(preL.Count, postT.Length);
            for (int i = 0; i < postT.Length; i++)
                AssertValuesEqual(postT[i], preL[i], $"{location}.Item{i + 1}");
            return;
        }
        if (postRead is IList postL && preWritten is ITuple preT && postRead is not ITuple)
        {
            Assert.Equal(preT.Length, postL.Count);
            for (int i = 0; i < preT.Length; i++)
                AssertValuesEqual(postL[i], preT[i], $"{location}.Item{i + 1}");
            return;
        }

        // IList cross-type
        if (postRead is IList postIList && preWritten is IList preIList)
        {
            AssertListsEqual(postIList, preIList, location);
            return;
        }

        // ITuple cross-type (different concrete tuple kinds from different sources)
        if (postRead is ITuple postTuple && preWritten is ITuple preTuple)
        {
            Assert.Equal(preTuple.Length, postTuple.Length);
            for (int i = 0; i < postTuple.Length; i++)
                AssertValuesEqual(postTuple[i], preTuple[i], $"{location}.Item{i + 1}");
            return;
        }

        // sbyte/short/int/long numeric conversions
        if (IsNumeric(postRead) && IsNumeric(preWritten))
        {
            var postDecimal = Convert.ToDecimal(postRead);
            var preDecimal = Convert.ToDecimal(preWritten);
            Assert.Equal(preDecimal, postDecimal);
            return;
        }

        // IDictionary cross-type — element-by-element so inner value normalization
        // (e.g. byte[] ↔ string for FixedString values) applies.
        if (postRead is IDictionary postDict && preWritten is IDictionary preDict)
        {
            Assert.Equal(preDict.Count, postDict.Count);
            foreach (DictionaryEntry preEntry in preDict)
            {
                Assert.True(postDict.Contains(preEntry.Key),
                    $"{location}: post-read dict missing key {preEntry.Key}");
                AssertValuesEqual(postDict[preEntry.Key], preEntry.Value, $"{location}[{preEntry.Key}]");
            }
            return;
        }

        // String representation fallback
        if (postRead is string || preWritten is string)
        {
            Assert.Equal(preWritten?.ToString(), postRead?.ToString());
            return;
        }

        Assert.Equal(preWritten, postRead);
    }

    private static void AssertDoublesEqual(double postRead, double preWritten, string location)
    {
        if (double.IsNaN(postRead) && double.IsNaN(preWritten)) return;
        if (double.IsPositiveInfinity(postRead) && double.IsPositiveInfinity(preWritten)) return;
        if (double.IsNegativeInfinity(postRead) && double.IsNegativeInfinity(preWritten)) return;

        // Check for -0.0
        if (IsNegativeZero(postRead) && IsNegativeZero(preWritten)) return;
        if (IsNegativeZero(postRead) != IsNegativeZero(preWritten))
        {
            Assert.Fail($"{location}: -0.0 mismatch. PostRead={postRead}, PreWritten={preWritten}");
            return;
        }

        Assert.Equal(preWritten, postRead);
    }

    private static void AssertFloatsEqual(float postRead, float preWritten, string location)
    {
        if (float.IsNaN(postRead) && float.IsNaN(preWritten)) return;
        if (float.IsPositiveInfinity(postRead) && float.IsPositiveInfinity(preWritten)) return;
        if (float.IsNegativeInfinity(postRead) && float.IsNegativeInfinity(preWritten)) return;
        Assert.Equal(preWritten, postRead);
    }

    private static bool IsNegativeZero(double d) =>
        d == 0.0 && double.IsNegative(d);

    private static void AssertDoubleMatchesString(double d, string s, string location)
    {
        if (double.IsNaN(d) && (s == "nan" || s == "NaN")) return;
        if (double.IsPositiveInfinity(d) && (s == "inf" || s == "Infinity" || s == "+inf")) return;
        if (double.IsNegativeInfinity(d) && (s == "-inf" || s == "-Infinity")) return;

        if (double.TryParse(s, out var parsed))
        {
            AssertDoublesEqual(d, parsed, location);
            return;
        }

        Assert.Fail($"{location}: Cannot compare double {d} with string '{s}'");
    }

    private static void AssertListsEqual(IList postList, IList preList, string location)
    {
        Assert.Equal(preList.Count, postList.Count);

        for (int i = 0; i < postList.Count; i++)
        {
            AssertValuesEqual(postList[i], preList[i], $"{location}[{i}]");
        }
    }

    private static bool TryNormalizeToBigInteger(object val, out BigInteger result)
    {
        result = default;

        if (val is BigInteger bi) { result = bi; return true; }
        if (val is Int128 i128) { result = (BigInteger)i128; return true; }
        if (val is UInt128 u128) { result = (BigInteger)u128; return true; }
        if (val is long l) { result = new BigInteger(l); return true; }
        if (val is ulong ul) { result = new BigInteger(ul); return true; }
        if (val is int i) { result = new BigInteger(i); return true; }
        if (val is uint ui) { result = new BigInteger(ui); return true; }

        if (val is string s && BigInteger.TryParse(s, out var parsed))
        {
            result = parsed;
            return true;
        }

        return false;
    }

    private static bool TryNormalizeToIPAddress(object val, out IPAddress? result)
    {
        result = null;

        if (val is IPAddress ip) { result = ip; return true; }
        if (val is string s && IPAddress.TryParse(s, out var parsed))
        {
            result = parsed;
            return true;
        }

        return false;
    }

    private static bool IsNumeric(object val) =>
        val is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;
}

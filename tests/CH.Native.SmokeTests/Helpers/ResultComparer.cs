using System.Collections;
using System.Net;
using System.Numerics;
using System.Text;
using Xunit;

namespace CH.Native.SmokeTests.Helpers;

public static class ResultComparer
{
    public static void AssertResultsEqual(
        List<object?[]> nativeResults,
        List<object?[]> driverResults,
        string? context = null)
    {
        var prefix = context != null ? $"[{context}] " : "";

        Assert.Equal(driverResults.Count, nativeResults.Count);

        for (int row = 0; row < nativeResults.Count; row++)
        {
            Assert.Equal(driverResults[row].Length, nativeResults[row].Length);

            for (int col = 0; col < nativeResults[row].Length; col++)
            {
                var nativeVal = nativeResults[row][col];
                var driverVal = driverResults[row][col];

                AssertValuesEqual(nativeVal, driverVal, $"{prefix}Row {row}, Col {col}");
            }
        }
    }

    public static void AssertValuesEqual(object? nativeVal, object? driverVal, string location)
    {
        // Both null
        if (nativeVal is null && driverVal is null)
            return;

        // One null, other not
        if (nativeVal is null || driverVal is null)
        {
            Assert.Fail($"{location}: Native={nativeVal ?? "null"}, Driver={driverVal ?? "null"}");
            return;
        }

        // Same type, direct comparison
        if (nativeVal.GetType() == driverVal.GetType())
        {
            if (nativeVal is double nd && driverVal is double dd)
            {
                AssertDoublesEqual(nd, dd, location);
                return;
            }

            if (nativeVal is float nf && driverVal is float df)
            {
                AssertFloatsEqual(nf, df, location);
                return;
            }

            if (nativeVal is byte[] nb && driverVal is byte[] db)
            {
                Assert.Equal(db, nb);
                return;
            }

            if (nativeVal is IList nativeList && driverVal is IList driverList)
            {
                AssertListsEqual(nativeList, driverList, location);
                return;
            }

            Assert.Equal(driverVal, nativeVal);
            return;
        }

        // Cross-type normalization

        // BigInteger conversions (Int128/UInt128/Int256/UInt256)
        if (TryNormalizeToBigInteger(nativeVal, out var nativeBi) &&
            TryNormalizeToBigInteger(driverVal, out var driverBi))
        {
            Assert.Equal(driverBi, nativeBi);
            return;
        }

        // IPAddress normalization
        if (TryNormalizeToIPAddress(nativeVal, out var nativeIp) &&
            TryNormalizeToIPAddress(driverVal, out var driverIp))
        {
            Assert.Equal(driverIp, nativeIp);
            return;
        }

        // DateOnly vs DateTime
        if (nativeVal is DateOnly nativeDateOnly && driverVal is DateTime driverDt)
        {
            Assert.Equal(DateOnly.FromDateTime(driverDt), nativeDateOnly);
            return;
        }
        if (nativeVal is DateTime nativeDt2 && driverVal is DateOnly driverDateOnly2)
        {
            Assert.Equal(driverDateOnly2, DateOnly.FromDateTime(nativeDt2));
            return;
        }

        // Guid vs string
        if (nativeVal is Guid nativeGuid && driverVal is string driverGuidStr)
        {
            Assert.Equal(Guid.Parse(driverGuidStr), nativeGuid);
            return;
        }
        if (nativeVal is string nativeGuidStr && driverVal is Guid driverGuid)
        {
            Assert.Equal(driverGuid, Guid.Parse(nativeGuidStr));
            return;
        }

        // byte[] (FixedString) vs string
        if (nativeVal is byte[] nativeBytes && driverVal is string driverStr)
        {
            var nativeStr = Encoding.UTF8.GetString(nativeBytes).TrimEnd('\0');
            Assert.Equal(driverStr.TrimEnd('\0'), nativeStr);
            return;
        }
        if (nativeVal is string nativeStr2 && driverVal is byte[] driverBytes)
        {
            var driverStr2 = Encoding.UTF8.GetString(driverBytes).TrimEnd('\0');
            Assert.Equal(driverStr2, nativeStr2);
            return;
        }

        // Float special values as string
        if (nativeVal is double nativeDouble && driverVal is string driverFloatStr)
        {
            AssertDoubleMatchesString(nativeDouble, driverFloatStr, location);
            return;
        }
        if (nativeVal is string nativeFloatStr && driverVal is double driverDouble)
        {
            AssertDoubleMatchesString(driverDouble, nativeFloatStr, location);
            return;
        }

        // DateTime with different precision
        if (nativeVal is DateTime nativeDt && driverVal is DateTime driverDt2b)
        {
            Assert.Equal(driverDt2b, nativeDt);
            return;
        }

        // Decimal with potentially different scale
        if (nativeVal is decimal nativeDec && driverVal is decimal driverDec)
        {
            Assert.Equal(driverDec, nativeDec);
            return;
        }

        // IList cross-type
        if (nativeVal is IList nList && driverVal is IList dList)
        {
            AssertListsEqual(nList, dList, location);
            return;
        }

        // sbyte/short/int/long numeric conversions
        if (IsNumeric(nativeVal) && IsNumeric(driverVal))
        {
            var nativeDecimal = Convert.ToDecimal(nativeVal);
            var driverDecimal = Convert.ToDecimal(driverVal);
            Assert.Equal(driverDecimal, nativeDecimal);
            return;
        }

        // String representation fallback
        if (nativeVal is string || driverVal is string)
        {
            Assert.Equal(driverVal?.ToString(), nativeVal?.ToString());
            return;
        }

        Assert.Equal(driverVal, nativeVal);
    }

    private static void AssertDoublesEqual(double native, double driver, string location)
    {
        if (double.IsNaN(native) && double.IsNaN(driver)) return;
        if (double.IsPositiveInfinity(native) && double.IsPositiveInfinity(driver)) return;
        if (double.IsNegativeInfinity(native) && double.IsNegativeInfinity(driver)) return;

        // Check for -0.0
        if (IsNegativeZero(native) && IsNegativeZero(driver)) return;
        if (IsNegativeZero(native) != IsNegativeZero(driver))
        {
            Assert.Fail($"{location}: -0.0 mismatch. Native={native}, Driver={driver}");
            return;
        }

        Assert.Equal(driver, native);
    }

    private static void AssertFloatsEqual(float native, float driver, string location)
    {
        if (float.IsNaN(native) && float.IsNaN(driver)) return;
        if (float.IsPositiveInfinity(native) && float.IsPositiveInfinity(driver)) return;
        if (float.IsNegativeInfinity(native) && float.IsNegativeInfinity(driver)) return;
        Assert.Equal(driver, native);
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

    private static void AssertListsEqual(IList nativeList, IList driverList, string location)
    {
        Assert.Equal(driverList.Count, nativeList.Count);

        for (int i = 0; i < nativeList.Count; i++)
        {
            AssertValuesEqual(nativeList[i], driverList[i], $"{location}[{i}]");
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

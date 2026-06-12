using System.Text;
using CH.Native.SmokeTests.Fixtures;

namespace CH.Native.SmokeTests.Helpers;

/// <summary>
/// Runs queries through the clickhouse-client binary inside the server container, giving the
/// smoke tests a third, independent client to roundtrip against. SELECTs come back as
/// TabSeparated text parsed into TSV-unescaped cells; a null cell is SQL NULL (\N).
/// </summary>
public static class CliQueryHelper
{
    public static async Task ExecuteNonQueryAsync(
        SmokeTestFixture fixture, string sql, IEnumerable<string>? extraArgs = null)
    {
        var result = await fixture.ExecClickHouseClientAsync(sql, extraArgs: extraArgs);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"clickhouse-client exit {result.ExitCode}\nstderr: {result.Stderr}\nsql: {sql}");
        }
    }

    public static async Task<List<string?[]>> QueryCanonicalAsync(
        SmokeTestFixture fixture, string sql, IEnumerable<string>? extraArgs = null)
    {
        var result = await fixture.ExecClickHouseClientAsync(sql, "TabSeparated", extraArgs);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"clickhouse-client exit {result.ExitCode}\nstderr: {result.Stderr}\nsql: {sql}");
        }

        return ParseTsv(result.Stdout);
    }

    internal static List<string?[]> ParseTsv(string stdout)
    {
        var rows = new List<string?[]>();
        // TabSeparated terminates every row with '\n' and escapes literal newlines inside
        // values as \n, so splitting on '\n' is safe. Only the empty remainder after the
        // final terminator is dropped — an empty line elsewhere is a real row holding a
        // single empty-string cell.
        var lines = stdout.Split('\n');
        int count = lines.Length;
        if (count > 0 && lines[count - 1].Length == 0)
        {
            count--;
        }

        for (int i = 0; i < count; i++)
        {
            rows.Add(lines[i].Split('\t').Select(UnescapeTsvField).ToArray());
        }

        return rows;
    }

    internal static string? UnescapeTsvField(string field)
    {
        if (field == "\\N")
        {
            return null;
        }

        if (!field.Contains('\\'))
        {
            return field;
        }

        var sb = new StringBuilder(field.Length);
        for (int i = 0; i < field.Length; i++)
        {
            if (field[i] != '\\' || i == field.Length - 1)
            {
                sb.Append(field[i]);
                continue;
            }

            i++;
            sb.Append(field[i] switch
            {
                't' => '\t',
                'n' => '\n',
                'r' => '\r',
                '\\' => '\\',
                '\'' => '\'',
                '0' => '\0',
                'b' => '\b',
                'f' => '\f',
                var c => c,
            });
        }

        return sb.ToString();
    }
}

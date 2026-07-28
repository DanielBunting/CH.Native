namespace CH.Native.Data.Types;

/// <summary>
/// Parser for ClickHouse type names into structured ClickHouseType objects.
/// </summary>
/// <remarks>
/// Handles:
/// - Simple types: Int32, String
/// - Parameterized: DateTime64(3), FixedString(32), Decimal128(4)
/// - Nested: Nullable(Int32), Array(String)
/// - Complex: Map(String, Array(Int32)), Tuple(Int32, String)
/// - Enums: Enum8('a' = 1, 'b' = 2)
/// - Timezones: DateTime64(3, 'UTC')
/// </remarks>
public static class ClickHouseTypeParser
{
    /// <summary>
    /// Parses a ClickHouse type name string into a structured type.
    /// </summary>
    /// <param name="typeName">The type name to parse.</param>
    /// <returns>The parsed type structure.</returns>
    /// <exception cref="FormatException">Thrown if the type name is malformed.</exception>
    public static ClickHouseType Parse(string typeName)
    {
        if (string.IsNullOrWhiteSpace(typeName))
            throw new ArgumentException("Type name cannot be empty.", nameof(typeName));

        var parser = new Parser(typeName);
        var result = parser.ParseType();

        if (!parser.IsAtEnd)
            throw new FormatException($"Unexpected characters after type name: '{typeName}'");

        return result;
    }

    /// <summary>
    /// Tries to parse a ClickHouse type name, returning null on failure.
    /// </summary>
    public static ClickHouseType? TryParse(string typeName)
    {
        try
        {
            return Parse(typeName);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Whether values of the given ClickHouse type may be NULL, accounting for the transparent
    /// wrappers <c>LowCardinality(...)</c> and <c>SimpleAggregateFunction(fn, T)</c> which are
    /// serialized as their inner type. A plain <c>StartsWith("Nullable(")</c> check misses
    /// <c>LowCardinality(Nullable(X))</c> and <c>SimpleAggregateFunction(fn, Nullable(X))</c>.
    /// Returns <see langword="false"/> for a type name that cannot be parsed.
    /// </summary>
    internal static bool IsEffectivelyNullable(string typeName)
    {
        var parsed = TryParse(typeName);
        return parsed is not null && IsNullable(parsed);

        static bool IsNullable(ClickHouseType t) => t.BaseName switch
        {
            "Nullable" => true,
            // Transparent wrappers: LowCardinality has a single inner type arg; the parser stores
            // SimpleAggregateFunction's inner type as its sole type arg (function name is elsewhere).
            "LowCardinality" or "SimpleAggregateFunction" when t.TypeArguments.Count >= 1
                => IsNullable(t.TypeArguments[^1]),
            _ => false,
        };
    }

    private ref struct Parser
    {
        // Bounds recursion depth. A column type name comes from the server and is
        // only length-capped, so a pathologically nested type (e.g. Array(Array(...)))
        // would recurse ParseType→ParseArguments→ParseType unbounded and throw an
        // uncatchable StackOverflowException. Real ClickHouse types nest only a few
        // levels; 100 is generous headroom while keeping the stack bounded. The same
        // cap protects the reader/skipper factories, which recurse on the parsed tree.
        private const int MaxDepth = 100;

        private readonly string _input;
        private int _pos;
        private int _depth;

        public Parser(string input)
        {
            _input = input;
            _pos = 0;
            _depth = 0;
        }

        public bool IsAtEnd => _pos >= _input.Length;

        public ClickHouseType ParseType()
        {
            if (++_depth > MaxDepth)
                throw new FormatException(
                    $"Type nesting depth exceeds the maximum of {MaxDepth} in '{_input}'.");
            try
            {
                return ParseTypeCore();
            }
            finally
            {
                _depth--;
            }
        }

        private ClickHouseType ParseTypeCore()
        {
            var startPos = _pos;
            var baseName = ParseIdentifier();

            if (IsAtEnd || Peek() != '(')
            {
                // Simple type with no parameters
                return new ClickHouseType(baseName, originalTypeName: _input[startPos.._pos]);
            }

            // Has parameters - consume '('
            Advance();
            SkipWhitespace();

            var typeArguments = new List<ClickHouseType>();
            var parameters = new List<string>();
            var fieldNames = new List<string>();
            string? aggregateFunctionName = null;
            List<string>? aggregateFunctionParameters = null;

            var isAggregateType = baseName is "AggregateFunction" or "SimpleAggregateFunction";

            if (isAggregateType)
            {
                // Empty parens is malformed: must have at least a function name.
                if (IsAtEnd || Peek() == ')')
                    throw new FormatException(
                        $"{baseName} requires at least a function name in '{_input}'");

                // First argument is the aggregate-function descriptor (identifier + optional
                // literal params). Subsequent arguments are type arguments.
                ParseAggregateFunctionDescriptor(out aggregateFunctionName, out aggregateFunctionParameters);
                SkipWhitespace();

                while (!IsAtEnd && Peek() == ',')
                {
                    Advance();
                    SkipWhitespace();
                    if (IsAtEnd || Peek() == ')')
                        break;
                    typeArguments.Add(ParseType());
                    SkipWhitespace();
                }
            }
            else if (!IsAtEnd && Peek() != ')')
            {
                ParseArguments(typeArguments, parameters, fieldNames, baseName);
            }

            // Consume ')'
            if (IsAtEnd || Peek() != ')')
                throw new FormatException($"Expected ')' in type '{_input}'");
            Advance();

            return new ClickHouseType(
                baseName,
                typeArguments.Count > 0 ? typeArguments : null,
                parameters.Count > 0 ? parameters : null,
                originalTypeName: _input[startPos.._pos],
                fieldNames: fieldNames.Count > 0 ? fieldNames : null,
                aggregateFunctionName: aggregateFunctionName,
                aggregateFunctionParameters: aggregateFunctionParameters);
        }

        private void ParseAggregateFunctionDescriptor(out string name, out List<string>? parameters)
        {
            name = ParseIdentifier();
            parameters = null;

            SkipWhitespace();
            if (IsAtEnd || Peek() != '(')
                return;

            Advance();
            SkipWhitespace();
            parameters = new List<string>();

            while (!IsAtEnd && Peek() != ')')
            {
                parameters.Add(ParseParameter());
                SkipWhitespace();
                if (!IsAtEnd && Peek() == ',')
                {
                    Advance();
                    SkipWhitespace();
                }
            }

            if (IsAtEnd || Peek() != ')')
                throw new FormatException(
                    $"Expected ')' closing aggregate-function descriptor in '{_input}'");
            Advance();
        }

        private void ParseArguments(List<ClickHouseType> typeArguments, List<string> parameters, List<string> fieldNames, string baseName)
        {
            // Determine if this type expects type arguments or literal parameters
            var expectsTypeArgs = IsTypeArgumentType(baseName);
            var supportsNamedFields = baseName is "Tuple" or "Nested";

            while (true)
            {
                SkipWhitespace();

                if (expectsTypeArgs && !IsLiteralStart())
                {
                    // Check for named field syntax: "fieldName TypeName"
                    // We need to look ahead to see if this is "name Type" or just "Type"
                    if (supportsNamedFields && IsNamedFieldStart())
                    {
                        var fieldName = ParseIdentifier();
                        SkipWhitespace();
                        typeArguments.Add(ParseType());
                        fieldNames.Add(fieldName);
                    }
                    else
                    {
                        // Parse as nested type (positional)
                        typeArguments.Add(ParseType());
                    }
                }
                else
                {
                    // Parse as literal parameter (number, string, or enum definition)
                    parameters.Add(ParseParameter());
                }

                SkipWhitespace();

                if (IsAtEnd || Peek() == ')')
                    break;

                if (Peek() == ',')
                {
                    Advance();
                    SkipWhitespace();
                }
                else
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Checks if the current position starts a named field (fieldName followed by type).
        /// Uses lookahead to distinguish "id UInt64" from just "UInt64".
        /// </summary>
        private bool IsNamedFieldStart()
        {
            // Save position for lookahead
            var savedPos = _pos;

            try
            {
                // Try to parse an identifier
                if (IsAtEnd || (!char.IsLetter(Peek()) && Peek() != '_'))
                    return false;

                // Skip the first identifier
                while (!IsAtEnd && (char.IsLetterOrDigit(Peek()) || Peek() == '_'))
                    Advance();

                // Must have whitespace after the first identifier
                if (IsAtEnd || !char.IsWhiteSpace(Peek()))
                    return false;

                SkipWhitespace();

                // Must be followed by another identifier (the type name), not a comma, paren, or end
                if (IsAtEnd)
                    return false;

                var c = Peek();

                // If next char starts an identifier, this is a named field
                // Otherwise it's just a type name
                return char.IsLetter(c) || c == '_';
            }
            finally
            {
                // Restore position
                _pos = savedPos;
            }
        }

        private bool IsTypeArgumentType(string baseName)
        {
            // These types have nested type arguments, not just literal parameters
            return baseName is "Nullable" or "Array" or "Map" or "Tuple" or "LowCardinality" or "Nested" or "Variant";
        }

        private bool IsLiteralStart()
        {
            if (IsAtEnd) return false;
            var c = Peek();
            // Literals start with digit, quote, or minus sign
            return char.IsDigit(c) || c == '\'' || c == '-';
        }

        private string ParseIdentifier()
        {
            var start = _pos;

            // First character must be letter or underscore
            if (IsAtEnd || (!char.IsLetter(Peek()) && Peek() != '_'))
                throw new FormatException($"Expected identifier at position {_pos} in '{_input}'");

            while (!IsAtEnd && (char.IsLetterOrDigit(Peek()) || Peek() == '_'))
            {
                Advance();
            }

            return _input[start.._pos];
        }

        private string ParseParameter()
        {
            SkipWhitespace();

            if (IsAtEnd)
                throw new FormatException($"Expected parameter at position {_pos} in '{_input}'");

            var c = Peek();

            if (c == '\'')
            {
                // String literal (possibly with = for enum)
                return ParseEnumOrStringParam();
            }
            else if (char.IsDigit(c) || c == '-')
            {
                // Numeric literal
                return ParseNumber();
            }
            else
            {
                // Could be an identifier, possibly with key=value syntax (e.g., max_dynamic_paths=100)
                var paramStart = _pos;
                ParseIdentifier();

                if (!IsAtEnd && Peek() == '=')
                {
                    Advance();
                    SkipWhitespace();
                    // Parse the value after '='
                    if (!IsAtEnd && (char.IsDigit(Peek()) || Peek() == '-'))
                        ParseNumber();
                    else if (!IsAtEnd && Peek() == '\'')
                        ParseStringLiteral();
                    else if (!IsAtEnd)
                        ParseIdentifier();
                }

                return _input[paramStart.._pos];
            }
        }

        private string ParseEnumOrStringParam()
        {
            var start = _pos;

            // Parse string literal
            var str = ParseStringLiteral();
            SkipWhitespace();

            // Check for enum-style assignment: 'name' = value
            if (!IsAtEnd && Peek() == '=')
            {
                Advance();
                SkipWhitespace();

                // Parse the value (could be negative)
                var valueStart = _pos;
                if (!IsAtEnd && Peek() == '-')
                    Advance();
                while (!IsAtEnd && char.IsDigit(Peek()))
                    Advance();

                // Return the full enum definition
                return _input[start.._pos];
            }

            // Just a string parameter
            return str;
        }

        private string ParseStringLiteral()
        {
            if (IsAtEnd || Peek() != '\'')
                throw new FormatException($"Expected string literal at position {_pos}");

            var start = _pos;
            Advance(); // consume opening quote

            while (!IsAtEnd)
            {
                var c = Peek();
                // Backslash escape (e.g. a label like 'a\'b' or 'C:\\x' in an Enum(...) type
                // string): consume the backslash and the escaped character verbatim so the
                // escaped quote does not terminate the literal.
                if (c == '\\')
                {
                    Advance();
                    if (!IsAtEnd)
                        Advance();
                    continue;
                }
                if (c == '\'')
                {
                    Advance();
                    // Check for doubled-quote escape ('')
                    if (!IsAtEnd && Peek() == '\'')
                    {
                        Advance();
                        continue;
                    }
                    break;
                }
                Advance();
            }

            return _input[start.._pos];
        }

        private string ParseNumber()
        {
            var start = _pos;

            // Optional minus sign
            if (!IsAtEnd && Peek() == '-')
                Advance();

            // Digits
            while (!IsAtEnd && char.IsDigit(Peek()))
                Advance();

            // Optional decimal part
            if (!IsAtEnd && Peek() == '.')
            {
                Advance();
                while (!IsAtEnd && char.IsDigit(Peek()))
                    Advance();
            }

            if (_pos == start)
                throw new FormatException($"Expected number at position {_pos} in '{_input}'");

            return _input[start.._pos];
        }

        private char Peek() => _input[_pos];

        private void Advance() => _pos++;

        private void SkipWhitespace()
        {
            while (!IsAtEnd && char.IsWhiteSpace(Peek()))
                Advance();
        }
    }
}

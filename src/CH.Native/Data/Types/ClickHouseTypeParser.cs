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

    private ref struct Parser
    {
        private readonly string _input;
        private int _pos;

        public Parser(string input)
        {
            _input = input;
            _pos = 0;
        }

        public bool IsAtEnd => _pos >= _input.Length;

        public ClickHouseType ParseType()
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

            // Parse arguments
            if (!IsAtEnd && Peek() != ')')
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
                fieldNames: fieldNames.Count > 0 ? fieldNames : null);
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
            return baseName is "Nullable" or "Array" or "Map" or "Tuple" or "LowCardinality" or "Nested";
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
                // Could be an identifier (for some complex parameters)
                return ParseIdentifier();
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
                if (c == '\'')
                {
                    Advance();
                    // Check for escaped quote ('')
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

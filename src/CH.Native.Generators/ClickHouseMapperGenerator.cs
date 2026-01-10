using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CH.Native.Generators;

/// <summary>
/// Source generator that creates AOT-compatible mappers for types decorated with [ClickHouseTable].
/// </summary>
[Generator]
public sealed class ClickHouseMapperGenerator : IIncrementalGenerator
{
    private const string TableAttributeName = "CH.Native.Mapping.ClickHouseTableAttribute";
    private const string ColumnAttributeName = "CH.Native.Mapping.ClickHouseColumnAttribute";
    private const string IgnoreAttributeName = "CH.Native.Mapping.ClickHouseIgnoreAttribute";

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // Find all types with [ClickHouseTable]
        var tableClasses = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                TableAttributeName,
                predicate: static (node, _) => node is TypeDeclarationSyntax,
                transform: static (ctx, ct) => ExtractTypeInfo(ctx, ct))
            .Where(static info => info.HasValue)
            .Select(static (info, _) => info!.Value);

        // Generate mapper for each type
        context.RegisterSourceOutput(tableClasses, GenerateMapper);
    }

    private static TypeMappingInfo? ExtractTypeInfo(
        GeneratorAttributeSyntaxContext ctx,
        System.Threading.CancellationToken ct)
    {
        var typeSymbol = ctx.TargetSymbol as INamedTypeSymbol;
        if (typeSymbol is null)
            return null;

        var syntax = ctx.TargetNode as TypeDeclarationSyntax;
        if (syntax is null)
            return null;

        // Check if partial
        bool isPartial = syntax.Modifiers.Any(m => m.IsKind(SyntaxKind.PartialKeyword));

        // Get table name from attribute
        string? tableName = null;
        foreach (var attr in ctx.Attributes)
        {
            if (attr.AttributeClass?.ToDisplayString() == TableAttributeName)
            {
                foreach (var namedArg in attr.NamedArguments)
                {
                    if (namedArg.Key == "TableName" && namedArg.Value.Value is string tn)
                    {
                        tableName = tn;
                    }
                }
            }
        }

        // Extract properties
        var properties = new List<PropertyMappingInfo>();
        var seenColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var diagnostics = new List<DiagnosticInfo>();
        int declarationOrder = 0;

        foreach (var member in typeSymbol.GetMembers())
        {
            if (member is not IPropertySymbol property)
                continue;

            // Skip static properties
            if (property.IsStatic)
                continue;

            // Skip indexers
            if (property.IsIndexer)
                continue;

            // Skip compiler-generated properties (e.g., EqualityContract for records)
            if (property.IsImplicitlyDeclared)
                continue;

            // Skip properties with [CompilerGenerated] attribute
            if (property.GetAttributes().Any(a =>
                a.AttributeClass?.ToDisplayString() == "System.Runtime.CompilerServices.CompilerGeneratedAttribute"))
                continue;

            // Check for [ClickHouseIgnore]
            bool isIgnored = property.GetAttributes()
                .Any(a => a.AttributeClass?.ToDisplayString() == IgnoreAttributeName);
            if (isIgnored)
                continue;

            // Get column attribute if present
            string? columnName = null;
            string? explicitClickHouseType = null;
            int order = int.MaxValue;

            foreach (var attr in property.GetAttributes())
            {
                if (attr.AttributeClass?.ToDisplayString() == ColumnAttributeName)
                {
                    foreach (var namedArg in attr.NamedArguments)
                    {
                        switch (namedArg.Key)
                        {
                            case "Name" when namedArg.Value.Value is string n:
                                columnName = n;
                                break;
                            case "ClickHouseType" when namedArg.Value.Value is string cht:
                                explicitClickHouseType = cht;
                                break;
                            case "Order" when namedArg.Value.Value is int o:
                                order = o;
                                break;
                        }
                    }
                }
            }

            // Default column name is property name
            columnName ??= property.Name;

            // Check for duplicate column names
            if (!seenColumnNames.Add(columnName))
            {
                diagnostics.Add(new DiagnosticInfo(
                    DiagnosticDescriptors.DuplicateColumnName,
                    property.Locations.FirstOrDefault(),
                    columnName));
                continue;
            }

            // Infer ClickHouse type if not explicit
            var clickHouseType = explicitClickHouseType ?? TypeMappingHelper.GetClickHouseType(property.Type);
            if (clickHouseType is null)
            {
                diagnostics.Add(new DiagnosticInfo(
                    DiagnosticDescriptors.UnsupportedType,
                    property.Locations.FirstOrDefault(),
                    property.Name,
                    property.Type.ToDisplayString()));
                continue;
            }

            // Check for getter/setter
            bool hasGetter = property.GetMethod is not null && property.GetMethod.DeclaredAccessibility != Accessibility.Private;
            bool hasSetter = property.SetMethod is not null && property.SetMethod.DeclaredAccessibility != Accessibility.Private;

            // Init-only setters are supported
            if (property.SetMethod?.IsInitOnly == true)
                hasSetter = true;

            if (!hasGetter)
            {
                diagnostics.Add(new DiagnosticInfo(
                    DiagnosticDescriptors.NoGetter,
                    property.Locations.FirstOrDefault(),
                    property.Name));
            }

            if (!hasSetter)
            {
                diagnostics.Add(new DiagnosticInfo(
                    DiagnosticDescriptors.NoSetter,
                    property.Locations.FirstOrDefault(),
                    property.Name));
            }

            // Use declaration order if no explicit order specified
            int effectiveOrder = order == int.MaxValue ? declarationOrder : order;
            declarationOrder++;

            properties.Add(new PropertyMappingInfo(
                property.Name,
                columnName,
                TypeMappingHelper.GetClrTypeName(property.Type),
                clickHouseType,
                TypeMappingHelper.IsNullable(property.Type),
                hasGetter,
                hasSetter,
                effectiveOrder));
        }

        // Sort by order (preserves declaration order for properties without explicit Order)
        properties.Sort((a, b) => a.Order.CompareTo(b.Order));

        // Get namespace
        var ns = typeSymbol.ContainingNamespace.IsGlobalNamespace
            ? null
            : typeSymbol.ContainingNamespace.ToDisplayString();

        return new TypeMappingInfo(
            ns,
            typeSymbol.Name,
            typeSymbol.IsRecord,
            isPartial,
            tableName ?? typeSymbol.Name,
            properties.ToArray(),
            diagnostics.ToArray());
    }

    private static void GenerateMapper(
        SourceProductionContext ctx,
        TypeMappingInfo info)
    {
        // Report diagnostics
        foreach (var diag in info.Diagnostics)
        {
            ctx.ReportDiagnostic(Diagnostic.Create(
                diag.Descriptor,
                diag.Location,
                diag.Args));
        }

        // Don't generate if not partial
        if (!info.IsPartial)
        {
            ctx.ReportDiagnostic(Diagnostic.Create(
                DiagnosticDescriptors.TypeMustBePartial,
                null,
                info.TypeName));
            return;
        }

        // Don't generate if there were errors
        if (info.Diagnostics.Any(d => d.Descriptor.DefaultSeverity == DiagnosticSeverity.Error))
            return;

        var source = GenerateMapperSource(info);
        ctx.AddSource($"{info.TypeName}.ClickHouseMapper.g.cs", source);
    }

    private static string GenerateMapperSource(TypeMappingInfo info)
    {
        var sb = new StringBuilder();

        sb.AppendLine("// <auto-generated />");
        sb.AppendLine("#nullable enable");
        sb.AppendLine();

        if (info.Namespace is not null)
        {
            sb.AppendLine($"namespace {info.Namespace};");
            sb.AppendLine();
        }

        var typeKeyword = info.IsRecord ? "record" : "class";
        sb.AppendLine($"partial {typeKeyword} {info.TypeName}");
        sb.AppendLine("{");
        sb.AppendLine("    /// <summary>");
        sb.AppendLine("    /// Generated ClickHouse mapper for AOT-compatible object mapping.");
        sb.AppendLine("    /// </summary>");
        sb.AppendLine("    internal static class ClickHouseMapper");
        sb.AppendLine("    {");

        // TableName
        sb.AppendLine($"        /// <summary>The ClickHouse table name.</summary>");
        sb.AppendLine($"        public static string TableName => \"{EscapeString(info.TableName)}\";");
        sb.AppendLine();

        // ColumnNames
        sb.AppendLine($"        /// <summary>Column names in order.</summary>");
        sb.Append("        public static string[] ColumnNames { get; } = new[] { ");
        sb.Append(string.Join(", ", info.Properties.Select(p => $"\"{EscapeString(p.ColumnName)}\"")));
        sb.AppendLine(" };");
        sb.AppendLine();

        // ColumnTypes
        sb.AppendLine($"        /// <summary>ClickHouse type names in order.</summary>");
        sb.Append("        public static string[] ColumnTypes { get; } = new[] { ");
        sb.Append(string.Join(", ", info.Properties.Select(p => $"\"{EscapeString(p.ClickHouseType)}\"")));
        sb.AppendLine(" };");
        sb.AppendLine();

        // ReadRow
        GenerateReadRow(sb, info);
        sb.AppendLine();

        // WriteRow
        GenerateWriteRow(sb, info);

        sb.AppendLine("    }");
        sb.AppendLine("}");

        return sb.ToString();
    }

    private static void GenerateReadRow(StringBuilder sb, TypeMappingInfo info)
    {
        sb.AppendLine("        /// <summary>Creates an instance from the current row of a data reader.</summary>");
        sb.AppendLine($"        public static {info.TypeName} ReadRow(global::CH.Native.Results.ClickHouseDataReader reader)");
        sb.AppendLine("        {");

        var readableProps = info.Properties.Where(p => p.HasSetter).ToList();

        if (info.IsRecord || readableProps.Count == info.Properties.Length)
        {
            // Use object initializer syntax
            sb.AppendLine($"            return new {info.TypeName}");
            sb.AppendLine("            {");

            for (int i = 0; i < readableProps.Count; i++)
            {
                var prop = readableProps[i];
                var comma = i < readableProps.Count - 1 ? "," : "";

                if (prop.IsNullable)
                {
                    sb.AppendLine($"                {prop.PropertyName} = reader.IsDBNull(\"{EscapeString(prop.ColumnName)}\") ? default : reader.GetFieldValue<{prop.ClrTypeName}>(\"{EscapeString(prop.ColumnName)}\"){comma}");
                }
                else
                {
                    sb.AppendLine($"                {prop.PropertyName} = reader.GetFieldValue<{prop.ClrTypeName}>(\"{EscapeString(prop.ColumnName)}\"){comma}");
                }
            }

            sb.AppendLine("            };");
        }
        else
        {
            // Use separate statements for partial property access
            sb.AppendLine($"            var result = new {info.TypeName}();");

            foreach (var prop in readableProps)
            {
                if (prop.IsNullable)
                {
                    sb.AppendLine($"            if (!reader.IsDBNull(\"{EscapeString(prop.ColumnName)}\"))");
                    sb.AppendLine($"                result.{prop.PropertyName} = reader.GetFieldValue<{prop.ClrTypeName}>(\"{EscapeString(prop.ColumnName)}\");");
                }
                else
                {
                    sb.AppendLine($"            result.{prop.PropertyName} = reader.GetFieldValue<{prop.ClrTypeName}>(\"{EscapeString(prop.ColumnName)}\");");
                }
            }

            sb.AppendLine("            return result;");
        }

        sb.AppendLine("        }");
    }

    private static void GenerateWriteRow(StringBuilder sb, TypeMappingInfo info)
    {
        sb.AppendLine("        /// <summary>Extracts column values from an instance into a values array.</summary>");
        // Use object?[] instead of Span<object?> to support reflection-based invocation
        sb.AppendLine($"        public static void WriteRow({info.TypeName} row, object?[] values)");
        sb.AppendLine("        {");

        var writableProps = info.Properties.Where(p => p.HasGetter).ToList();

        for (int i = 0; i < writableProps.Count; i++)
        {
            var prop = writableProps[i];
            sb.AppendLine($"            values[{i}] = row.{prop.PropertyName};");
        }

        sb.AppendLine("        }");
    }

    private static string EscapeString(string s)
    {
        return s.Replace("\\", "\\\\").Replace("\"", "\\\"");
    }
}

/// <summary>
/// Extracted type information for source generation.
/// </summary>
internal readonly record struct TypeMappingInfo(
    string? Namespace,
    string TypeName,
    bool IsRecord,
    bool IsPartial,
    string TableName,
    EquatableArray<PropertyMappingInfo> Properties,
    EquatableArray<DiagnosticInfo> Diagnostics);

/// <summary>
/// Extracted property information for source generation.
/// </summary>
internal readonly record struct PropertyMappingInfo(
    string PropertyName,
    string ColumnName,
    string ClrTypeName,
    string ClickHouseType,
    bool IsNullable,
    bool HasGetter,
    bool HasSetter,
    int Order) : IEquatable<PropertyMappingInfo>;

/// <summary>
/// Diagnostic information to report.
/// </summary>
internal readonly struct DiagnosticInfo : IEquatable<DiagnosticInfo>
{
    public DiagnosticDescriptor Descriptor { get; }
    public Location? Location { get; }
    public object?[] Args { get; }

    public DiagnosticInfo(DiagnosticDescriptor descriptor, Location? location, params object?[] args)
    {
        Descriptor = descriptor;
        Location = location;
        Args = args;
    }

    public bool Equals(DiagnosticInfo other)
    {
        // For caching, we only care about the descriptor ID and args
        if (Descriptor.Id != other.Descriptor.Id)
            return false;
        if (Args.Length != other.Args.Length)
            return false;
        for (int i = 0; i < Args.Length; i++)
        {
            if (!Equals(Args[i], other.Args[i]))
                return false;
        }
        return true;
    }

    public override bool Equals(object? obj) => obj is DiagnosticInfo other && Equals(other);

    public override int GetHashCode()
    {
        unchecked
        {
            int hash = Descriptor.Id.GetHashCode();
            foreach (var arg in Args)
            {
                hash = hash * 31 + (arg?.GetHashCode() ?? 0);
            }
            return hash;
        }
    }
}

using System.Collections;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using CH.Native.Parameters;

namespace CH.Native.Linq;

/// <summary>
/// Translates LINQ expression trees into ClickHouse SQL.
/// Handles operator ordering and clause building.
/// </summary>
internal sealed class ClickHouseExpressionVisitor : ExpressionVisitor
{
    private readonly ClickHouseQueryContext _context;
    private readonly SqlBuilder _sql;
    private readonly StringBuilder _currentExpression = new();

    public ClickHouseExpressionVisitor(ClickHouseQueryContext context)
    {
        _context = context;
        _sql = new SqlBuilder();
        _sql.Table(_context.TableName);
    }

    /// <summary>
    /// Translates the expression tree to SQL.
    /// </summary>
    public string Translate(Expression expression)
    {
        Visit(expression);
        return _sql.Build();
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Handle Queryable extension methods
        if (node.Method.DeclaringType == typeof(Queryable) ||
            node.Method.DeclaringType == typeof(Enumerable))
        {
            // Visit source first (innermost query)
            if (node.Arguments.Count > 0)
            {
                Visit(node.Arguments[0]);
            }

            return node.Method.Name switch
            {
                nameof(Queryable.Where) => VisitWhere(node),
                nameof(Queryable.Select) => VisitSelect(node),
                nameof(Queryable.OrderBy) => VisitOrderBy(node, descending: false, clear: true),
                nameof(Queryable.OrderByDescending) => VisitOrderBy(node, descending: true, clear: true),
                nameof(Queryable.ThenBy) => VisitOrderBy(node, descending: false, clear: false),
                nameof(Queryable.ThenByDescending) => VisitOrderBy(node, descending: true, clear: false),
                nameof(Queryable.Take) => VisitTake(node),
                nameof(Queryable.Skip) => VisitSkip(node),
                nameof(Queryable.First) or nameof(Queryable.FirstOrDefault) => VisitFirst(node),
                nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault) => VisitSingle(node),
                nameof(Queryable.Count) or nameof(Queryable.LongCount) => VisitCount(node),
                nameof(Queryable.Sum) => VisitAggregate(node, "sum"),
                nameof(Queryable.Average) => VisitAggregate(node, "avg"),
                nameof(Queryable.Min) => VisitAggregate(node, "min"),
                nameof(Queryable.Max) => VisitAggregate(node, "max"),
                nameof(Queryable.Any) => VisitAny(node),
                nameof(Queryable.All) => VisitAll(node),
                nameof(Queryable.Distinct) => VisitDistinct(node),
                nameof(Queryable.GroupBy) => VisitGroupBy(node),
                _ => throw new NotSupportedException($"LINQ method '{node.Method.Name}' is not supported")
            };
        }

        // Handle ClickHouse-specific extension methods
        if (node.Method.DeclaringType == typeof(ClickHouseQueryableExtensions))
        {
            // Visit source first
            if (node.Arguments.Count > 0)
            {
                Visit(node.Arguments[0]);
            }

            return node.Method.Name switch
            {
                nameof(ClickHouseQueryableExtensions.Final) => VisitFinal(node),
                nameof(ClickHouseQueryableExtensions.Sample) => VisitSample(node),
                _ => throw new NotSupportedException($"ClickHouse extension '{node.Method.Name}' is not supported")
            };
        }

        return base.VisitMethodCall(node);
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        // Skip IQueryable constants (root query source)
        if (node.Value is IQueryable)
            return node;

        return base.VisitConstant(node);
    }

    #region LINQ Operators

    private Expression VisitWhere(MethodCallExpression node)
    {
        var lambda = GetLambda(node.Arguments[1]);
        var whereSql = TranslatePredicate(lambda.Body);
        _sql.Where(whereSql);
        return node;
    }

    private Expression VisitSelect(MethodCallExpression node)
    {
        var lambda = GetLambda(node.Arguments[1]);
        TranslateProjection(lambda);
        return node;
    }

    private Expression VisitOrderBy(MethodCallExpression node, bool descending, bool clear)
    {
        var lambda = GetLambda(node.Arguments[1]);
        var columnSql = TranslateExpression(lambda.Body);

        if (clear)
        {
            _sql.ClearAndOrderBy(columnSql, descending);
        }
        else
        {
            _sql.OrderBy(columnSql, descending);
        }

        return node;
    }

    private Expression VisitTake(MethodCallExpression node)
    {
        var count = GetConstantValue<int>(node.Arguments[1]);
        _sql.Limit(count);
        return node;
    }

    private Expression VisitSkip(MethodCallExpression node)
    {
        var count = GetConstantValue<int>(node.Arguments[1]);
        _sql.Offset(count);
        return node;
    }

    private Expression VisitFirst(MethodCallExpression node)
    {
        // First with predicate
        if (node.Arguments.Count > 1)
        {
            var lambda = GetLambda(node.Arguments[1]);
            var whereSql = TranslatePredicate(lambda.Body);
            _sql.Where(whereSql);
        }
        _sql.Limit(1);
        return node;
    }

    private Expression VisitSingle(MethodCallExpression node)
    {
        // Single with predicate
        if (node.Arguments.Count > 1)
        {
            var lambda = GetLambda(node.Arguments[1]);
            var whereSql = TranslatePredicate(lambda.Body);
            _sql.Where(whereSql);
        }
        // Take 2 to detect multiple results
        _sql.Limit(2);
        return node;
    }

    private Expression VisitCount(MethodCallExpression node)
    {
        // Count with predicate
        if (node.Arguments.Count > 1)
        {
            var lambda = GetLambda(node.Arguments[1]);
            var whereSql = TranslatePredicate(lambda.Body);
            _sql.Where(whereSql);
        }
        _sql.SetAggregate("count");
        return node;
    }

    private Expression VisitAggregate(MethodCallExpression node, string function)
    {
        if (node.Arguments.Count > 1)
        {
            var lambda = GetLambda(node.Arguments[1]);
            var columnSql = TranslateExpression(lambda.Body);
            _sql.SetAggregate(function, columnSql);
        }
        else
        {
            _sql.SetAggregate(function, "*");
        }
        return node;
    }

    private Expression VisitAny(MethodCallExpression node)
    {
        // Any with predicate
        if (node.Arguments.Count > 1)
        {
            var lambda = GetLambda(node.Arguments[1]);
            var whereSql = TranslatePredicate(lambda.Body);
            _sql.Where(whereSql);
        }
        _sql.Select("1");
        _sql.Limit(1);
        return node;
    }

    private Expression VisitAll(MethodCallExpression node)
    {
        // All(predicate) translates to: NOT EXISTS (... WHERE NOT predicate)
        // For simplicity, we use: count() where NOT predicate = 0
        var lambda = GetLambda(node.Arguments[1]);

        // Negate the predicate
        var negated = Expression.Not(lambda.Body);
        var whereSql = TranslatePredicate(negated);
        _sql.Where(whereSql);
        _sql.SetAggregate("count");
        return node;
    }

    private Expression VisitDistinct(MethodCallExpression node)
    {
        _sql.Distinct();
        return node;
    }

    private Expression VisitGroupBy(MethodCallExpression node)
    {
        var lambda = GetLambda(node.Arguments[1]);
        var columnSql = TranslateExpression(lambda.Body);
        _sql.GroupBy(columnSql);
        return node;
    }

    private Expression VisitFinal(MethodCallExpression node)
    {
        _sql.Final();
        return node;
    }

    private Expression VisitSample(MethodCallExpression node)
    {
        var ratio = GetConstantValue<double>(node.Arguments[1]);
        _sql.Sample(ratio);
        return node;
    }

    #endregion

    #region Expression Translation

    /// <summary>
    /// Translates a predicate expression to SQL.
    /// </summary>
    private string TranslatePredicate(Expression expression)
    {
        _currentExpression.Clear();
        VisitPredicate(expression);
        return _currentExpression.ToString();
    }

    /// <summary>
    /// Translates a simple expression (column, constant) to SQL.
    /// </summary>
    private string TranslateExpression(Expression expression)
    {
        _currentExpression.Clear();
        VisitPredicate(expression);
        return _currentExpression.ToString();
    }

    private void VisitPredicate(Expression expression)
    {
        switch (expression)
        {
            case BinaryExpression binary:
                VisitBinaryPredicate(binary);
                break;

            case MemberExpression member:
                VisitMemberPredicate(member);
                break;

            case ConstantExpression constant:
                AppendConstant(constant.Value);
                break;

            case UnaryExpression unary:
                VisitUnaryPredicate(unary);
                break;

            case MethodCallExpression method:
                VisitMethodPredicate(method);
                break;

            case NewExpression newExpr:
                // Handle new { x.A, x.B } for multi-column GroupBy
                VisitNewExpressionPredicate(newExpr);
                break;

            case ConditionalExpression conditional:
                // x ? y : z -> if(x, y, z) in ClickHouse
                _currentExpression.Append("if(");
                VisitPredicate(conditional.Test);
                _currentExpression.Append(", ");
                VisitPredicate(conditional.IfTrue);
                _currentExpression.Append(", ");
                VisitPredicate(conditional.IfFalse);
                _currentExpression.Append(')');
                break;

            default:
                throw new NotSupportedException($"Expression type '{expression.NodeType}' is not supported");
        }
    }

    private void VisitBinaryPredicate(BinaryExpression binary)
    {
        // Handle null comparisons specially
        if (IsNullConstant(binary.Right))
        {
            VisitPredicate(binary.Left);
            _currentExpression.Append(binary.NodeType == ExpressionType.Equal
                ? " IS NULL"
                : " IS NOT NULL");
            return;
        }

        if (IsNullConstant(binary.Left))
        {
            VisitPredicate(binary.Right);
            _currentExpression.Append(binary.NodeType == ExpressionType.Equal
                ? " IS NULL"
                : " IS NOT NULL");
            return;
        }

        _currentExpression.Append('(');
        VisitPredicate(binary.Left);

        _currentExpression.Append(binary.NodeType switch
        {
            ExpressionType.Equal => " = ",
            ExpressionType.NotEqual => " != ",
            ExpressionType.LessThan => " < ",
            ExpressionType.LessThanOrEqual => " <= ",
            ExpressionType.GreaterThan => " > ",
            ExpressionType.GreaterThanOrEqual => " >= ",
            ExpressionType.AndAlso => " AND ",
            ExpressionType.OrElse => " OR ",
            ExpressionType.Add => " + ",
            ExpressionType.Subtract => " - ",
            ExpressionType.Multiply => " * ",
            ExpressionType.Divide => " / ",
            ExpressionType.Modulo => " % ",
            _ => throw new NotSupportedException($"Binary operator '{binary.NodeType}' is not supported")
        });

        VisitPredicate(binary.Right);
        _currentExpression.Append(')');
    }

    private void VisitMemberPredicate(MemberExpression member)
    {
        // Check if this is a captured variable (closure)
        if (member.Expression is ConstantExpression)
        {
            var value = GetMemberValue(member);
            AppendConstant(value);
            return;
        }

        // Check if this is a property on the entity type (lambda parameter)
        if (member.Expression is ParameterExpression)
        {
            var columnName = TableNameResolver.ToSnakeCase(member.Member.Name);
            _currentExpression.Append(columnName);
            return;
        }

        // Handle nested member access for captured variables
        if (member.Expression is MemberExpression parentMember)
        {
            // Check for DateTime properties
            if (member.Member.Name == "Year" && IsDateTimeType(parentMember.Type))
            {
                _currentExpression.Append("toYear(");
                VisitPredicate(parentMember);
                _currentExpression.Append(')');
                return;
            }
            if (member.Member.Name == "Month" && IsDateTimeType(parentMember.Type))
            {
                _currentExpression.Append("toMonth(");
                VisitPredicate(parentMember);
                _currentExpression.Append(')');
                return;
            }
            if (member.Member.Name == "Day" && IsDateTimeType(parentMember.Type))
            {
                _currentExpression.Append("toDayOfMonth(");
                VisitPredicate(parentMember);
                _currentExpression.Append(')');
                return;
            }

            // Try to evaluate the full path as a captured variable
            var value = GetMemberValue(member);
            AppendConstant(value);
            return;
        }

        // Handle Nullable<T>.HasValue and .Value
        if (member.Member.Name == "HasValue" && IsNullableType(member.Expression?.Type))
        {
            VisitPredicate(member.Expression!);
            _currentExpression.Append(" IS NOT NULL");
            return;
        }

        if (member.Member.Name == "Value" && IsNullableType(member.Expression?.Type))
        {
            VisitPredicate(member.Expression!);
            return;
        }

        throw new NotSupportedException($"Member access '{member.Member.Name}' on '{member.Expression?.Type?.Name}' is not supported");
    }

    private void VisitUnaryPredicate(UnaryExpression unary)
    {
        switch (unary.NodeType)
        {
            case ExpressionType.Not:
                _currentExpression.Append("NOT (");
                VisitPredicate(unary.Operand);
                _currentExpression.Append(')');
                break;

            case ExpressionType.Negate:
            case ExpressionType.NegateChecked:
                _currentExpression.Append("-(");
                VisitPredicate(unary.Operand);
                _currentExpression.Append(')');
                break;

            case ExpressionType.Convert:
            case ExpressionType.ConvertChecked:
            case ExpressionType.Quote:
                // Pass through conversions
                VisitPredicate(unary.Operand);
                break;

            default:
                throw new NotSupportedException($"Unary operator '{unary.NodeType}' is not supported");
        }
    }

    private void VisitMethodPredicate(MethodCallExpression method)
    {
        // String instance methods
        if (method.Object?.Type == typeof(string))
        {
            VisitStringMethod(method);
            return;
        }

        // Collection Contains (list.Contains(x.Id))
        if (method.Method.Name == "Contains" && method.Object != null)
        {
            VisitCollectionContains(method);
            return;
        }

        // Static Enumerable.Contains
        if (method.Method.DeclaringType == typeof(Enumerable) && method.Method.Name == "Contains")
        {
            VisitEnumerableContains(method);
            return;
        }

        // MemoryExtensions.Contains for arrays
        if (method.Method.DeclaringType?.Name == "MemoryExtensions" && method.Method.Name == "Contains")
        {
            VisitMemoryExtensionsContains(method);
            return;
        }

        // String.IsNullOrEmpty
        if (method.Method.DeclaringType == typeof(string) && method.Method.Name == nameof(string.IsNullOrEmpty))
        {
            _currentExpression.Append('(');
            VisitPredicate(method.Arguments[0]);
            _currentExpression.Append(" IS NULL OR ");
            VisitPredicate(method.Arguments[0]);
            _currentExpression.Append(" = '')");
            return;
        }

        throw new NotSupportedException($"Method '{method.Method.Name}' on '{method.Method.DeclaringType?.Name}' is not supported");
    }

    private void VisitStringMethod(MethodCallExpression method)
    {
        switch (method.Method.Name)
        {
            case nameof(string.Contains):
                VisitPredicate(method.Object!);
                _currentExpression.Append(" LIKE ");
                var containsValue = GetConstantValue<string>(method.Arguments[0]);
                _currentExpression.Append(EscapeLikePattern($"%{containsValue}%"));
                break;

            case nameof(string.StartsWith):
                VisitPredicate(method.Object!);
                _currentExpression.Append(" LIKE ");
                var startsValue = GetConstantValue<string>(method.Arguments[0]);
                _currentExpression.Append(EscapeLikePattern($"{startsValue}%"));
                break;

            case nameof(string.EndsWith):
                VisitPredicate(method.Object!);
                _currentExpression.Append(" LIKE ");
                var endsValue = GetConstantValue<string>(method.Arguments[0]);
                _currentExpression.Append(EscapeLikePattern($"%{endsValue}"));
                break;

            case nameof(string.ToLower):
            case nameof(string.ToLowerInvariant):
                _currentExpression.Append("lower(");
                VisitPredicate(method.Object!);
                _currentExpression.Append(')');
                break;

            case nameof(string.ToUpper):
            case nameof(string.ToUpperInvariant):
                _currentExpression.Append("upper(");
                VisitPredicate(method.Object!);
                _currentExpression.Append(')');
                break;

            case nameof(string.Trim):
                _currentExpression.Append("trim(");
                VisitPredicate(method.Object!);
                _currentExpression.Append(')');
                break;

            case nameof(string.TrimStart):
                _currentExpression.Append("trimLeft(");
                VisitPredicate(method.Object!);
                _currentExpression.Append(')');
                break;

            case nameof(string.TrimEnd):
                _currentExpression.Append("trimRight(");
                VisitPredicate(method.Object!);
                _currentExpression.Append(')');
                break;

            default:
                throw new NotSupportedException($"String method '{method.Method.Name}' is not supported");
        }
    }

    private void VisitCollectionContains(MethodCallExpression method)
    {
        // list.Contains(x.Id) -> id IN (1, 2, 3)
        var collection = GetMemberValue(method.Object as MemberExpression) as IEnumerable;
        if (collection == null)
            throw new InvalidOperationException("Contains requires a non-null collection");

        var values = collection.Cast<object>().ToList();

        if (values.Count == 0)
        {
            // Empty collection: always false
            _currentExpression.Append("1 = 0");
            return;
        }

        VisitPredicate(method.Arguments[0]);
        _currentExpression.Append(" IN (");

        bool first = true;
        foreach (var item in values)
        {
            if (!first) _currentExpression.Append(", ");
            AppendConstant(item);
            first = false;
        }

        _currentExpression.Append(')');
    }

    private void VisitMemoryExtensionsContains(MethodCallExpression method)
    {
        // MemoryExtensions.Contains(span, value) where span is created from an array
        // We need to find the underlying array through the span conversion
        var spanExpr = method.Arguments[0];

        // Extract the array from the span expression
        var array = ExtractArrayFromSpanExpression(spanExpr);
        if (array == null)
            throw new InvalidOperationException("Could not extract array from span expression for Contains");

        var values = array.Cast<object>().ToList();

        if (values.Count == 0)
        {
            _currentExpression.Append("1 = 0");
            return;
        }

        VisitPredicate(method.Arguments[1]);
        _currentExpression.Append(" IN (");

        bool first = true;
        foreach (var item in values)
        {
            if (!first) _currentExpression.Append(", ");
            AppendConstant(item);
            first = false;
        }

        _currentExpression.Append(')');
    }

    private static IEnumerable? ExtractArrayFromSpanExpression(Expression expression)
    {
        // Recursively unwrap until we find the array
        return expression switch
        {
            ConstantExpression constant => constant.Value as IEnumerable,
            MemberExpression member => GetMemberValue(member) as IEnumerable,
            UnaryExpression { NodeType: ExpressionType.Convert } convert => ExtractArrayFromSpanExpression(convert.Operand),
            MethodCallExpression methodCall when methodCall.Method.IsSpecialName =>
                methodCall.Arguments.Count > 0
                    ? ExtractArrayFromSpanExpression(methodCall.Arguments[0])
                    : methodCall.Object != null
                        ? ExtractArrayFromSpanExpression(methodCall.Object)
                        : null,
            NewArrayExpression newArray => EvaluateNewArray(newArray),
            _ => null
        };
    }

    private static IEnumerable? EvaluateNewArray(NewArrayExpression newArray)
    {
        var elementType = newArray.Type.GetElementType()!;
        var array = Array.CreateInstance(elementType, newArray.Expressions.Count);
        for (int i = 0; i < newArray.Expressions.Count; i++)
        {
            var value = EvaluateExpressionSafe(newArray.Expressions[i]);
            array.SetValue(value, i);
        }
        return array;
    }

    private void VisitEnumerableContains(MethodCallExpression method)
    {
        // Enumerable.Contains(collection, value) -> value IN (collection)
        var collectionExpr = method.Arguments[0];
        var collection = EvaluateExpressionSafe(collectionExpr) as IEnumerable;
        if (collection == null)
            throw new InvalidOperationException("Contains requires a non-null collection");

        var values = collection.Cast<object>().ToList();

        if (values.Count == 0)
        {
            _currentExpression.Append("1 = 0");
            return;
        }

        VisitPredicate(method.Arguments[1]);
        _currentExpression.Append(" IN (");

        bool first = true;
        foreach (var item in values)
        {
            if (!first) _currentExpression.Append(", ");
            AppendConstant(item);
            first = false;
        }

        _currentExpression.Append(')');
    }

    private static object? EvaluateExpressionSafe(Expression expression)
    {
        // Handle constant expressions directly
        if (expression is ConstantExpression constant)
            return constant.Value;

        // Handle unary conversions (unwrap them)
        if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
            return EvaluateExpressionSafe(unary.Operand);

        // Handle method calls that are implicit operators (like span conversion)
        if (expression is MethodCallExpression methodCall && methodCall.Method.IsSpecialName)
        {
            // Implicit operator - evaluate the argument
            if (methodCall.Arguments.Count > 0)
                return EvaluateExpressionSafe(methodCall.Arguments[0]);
            if (methodCall.Object != null)
                return EvaluateExpressionSafe(methodCall.Object);
        }

        // Handle member access on constants (captured variables)
        if (expression is MemberExpression member)
            return GetMemberValue(member);

        // Handle NewArrayInit for inline arrays
        if (expression is NewArrayExpression newArray)
        {
            var elementType = expression.Type.GetElementType()!;
            var array = Array.CreateInstance(elementType, newArray.Expressions.Count);
            for (int i = 0; i < newArray.Expressions.Count; i++)
            {
                var value = EvaluateExpressionSafe(newArray.Expressions[i]);
                array.SetValue(value, i);
            }
            return array;
        }

        // For member expressions that access array elements
        if (expression is MemberExpression memberExpr)
            return GetMemberValue(memberExpr);

        // As a last resort, try to compile - but avoid if it's a span type
        if (expression.Type.FullName?.Contains("Span") == true)
            throw new NotSupportedException($"Cannot evaluate span expression: {expression}");

        return EvaluateExpression(expression);
    }

    private void VisitNewExpressionPredicate(NewExpression newExpr)
    {
        // new { x.A, x.B } for multi-column expressions
        bool first = true;
        foreach (var arg in newExpr.Arguments)
        {
            if (!first) _currentExpression.Append(", ");
            VisitPredicate(arg);
            first = false;
        }
    }

    private void TranslateProjection(LambdaExpression lambda)
    {
        switch (lambda.Body)
        {
            case NewExpression newExpr:
                // Anonymous type: new { x.Id, Name = x.CustomerName }
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var argSql = TranslateExpression(newExpr.Arguments[i]);
                    var alias = newExpr.Members?[i].Name;
                    if (alias != null)
                    {
                        _sql.Select(argSql, alias);
                    }
                    else
                    {
                        _sql.Select(argSql);
                    }
                }
                break;

            case MemberInitExpression memberInit:
                // DTO: new UserDto { Id = x.Id, Name = x.CustomerName }
                foreach (var binding in memberInit.Bindings.OfType<MemberAssignment>())
                {
                    var exprSql = TranslateExpression(binding.Expression);
                    _sql.Select(exprSql, binding.Member.Name);
                }
                break;

            case MemberExpression member:
                // Single property: x => x.Id
                var columnSql = TranslateExpression(member);
                _sql.Select(columnSql);
                break;

            default:
                // Expression: x => x.Id + 1
                var sql = TranslateExpression(lambda.Body);
                _sql.Select(sql);
                break;
        }
    }

    #endregion

    #region Helpers

    private void AppendConstant(object? value)
    {
        if (value is null)
        {
            _currentExpression.Append("NULL");
            return;
        }

        switch (value)
        {
            case string s:
                _currentExpression.Append(ParameterSerializer.EscapeString(s));
                break;

            case bool b:
                _currentExpression.Append(b ? "1" : "0");
                break;

            case DateTime dt:
                _currentExpression.Append($"toDateTime('{dt:yyyy-MM-dd HH:mm:ss}')");
                break;

            case DateTimeOffset dto:
                _currentExpression.Append($"toDateTime('{dto.UtcDateTime:yyyy-MM-dd HH:mm:ss}')");
                break;

            case DateOnly d:
                _currentExpression.Append($"toDate('{d:yyyy-MM-dd}')");
                break;

            case Guid g:
                _currentExpression.Append($"toUUID('{g:D}')");
                break;

            case IFormattable f:
                _currentExpression.Append(f.ToString(null, CultureInfo.InvariantCulture));
                break;

            default:
                _currentExpression.Append(value.ToString());
                break;
        }
    }

    private static string EscapeLikePattern(string pattern)
    {
        // Escape ClickHouse LIKE special characters in the search term
        // Note: The % and _ in the pattern template are intentional wildcards
        var sb = new StringBuilder(pattern.Length + 10);
        sb.Append('\'');

        foreach (var c in pattern)
        {
            switch (c)
            {
                case '\'':
                    sb.Append("\\'");
                    break;
                case '\\':
                    sb.Append("\\\\");
                    break;
                default:
                    sb.Append(c);
                    break;
            }
        }

        sb.Append('\'');
        return sb.ToString();
    }

    private static LambdaExpression GetLambda(Expression expression)
    {
        if (expression is UnaryExpression { NodeType: ExpressionType.Quote } unary)
            return (LambdaExpression)unary.Operand;
        return (LambdaExpression)expression;
    }

    private static T GetConstantValue<T>(Expression expression)
    {
        var value = EvaluateExpression(expression);
        return (T)value!;
    }

    private static object? EvaluateExpression(Expression expression)
    {
        // Handle constant expressions directly
        if (expression is ConstantExpression constant)
            return constant.Value;

        // Handle member access on constants (captured variables)
        if (expression is MemberExpression member)
            return GetMemberValue(member);

        // Compile and execute the expression
        var lambda = Expression.Lambda<Func<object?>>(Expression.Convert(expression, typeof(object)));
        return lambda.Compile()();
    }

    private static object? GetMemberValue(MemberExpression? member)
    {
        if (member == null)
            return null;

        // Build the member chain
        var chain = new List<MemberInfo>();
        Expression? current = member;

        while (current is MemberExpression memberExpr)
        {
            chain.Add(memberExpr.Member);
            current = memberExpr.Expression;
        }

        if (current is not ConstantExpression constExpr)
        {
            // Fallback: compile and execute
            var lambda = Expression.Lambda<Func<object?>>(Expression.Convert(member, typeof(object)));
            return lambda.Compile()();
        }

        // Evaluate the chain from root to leaf
        object? value = constExpr.Value;
        for (int i = chain.Count - 1; i >= 0; i--)
        {
            if (value == null)
                return null;

            var memberInfo = chain[i];
            value = memberInfo switch
            {
                FieldInfo field => field.GetValue(value),
                PropertyInfo prop => prop.GetValue(value),
                _ => throw new NotSupportedException($"Member type '{memberInfo.MemberType}' is not supported")
            };
        }

        return value;
    }

    private static bool IsNullConstant(Expression expr)
    {
        return expr is ConstantExpression { Value: null };
    }

    private static bool IsDateTimeType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset);
    }

    private static bool IsNullableType(Type? type)
    {
        return type != null && Nullable.GetUnderlyingType(type) != null;
    }

    #endregion
}

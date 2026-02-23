using System.Linq.Expressions;
using CH.Native.Connection;
using CH.Native.Linq;
using Xunit;

namespace CH.Native.Tests.Unit.Linq;

public class SqlGeneratorTests
{
    #region Test Setup

    private static string GenerateSql<T>(Func<IQueryable<T>, IQueryable<T>> transform)
    {
        var context = new ClickHouseQueryContext(
            connection: null!, // Not used for SQL generation
            tableName: TableNameResolver.ToSnakeCase(typeof(T).Name),
            elementType: typeof(T));

        var queryable = new ClickHouseQueryable<T>(context);
        var transformed = transform(queryable);

        return transformed.ToSql();
    }

    private static string GenerateSql<T>(IQueryable<T> queryable)
    {
        return ((ClickHouseQueryable<T>)queryable).ToSql();
    }

    #endregion

    #region Test Models

    public class TestOrder
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public string CustomerName { get; set; } = "";
        public decimal Amount { get; set; }
        public int Quantity { get; set; }
        public string Status { get; set; } = "";
        public bool IsActive { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? ShippedDate { get; set; }
        public string? Notes { get; set; }
    }

    #endregion

    #region Table Name Resolution

    [Fact]
    public void Table_UsesSnakeCaseTableName()
    {
        var context = new ClickHouseQueryContext(null!, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var sql = queryable.ToSql();

        Assert.Contains("FROM test_order", sql);
    }

    [Theory]
    [InlineData("UserAccount", "user_account")]
    [InlineData("XMLParser", "xml_parser")]
    [InlineData("ID", "id")]
    [InlineData("OrderID", "order_id")]
    [InlineData("HTTPRequest", "http_request")]
    [InlineData("User", "user")]
    public void ToSnakeCase_ConvertsCorrectly(string input, string expected)
    {
        var result = TableNameResolver.ToSnakeCase(input);
        Assert.Equal(expected, result);
    }

    #endregion

    #region Where Clause Tests

    [Fact]
    public void Where_Equality_GeneratesCorrectSql()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.Id == 42));

        Assert.Contains("WHERE", sql);
        Assert.Contains("id = 42", sql);
    }

    [Fact]
    public void Where_StringEquality_GeneratesQuotedValue()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.CustomerName == "John"));

        Assert.Contains("customer_name = 'John'", sql);
    }

    [Fact]
    public void Where_GreaterThan_GeneratesCorrectOperator()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.Amount > 100));

        Assert.Contains("amount > 100", sql);
    }

    [Fact]
    public void Where_LessThanOrEqual_GeneratesCorrectOperator()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.Quantity <= 10));

        Assert.Contains("quantity <= 10", sql);
    }

    [Fact]
    public void Where_AndCondition_GeneratesAndOperator()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.IsActive && o.Amount > 0));

        Assert.Contains("AND", sql);
        Assert.Contains("is_active", sql);
        Assert.Contains("amount > 0", sql);
    }

    [Fact]
    public void Where_OrCondition_GeneratesOrOperator()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.Status == "New" || o.Status == "Pending"));

        Assert.Contains("OR", sql);
    }

    [Fact]
    public void Where_NotCondition_GeneratesNotOperator()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => !o.IsActive));

        Assert.Contains("NOT", sql);
    }

    [Fact]
    public void Where_NullableEqualsNull_GeneratesIsNull()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.ShippedDate == null));

        Assert.Contains("IS NULL", sql);
    }

    [Fact]
    public void Where_NullableNotNull_GeneratesIsNotNull()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.ShippedDate != null));

        Assert.Contains("IS NOT NULL", sql);
    }

    [Fact]
    public void Where_MultipleConditions_CombinesWithAnd()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.Amount > 100)
             .Where(o => o.Status == "Active"));

        Assert.Contains("AND", sql);
        Assert.Contains("amount > 100", sql);
        Assert.Contains("status = 'Active'", sql);
    }

    #endregion

    #region String Method Tests

    [Fact]
    public void Where_StringContains_GeneratesLike()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.CustomerName.Contains("Smith")));

        Assert.Contains("LIKE '%Smith%'", sql);
    }

    [Fact]
    public void Where_StringStartsWith_GeneratesLikePrefix()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.CustomerName.StartsWith("John")));

        Assert.Contains("LIKE 'John%'", sql);
    }

    [Fact]
    public void Where_StringEndsWith_GeneratesLikeSuffix()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.CustomerName.EndsWith("son")));

        Assert.Contains("LIKE '%son'", sql);
    }

    [Fact]
    public void Where_StringToLower_GeneratesLowerFunction()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.CustomerName.ToLower() == "john"));

        Assert.Contains("lower(customer_name)", sql);
    }

    [Fact]
    public void Where_StringToUpper_GeneratesUpperFunction()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.CustomerName.ToUpper() == "JOHN"));

        Assert.Contains("upper(customer_name)", sql);
    }

    [Fact]
    public void Where_StringTrim_GeneratesTrimFunction()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.CustomerName.Trim() == "John"));

        Assert.Contains("trim(customer_name)", sql);
    }

    #endregion

    #region Contains (IN clause) Tests

    [Fact]
    public void Where_ListContains_GeneratesInClause()
    {
        var ids = new List<int> { 1, 2, 3, 4, 5 };
        var sql = GenerateSql<TestOrder>(q => q.Where(o => ids.Contains(o.Id)));

        Assert.Contains("id IN (1, 2, 3, 4, 5)", sql);
    }

    [Fact]
    public void Where_ArrayContains_GeneratesInClause()
    {
        var statuses = new[] { "Pending", "Processing" };
        var sql = GenerateSql<TestOrder>(q => q.Where(o => statuses.Contains(o.Status)));

        Assert.Contains("status IN ('Pending', 'Processing')", sql);
    }

    [Fact]
    public void Where_EmptyListContains_GeneratesAlwaysFalse()
    {
        var ids = new List<int>();
        var sql = GenerateSql<TestOrder>(q => q.Where(o => ids.Contains(o.Id)));

        Assert.Contains("1 = 0", sql);
    }

    #endregion

    #region OrderBy Tests

    [Fact]
    public void OrderBy_SingleColumn_GeneratesOrderByClause()
    {
        var sql = GenerateSql<TestOrder>(q => q.OrderBy(o => o.CreatedAt));

        Assert.Contains("ORDER BY created_at", sql);
        Assert.DoesNotContain("DESC", sql);
    }

    [Fact]
    public void OrderByDescending_SingleColumn_GeneratesDescClause()
    {
        var sql = GenerateSql<TestOrder>(q => q.OrderByDescending(o => o.CreatedAt));

        Assert.Contains("ORDER BY created_at DESC", sql);
    }

    [Fact]
    public void OrderBy_ThenBy_GeneratesMultipleColumns()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.OrderBy(o => o.CustomerId).ThenBy(o => o.CreatedAt));

        Assert.Contains("ORDER BY customer_id, created_at", sql);
    }

    [Fact]
    public void OrderBy_ThenByDescending_GeneratesMixedOrder()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.OrderBy(o => o.CustomerId).ThenByDescending(o => o.Amount));

        Assert.Contains("ORDER BY customer_id, amount DESC", sql);
    }

    #endregion

    #region Take/Skip Tests

    [Fact]
    public void Take_GeneratesLimitClause()
    {
        var sql = GenerateSql<TestOrder>(q => q.Take(10));

        Assert.Contains("LIMIT 10", sql);
    }

    [Fact]
    public void Skip_GeneratesOffsetClause()
    {
        var sql = GenerateSql<TestOrder>(q => q.Skip(20));

        Assert.Contains("OFFSET 20", sql);
    }

    [Fact]
    public void Skip_Take_GeneratesLimitWithOffset()
    {
        var sql = GenerateSql<TestOrder>(q => q.Skip(20).Take(10));

        Assert.Contains("LIMIT 10", sql);
        Assert.Contains("OFFSET 20", sql);
    }

    #endregion

    #region First/Single Tests

    [Fact]
    public void First_AddsLimit1()
    {
        var context = new ClickHouseQueryContext(null!, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        // We can't call FirstAsync without a connection, but we can test the expression
        var expression = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.First),
            new[] { typeof(TestOrder) },
            queryable.Expression);

        var visitor = new ClickHouseExpressionVisitor(context);
        var sql = visitor.Translate(expression);

        Assert.Contains("LIMIT 1", sql);
    }

    [Fact]
    public void Single_AddsLimit2()
    {
        var context = new ClickHouseQueryContext(null!, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var expression = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Single),
            new[] { typeof(TestOrder) },
            queryable.Expression);

        var visitor = new ClickHouseExpressionVisitor(context);
        var sql = visitor.Translate(expression);

        Assert.Contains("LIMIT 2", sql);
    }

    #endregion

    #region Aggregate Tests

    [Fact]
    public void Count_GeneratesCountFunction()
    {
        var context = new ClickHouseQueryContext(null!, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var expression = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Count),
            new[] { typeof(TestOrder) },
            queryable.Expression);

        var visitor = new ClickHouseExpressionVisitor(context);
        var sql = visitor.Translate(expression);

        Assert.Contains("SELECT count()", sql);
    }

    [Fact]
    public void Sum_GeneratesSumFunction()
    {
        var context = new ClickHouseQueryContext(null!, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        Expression<Func<TestOrder, decimal>> selector = o => o.Amount;

        var expression = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Sum),
            new[] { typeof(TestOrder) },
            queryable.Expression,
            selector);

        var visitor = new ClickHouseExpressionVisitor(context);
        var sql = visitor.Translate(expression);

        Assert.Contains("sum(amount)", sql);
    }

    #endregion

    #region Distinct Tests

    [Fact]
    public void Distinct_GeneratesDistinctKeyword()
    {
        var sql = GenerateSql<TestOrder>(q => q.Distinct());

        Assert.Contains("SELECT DISTINCT", sql);
    }

    #endregion

    #region ClickHouse Extensions Tests

    [Fact]
    public void Final_GeneratesFinalKeyword()
    {
        var sql = GenerateSql<TestOrder>(q => q.Final());

        Assert.Contains("FINAL", sql);
    }

    [Fact]
    public void Sample_GeneratesSampleClause()
    {
        var sql = GenerateSql<TestOrder>(q => q.Sample(0.1));

        Assert.Contains("SAMPLE 0.1", sql);
    }

    #endregion

    #region Complex Query Tests

    [Fact]
    public void CompleteQuery_GeneratesValidSql()
    {
        var sql = GenerateSql<TestOrder>(q =>
            q.Where(o => o.IsActive && o.Amount > 100)
             .OrderByDescending(o => o.CreatedAt)
             .Take(10));

        Assert.StartsWith("SELECT", sql.Trim());
        Assert.Contains("FROM", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("LIMIT", sql);
    }

    [Fact]
    public void Query_WithCapturedVariable_InlinesValue()
    {
        var minAmount = 50.0m;
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.Amount > minAmount));

        Assert.Contains("amount > 50", sql);
    }

    #endregion

    #region Select Projection Tests

    [Fact]
    public void Select_SingleProperty_GeneratesColumnName()
    {
        var context = new ClickHouseQueryContext(null, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var projected = queryable.Select(o => o.Id);
        var sql = projected.ToSql();

        Assert.Contains("SELECT id", sql);
        Assert.DoesNotContain("*", sql);
    }

    [Fact]
    public void Select_AnonymousType_GeneratesColumnsWithAliases()
    {
        var context = new ClickHouseQueryContext(null, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var projected = queryable.Select(o => new { o.Id, o.CustomerName });
        var sql = projected.ToSql();

        Assert.Contains("SELECT", sql);
        Assert.Contains("id", sql);
        Assert.Contains("customer_name", sql);
        Assert.DoesNotContain("*", sql);
    }

    [Fact]
    public void Select_AnonymousTypeWithRename_GeneratesAliasedColumns()
    {
        var context = new ClickHouseQueryContext(null, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var projected = queryable.Select(o => new { OrderId = o.Id, Name = o.CustomerName });
        var sql = projected.ToSql();

        Assert.Contains("id AS", sql);
        Assert.Contains("customer_name AS", sql);
    }

    [Fact]
    public void Select_MultipleColumns_GeneratesCommaSeparatedList()
    {
        var context = new ClickHouseQueryContext(null, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var projected = queryable.Select(o => new { o.Id, o.Amount, o.Quantity });
        var sql = projected.ToSql();

        Assert.Contains("id", sql);
        Assert.Contains("amount", sql);
        Assert.Contains("quantity", sql);
    }

    [Fact]
    public void Select_WithWhere_CombinesCorrectly()
    {
        var context = new ClickHouseQueryContext(null, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var projected = queryable
            .Where(o => o.IsActive)
            .Select(o => new { o.Id, o.CustomerName });
        var sql = projected.ToSql();

        Assert.Contains("SELECT", sql);
        Assert.Contains("id", sql);
        Assert.Contains("customer_name", sql);
        Assert.Contains("WHERE", sql);
        Assert.Contains("is_active", sql);
    }

    [Fact]
    public void Select_WithOrderByAndTake_CombinesCorrectly()
    {
        var context = new ClickHouseQueryContext(null, "test_order", typeof(TestOrder));
        IQueryable<TestOrder> queryable = new ClickHouseQueryable<TestOrder>(context);

        var projected = queryable
            .Select(o => new { o.Id, o.Amount })
            .OrderByDescending(o => o.Amount)
            .Take(10);
        var sql = projected.ToSql();

        Assert.Contains("SELECT", sql);
        Assert.Contains("ORDER BY", sql);
        Assert.Contains("DESC", sql);
        Assert.Contains("LIMIT 10", sql);
    }

    #endregion

    #region Special Character Tests

    [Fact]
    public void Where_StringWithSingleQuote_EscapesQuote()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.CustomerName == "O'Brien"));

        Assert.Contains("O\\'Brien", sql);
    }

    [Fact]
    public void Where_StringWithBackslash_EscapesBackslash()
    {
        var sql = GenerateSql<TestOrder>(q => q.Where(o => o.CustomerName == "C:\\Path"));

        Assert.Contains("C:\\\\Path", sql);
    }

    #endregion
}

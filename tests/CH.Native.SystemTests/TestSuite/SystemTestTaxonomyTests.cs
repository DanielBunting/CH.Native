using System.Reflection;
using Xunit;

namespace CH.Native.SystemTests.TestSuite;

/// <summary>
/// Guards the system-test suite itself: category filters are how CI and local
/// runs select expensive subsets, so missing traits make coverage hard to run.
/// </summary>
[Trait(Categories.Name, Categories.Suite)]
public sealed class SystemTestTaxonomyTests
{
    [Fact]
    public void AllFactAndTheoryTests_HaveCategoryTrait()
    {
        var missing = DiscoverTestMethods()
            .Where(test => !HasCategoryTrait(test.Type) && !HasCategoryTrait(test.Method))
            .Select(test => $"{test.Type.FullName}.{test.Method.Name}")
            .Order()
            .ToArray();

        Assert.True(missing.Length == 0,
            "Every system test must declare a Category trait on the method or class." +
            Environment.NewLine +
            string.Join(Environment.NewLine, missing));
    }

    [Fact]
    public void DeclaredCategoryConstants_AreUsedByAtLeastOneDiscoveredTest()
    {
        var declared = typeof(Categories)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field.IsLiteral && field.FieldType == typeof(string))
            .Where(field => field.Name != nameof(Categories.Name))
            .Select(field => (string)field.GetRawConstantValue()!)
            .Order()
            .ToArray();

        var used = DiscoverTestMethods()
            .SelectMany(test => GetCategoryTraits(test.Type).Concat(GetCategoryTraits(test.Method)))
            .Distinct()
            .Order()
            .ToArray();

        var missing = declared.Except(used).ToArray();

        Assert.True(missing.Length == 0,
            "Every declared system-test category should have at least one test." +
            Environment.NewLine +
            string.Join(Environment.NewLine, missing));
    }

    private static IEnumerable<(Type Type, MethodInfo Method)> DiscoverTestMethods()
    {
        return typeof(SystemTestTaxonomyTests).Assembly
            .GetTypes()
            .Where(type => !type.IsAbstract)
            .SelectMany(type => type
                .GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
                .Where(IsFactOrTheory)
                .Select(method => (type, method)));
    }

    private static bool IsFactOrTheory(MethodInfo method)
    {
        return method.GetCustomAttributes<FactAttribute>(inherit: true).Any();
    }

    private static bool HasCategoryTrait(MemberInfo member)
    {
        return GetCategoryTraits(member).Any();
    }

    private static IEnumerable<string> GetCategoryTraits(MemberInfo member)
    {
        return member
            .GetCustomAttributesData()
            .Where(attribute => attribute.AttributeType == typeof(TraitAttribute))
            .Where(attribute => attribute.ConstructorArguments.Count == 2)
            .Where(attribute => (string?)attribute.ConstructorArguments[0].Value == Categories.Name)
            .Select(attribute => (string)attribute.ConstructorArguments[1].Value!);
    }
}

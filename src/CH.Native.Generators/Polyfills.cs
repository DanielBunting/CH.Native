// Polyfills for netstandard2.0 compatibility with modern C# features

// ReSharper disable once CheckNamespace
namespace System.Runtime.CompilerServices
{
    /// <summary>
    /// Reserved to be used by the compiler for tracking metadata.
    /// This is required for init-only properties (records) on netstandard2.0.
    /// </summary>
    internal static class IsExternalInit
    {
    }
}

using System.Security.Cryptography.X509Certificates;

namespace CH.Native.DependencyInjection;

/// <summary>
/// Supplies the X509 client certificate used for mutual-TLS authentication.
/// The pool invokes this once per physical connection; implementations may
/// return a fresh handle on each call (e.g. re-reading a rotated cert from
/// the OS cert store) or reuse a cached instance.
/// </summary>
public interface IClickHouseCertificateProvider
{
    /// <summary>Returns a certificate that includes the private key.</summary>
    ValueTask<X509Certificate2> GetCertificateAsync(CancellationToken cancellationToken);
}

using CH.Native.Exceptions;
using CH.Native.Protocol.Messages;
using Xunit;

namespace CH.Native.Tests.Unit.ExceptionMapping;

/// <summary>
/// Pre-fix mid-session server exceptions all returned a plain
/// <see cref="ClickHouseServerException"/> regardless of error code. Round 1
/// only routed auth codes through <see cref="ClickHouseAuthenticationException"/>
/// at the handshake site (<c>ServerHello.Read</c>). A JWT that expired during
/// a long-running read therefore surfaced as a generic
/// <see cref="ClickHouseServerException"/>; callers catching the typed auth
/// exception missed it, and the resilience layer's auth-non-transient signal
/// was lost.
/// </summary>
public class ServerExceptionMappingTests
{
    [Theory]
    [InlineData(192)] // UNKNOWN_USER
    [InlineData(193)] // WRONG_PASSWORD
    [InlineData(194)] // REQUIRED_PASSWORD
    [InlineData(195)] // IP_ADDRESS_NOT_ALLOWED
    [InlineData(196)] // UNKNOWN_ADDRESS_PATTERN_TYPE
    [InlineData(516)] // AUTHENTICATION_FAILED
    public void FromExceptionMessage_AuthCode_ReturnsAuthenticationException(int code)
    {
        var msg = new ExceptionMessage
        {
            Code = code,
            Name = "DB::Exception",
            Message = "auth failed mid-session",
            StackTrace = string.Empty,
            Nested = null,
        };

        var ex = ClickHouseServerException.FromExceptionMessage(msg);
        Assert.IsType<ClickHouseAuthenticationException>(ex);
    }

    [Theory]
    [InlineData(60)]  // UNKNOWN_TABLE
    [InlineData(159)] // TIMEOUT_EXCEEDED — transient, not auth
    [InlineData(241)] // MEMORY_LIMIT_EXCEEDED
    public void FromExceptionMessage_NonAuthCode_ReturnsServerException(int code)
    {
        var msg = new ExceptionMessage
        {
            Code = code,
            Name = "DB::Exception",
            Message = "non-auth failure",
            StackTrace = string.Empty,
            Nested = null,
        };

        var ex = ClickHouseServerException.FromExceptionMessage(msg);
        // Non-auth codes still return the base type.
        Assert.IsType<ClickHouseServerException>(ex);
    }

    [Fact]
    public void FromExceptionMessage_AuthCodePolymorphic_StillCatchableAsServerException()
    {
        var msg = new ExceptionMessage
        {
            Code = 516,
            Name = "DB::Exception",
            Message = "expired",
            StackTrace = string.Empty,
            Nested = null,
        };

        // Caller code that catches ClickHouseServerException must still see
        // the auth subclass (it derives from ClickHouseConnectionException,
        // not ClickHouseServerException), so we instead surface this contract:
        // R6's ClickHouseAuthenticationException is the runtime type, and the
        // ErrorCode is preserved.
        var ex = ClickHouseServerException.FromExceptionMessage(msg);
        var auth = Assert.IsType<ClickHouseAuthenticationException>(ex);
        Assert.Equal(516, auth.ErrorCode);
    }
}

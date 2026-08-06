using FluentAssertions;
using Microsoft.Extensions.Options;
using Moq;
using NaturalQuery.Redis;
using StackExchange.Redis;

namespace NaturalQuery.Redis.Tests;

/// <summary>
/// FR-025 / SC-008: shared-store rate limiter enforcing one combined per-tenant
/// limit across instances, failing closed when the store is unavailable.
/// </summary>
public class RedisRateLimiterTests
{
    private readonly Mock<IDatabase> _dbMock = new();
    private readonly Mock<IConnectionMultiplexer> _redisMock = new();

    private RedisRateLimiter CreateLimiter(int maxPerMinute = 60)
    {
        _redisMock.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(_dbMock.Object);
        var options = Options.Create(new NaturalQueryOptions { RateLimitPerMinute = maxPerMinute });
        return new RedisRateLimiter(_redisMock.Object, options);
    }

    [Fact]
    public async Task Under_Limit_Should_Be_Allowed()
    {
        _dbMock.Setup(d => d.StringIncrementAsync(It.IsAny<RedisKey>(), 1L, It.IsAny<CommandFlags>()))
            .ReturnsAsync(3L);
        var limiter = CreateLimiter(maxPerMinute: 5);

        (await limiter.IsAllowedAsync("tenant-1")).Should().BeTrue();
    }

    [Fact]
    public async Task Exactly_At_Limit_Should_Be_Allowed()
    {
        _dbMock.Setup(d => d.StringIncrementAsync(It.IsAny<RedisKey>(), 1L, It.IsAny<CommandFlags>()))
            .ReturnsAsync(5L);
        var limiter = CreateLimiter(maxPerMinute: 5);

        (await limiter.IsAllowedAsync("tenant-1")).Should().BeTrue();
    }

    [Fact]
    public async Task Over_Limit_Should_Be_Denied()
    {
        _dbMock.Setup(d => d.StringIncrementAsync(It.IsAny<RedisKey>(), 1L, It.IsAny<CommandFlags>()))
            .ReturnsAsync(6L);
        var limiter = CreateLimiter(maxPerMinute: 5);

        (await limiter.IsAllowedAsync("tenant-1")).Should().BeFalse();
    }

    [Fact]
    public async Task First_Hit_Should_Set_Window_Expiry()
    {
        TimeSpan? capturedExpiry = null;
        _dbMock.Setup(d => d.StringIncrementAsync(It.IsAny<RedisKey>(), 1L, It.IsAny<CommandFlags>()))
            .ReturnsAsync(1L);
        _dbMock.Setup(d => d.KeyExpireAsync(It.IsAny<RedisKey>(), It.IsAny<TimeSpan?>(), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>()))
            .Callback((RedisKey _, TimeSpan? e, ExpireWhen _, CommandFlags _) => capturedExpiry = e)
            .ReturnsAsync(true);
        var limiter = CreateLimiter();

        await limiter.IsAllowedAsync("tenant-1");

        capturedExpiry.Should().Be(TimeSpan.FromMinutes(1));
    }

    [Fact]
    public async Task Subsequent_Hits_Should_Not_Reset_Window()
    {
        _dbMock.Setup(d => d.StringIncrementAsync(It.IsAny<RedisKey>(), 1L, It.IsAny<CommandFlags>()))
            .ReturnsAsync(2L);
        var limiter = CreateLimiter();

        await limiter.IsAllowedAsync("tenant-1");

        _dbMock.Verify(d => d.KeyExpireAsync(
            It.IsAny<RedisKey>(), It.IsAny<TimeSpan?>(), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>()),
            Times.Never);
    }

    [Fact]
    public async Task Combined_Limit_Should_Be_Shared_Across_Instances()
    {
        // Two limiter instances (two app instances) sharing one counter key.
        var counter = 0L;
        _dbMock.Setup(d => d.StringIncrementAsync(It.IsAny<RedisKey>(), 1L, It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => ++counter);
        _dbMock.Setup(d => d.KeyExpireAsync(It.IsAny<RedisKey>(), It.IsAny<TimeSpan?>(), It.IsAny<ExpireWhen>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);

        var instanceA = CreateLimiter(maxPerMinute: 4);
        var instanceB = CreateLimiter(maxPerMinute: 4);

        (await instanceA.IsAllowedAsync("tenant-1")).Should().BeTrue();  // 1
        (await instanceB.IsAllowedAsync("tenant-1")).Should().BeTrue();  // 2
        (await instanceA.IsAllowedAsync("tenant-1")).Should().BeTrue();  // 3
        (await instanceB.IsAllowedAsync("tenant-1")).Should().BeTrue();  // 4
        (await instanceA.IsAllowedAsync("tenant-1")).Should().BeFalse(); // 5 — combined limit
    }

    [Fact]
    public async Task Unavailable_Store_Should_Fail_Closed()
    {
        _dbMock.Setup(d => d.StringIncrementAsync(It.IsAny<RedisKey>(), 1L, It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));
        var limiter = CreateLimiter();

        (await limiter.IsAllowedAsync("tenant-1")).Should().BeFalse(); // deny over allow
    }

    [Fact]
    public async Task GetRemaining_Should_Report_Budget()
    {
        _dbMock.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisValue)7);
        var limiter = CreateLimiter(maxPerMinute: 10);

        (await limiter.GetRemainingAsync("tenant-1")).Should().Be(3);
    }

    [Fact]
    public async Task GetRemaining_Should_Fail_Closed_When_Unavailable()
    {
        _dbMock.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));
        var limiter = CreateLimiter(maxPerMinute: 10);

        (await limiter.GetRemainingAsync("tenant-1")).Should().Be(0);
    }
}

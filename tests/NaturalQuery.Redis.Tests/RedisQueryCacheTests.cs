using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Moq;
using NaturalQuery.Models;
using NaturalQuery.Redis;
using StackExchange.Redis;

namespace NaturalQuery.Redis.Tests;

/// <summary>
/// FR-024: shared-store cache. An unavailable store degrades to a cache miss
/// (fail-open) so the application keeps running.
/// </summary>
public class RedisQueryCacheTests
{
    private readonly Mock<IDatabase> _dbMock = new();
    private readonly Mock<IConnectionMultiplexer> _redisMock = new();

    private RedisQueryCache CreateCache(int ttlMinutes = 5)
    {
        _redisMock.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(_dbMock.Object);
        var options = Options.Create(new NaturalQueryOptions { CacheTtlMinutes = ttlMinutes });
        return new RedisQueryCache(_redisMock.Object, options);
    }

    [Fact]
    public async Task Get_Should_Deserialize_Stored_Result()
    {
        var stored = JsonSerializer.Serialize(new QueryResult { Sql = "SELECT 1", Title = "t" });
        _dbMock.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(stored);
        var cache = CreateCache();

        var result = await cache.GetAsync("how many users?", "tenant-1");

        result.Should().NotBeNull();
        result!.Sql.Should().Be("SELECT 1");
    }

    [Fact]
    public async Task Get_Should_Return_Null_On_Miss()
    {
        _dbMock.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);
        var cache = CreateCache();

        var result = await cache.GetAsync("q", "tenant-1");

        result.Should().BeNull();
    }

    [Fact]
    public async Task Get_Should_Fail_Open_When_Redis_Unavailable()
    {
        _dbMock.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));
        var cache = CreateCache();

        var result = await cache.GetAsync("q", "tenant-1");

        result.Should().BeNull(); // miss, not an exception
    }

    [Fact]
    public async Task Set_Should_Fail_Open_When_Redis_Unavailable()
    {
        _dbMock.Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(),
                It.IsAny<bool>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));
        var cache = CreateCache();

        var act = () => cache.SetAsync("q", "tenant-1", new QueryResult { Sql = "SELECT 1" });

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task Set_Should_Apply_Configured_Ttl()
    {
        _dbMock
            .Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(),
                It.IsAny<bool>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        var cache = CreateCache(ttlMinutes: 15);

        await cache.SetAsync("q", "tenant-1", new QueryResult { Sql = "SELECT 1" });

        var invocation = _dbMock.Invocations.Single(i => i.Method.Name == "StringSetAsync");
        var expiryArg = invocation.Arguments[2];
        // Newer StackExchange.Redis versions type this parameter as `Expiration`
        // (implicitly convertible from TimeSpan?) rather than `TimeSpan?` directly.
        expiryArg!.ToString().Should().Contain("900"); // 15 minutes = 900 seconds
    }

    [Fact]
    public async Task Keys_Should_Be_Tenant_Scoped()
    {
        var keys = new List<RedisKey>();
        _dbMock.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Callback((RedisKey k, CommandFlags _) => keys.Add(k))
            .ReturnsAsync(RedisValue.Null);
        var cache = CreateCache();

        await cache.GetAsync("same question", "tenant-a");
        await cache.GetAsync("same question", "tenant-b");

        keys.Should().HaveCount(2);
        keys[0].ToString().Should().NotBe(keys[1].ToString());
    }

    [Fact]
    public async Task Same_Question_And_Tenant_Should_Map_To_Same_Key()
    {
        var keys = new List<RedisKey>();
        _dbMock.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Callback((RedisKey k, CommandFlags _) => keys.Add(k))
            .ReturnsAsync(RedisValue.Null);
        var cache = CreateCache();

        await cache.GetAsync("How Many Users?", "tenant-a");
        await cache.GetAsync("  how many users?  ", "tenant-a");

        keys[0].ToString().Should().Be(keys[1].ToString());
    }
}

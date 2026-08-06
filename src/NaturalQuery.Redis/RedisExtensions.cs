using Microsoft.Extensions.DependencyInjection;
using NaturalQuery.Caching;
using NaturalQuery.Extensions;
using NaturalQuery.RateLimiting;
using StackExchange.Redis;

namespace NaturalQuery.Redis;

/// <summary>
/// Registration helpers for the Redis-backed cache and rate limiter.
/// </summary>
public static class RedisExtensions
{
    /// <summary>
    /// Use a Redis-backed query cache shared across application instances.
    /// Fails open (cache miss) when Redis is unavailable.
    /// </summary>
    /// <param name="builder">The NaturalQuery builder.</param>
    /// <param name="configuration">Redis connection string (e.g., "localhost:6379").</param>
    public static NaturalQueryBuilder UseRedisCache(this NaturalQueryBuilder builder, string configuration)
    {
        EnsureConnection(builder.Services, configuration);
        builder.Services.AddSingleton<IQueryCache, RedisQueryCache>();
        return builder;
    }

    /// <summary>
    /// Use a Redis-backed rate limiter enforcing the per-tenant limit across all
    /// application instances combined. Fails closed (deny) when Redis is unavailable.
    /// </summary>
    /// <param name="builder">The NaturalQuery builder.</param>
    /// <param name="configuration">Redis connection string (e.g., "localhost:6379").</param>
    public static NaturalQueryBuilder UseRedisRateLimiter(this NaturalQueryBuilder builder, string configuration)
    {
        EnsureConnection(builder.Services, configuration);
        builder.Services.AddSingleton<IRateLimiter, RedisRateLimiter>();
        return builder;
    }

    /// <summary>
    /// Use both the Redis cache and the Redis rate limiter with a single connection.
    /// </summary>
    public static NaturalQueryBuilder UseRedis(this NaturalQueryBuilder builder, string configuration)
        => builder.UseRedisCache(configuration).UseRedisRateLimiter(configuration);

    private static void EnsureConnection(IServiceCollection services, string configuration)
    {
        // Register the multiplexer once; multiple Use* calls with the same string share it.
        if (services.Any(d => d.ServiceType == typeof(IConnectionMultiplexer)))
            return;

        services.AddSingleton<IConnectionMultiplexer>(_ =>
            ConnectionMultiplexer.Connect(configuration));
    }
}

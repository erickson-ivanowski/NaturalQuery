using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NaturalQuery;
using NaturalQuery.RateLimiting;
using StackExchange.Redis;

namespace NaturalQuery.Redis;

/// <summary>
/// Redis-backed <see cref="IRateLimiter"/> enforcing a per-tenant fixed-window limit
/// across all application instances combined. Fixed-window counting is atomic
/// (INCR + EXPIRE). Fails closed: if Redis is unavailable, requests are denied and
/// the condition is logged (deny over allow), per FR-025.
/// </summary>
public class RedisRateLimiter : IRateLimiter
{
    private const string KeyPrefix = "nq:ratelimit:";
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(1);

    private readonly IConnectionMultiplexer _redis;
    private readonly int _maxPerMinute;
    private readonly ILogger<RedisRateLimiter> _logger;

    public RedisRateLimiter(
        IConnectionMultiplexer redis,
        IOptions<NaturalQueryOptions> options,
        ILogger<RedisRateLimiter>? logger = null)
    {
        _redis = redis;
        _maxPerMinute = options.Value.RateLimitPerMinute > 0 ? options.Value.RateLimitPerMinute : 60;
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisRateLimiter>.Instance;
    }

    /// <inheritdoc />
    public async Task<bool> IsAllowedAsync(string tenantId, CancellationToken ct = default)
    {
        var key = BuildKey(tenantId);
        try
        {
            var db = _redis.GetDatabase();

            // Atomic increment; set the expiry only on the first hit of the window.
            var count = await db.StringIncrementAsync(key);
            if (count == 1)
                await db.KeyExpireAsync(key, Window);

            return count <= _maxPerMinute;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Redis rate limiter unavailable; failing closed (denying request) for tenant {Tenant}.", tenantId);
            return false; // fail-closed
        }
    }

    /// <inheritdoc />
    public async Task<int> GetRemainingAsync(string tenantId, CancellationToken ct = default)
    {
        try
        {
            var db = _redis.GetDatabase();
            var value = await db.StringGetAsync(BuildKey(tenantId));
            var used = value.IsNullOrEmpty ? 0 : (int)value;
            return Math.Max(0, _maxPerMinute - used);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Redis rate limiter unavailable while reading remaining count for tenant {Tenant}.", tenantId);
            return 0; // fail-closed: report no remaining budget
        }
    }

    private static string BuildKey(string tenantId) => $"{KeyPrefix}{tenantId}";
}

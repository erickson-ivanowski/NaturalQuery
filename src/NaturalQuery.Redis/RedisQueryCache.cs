using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NaturalQuery;
using NaturalQuery.Caching;
using NaturalQuery.Models;
using StackExchange.Redis;

namespace NaturalQuery.Redis;

/// <summary>
/// Redis-backed <see cref="IQueryCache"/> so multiple application instances share
/// cached results. Fails open: any Redis error degrades to a cache miss (or a no-op
/// store) so the application keeps running.
/// </summary>
public class RedisQueryCache : IQueryCache
{
    private const string KeyPrefix = "nq:cache:";

    private readonly IConnectionMultiplexer _redis;
    private readonly TimeSpan _ttl;
    private readonly ILogger<RedisQueryCache> _logger;

    public RedisQueryCache(
        IConnectionMultiplexer redis,
        IOptions<NaturalQueryOptions> options,
        ILogger<RedisQueryCache>? logger = null)
    {
        _redis = redis;
        _ttl = TimeSpan.FromMinutes(options.Value.CacheTtlMinutes > 0 ? options.Value.CacheTtlMinutes : 5);
        _logger = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<RedisQueryCache>.Instance;
    }

    /// <inheritdoc />
    public async Task<QueryResult?> GetAsync(string question, string? tenantId, CancellationToken ct = default)
    {
        try
        {
            var db = _redis.GetDatabase();
            var value = await db.StringGetAsync(BuildKey(question, tenantId));
            if (value.IsNullOrEmpty)
                return null;

            return JsonSerializer.Deserialize<QueryResult>(value!);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Redis cache GET failed; treating as a miss.");
            return null; // fail-open
        }
    }

    /// <inheritdoc />
    public async Task SetAsync(string question, string? tenantId, QueryResult result, CancellationToken ct = default)
    {
        try
        {
            var db = _redis.GetDatabase();
            var json = JsonSerializer.Serialize(result);
            await db.StringSetAsync(BuildKey(question, tenantId), json, _ttl);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Redis cache SET failed; result not cached.");
            // fail-open: caching is best-effort
        }
    }

    /// <inheritdoc />
    public async Task InvalidateAsync(string? tenantId = null, CancellationToken ct = default)
    {
        try
        {
            var db = _redis.GetDatabase();
            var pattern = tenantId == null ? $"{KeyPrefix}*" : $"{KeyPrefix}{tenantId}:*";

            foreach (var endpoint in _redis.GetEndPoints())
            {
                var server = _redis.GetServer(endpoint);
                foreach (var key in server.Keys(pattern: pattern))
                    await db.KeyDeleteAsync(key);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Redis cache invalidation failed.");
        }
    }

    private static string BuildKey(string question, string? tenantId)
    {
        var tenant = tenantId ?? "global";
        var raw = $"{tenant}:{question.Trim().ToLowerInvariant()}";
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(raw)));
        return $"{KeyPrefix}{tenant}:{hash}";
    }
}

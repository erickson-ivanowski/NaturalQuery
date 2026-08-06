using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace NaturalQuery.RateLimiting;

/// <summary>
/// In-memory sliding window rate limiter. Tracks requests per tenant
/// in a 1-minute window. Thread-safe via ConcurrentDictionary.
/// Memory is bounded under unbounded distinct tenant identifiers: when the
/// tracked-tenant count exceeds a soft cap, entries idle for at least two full
/// windows are evicted. The sweep runs at most once per second (amortized), and
/// an active tenant never loses its window mid-minute.
/// </summary>
public class InMemoryRateLimiter : IRateLimiter
{
    /// <summary>Eviction engages only when more tenants than this are tracked.</summary>
    private const int SoftCap = 10_000;

    /// <summary>An entry is evictable after being idle for two full windows.</summary>
    private static readonly TimeSpan IdleEvictionAge = TimeSpan.FromMinutes(2);

    private readonly int _maxPerMinute;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, TenantWindow> _windows = new();
    private long _lastSweepTicks;

    /// <summary>Initializes the rate limiter with the configured limit from NaturalQueryOptions.</summary>
    public InMemoryRateLimiter(IOptions<NaturalQueryOptions> options)
        : this(options, null)
    {
    }

    /// <summary>
    /// Initializes the rate limiter with an explicit time source (testing / advanced hosting).
    /// </summary>
    public InMemoryRateLimiter(IOptions<NaturalQueryOptions> options, TimeProvider? timeProvider)
    {
        _maxPerMinute = options.Value.RateLimitPerMinute > 0 ? options.Value.RateLimitPerMinute : 60;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Number of tenant windows currently tracked (bounded by eviction).</summary>
    public int TrackedTenantCount => _windows.Count;

    /// <inheritdoc />
    public Task<bool> IsAllowedAsync(string tenantId, CancellationToken ct = default)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        SweepIfDue(now);

        var key = tenantId ?? "global";
        var window = _windows.GetOrAdd(key, _ => new TenantWindow());

        lock (window)
        {
            window.CleanExpired(now);

            if (window.Timestamps.Count >= _maxPerMinute)
            {
                window.LastActivity = now;
                return Task.FromResult(false);
            }

            window.Timestamps.Add(now);
            window.LastActivity = now;
            return Task.FromResult(true);
        }
    }

    /// <inheritdoc />
    public Task<int> GetRemainingAsync(string tenantId, CancellationToken ct = default)
    {
        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var key = tenantId ?? "global";
        if (!_windows.TryGetValue(key, out var window))
            return Task.FromResult(_maxPerMinute);

        lock (window)
        {
            window.CleanExpired(now);
            return Task.FromResult(Math.Max(0, _maxPerMinute - window.Timestamps.Count));
        }
    }

    /// <summary>
    /// Evicts idle entries when over the soft cap. Runs at most once per second so a
    /// request flood pays a negligible amortized cost. Only entries idle for at least
    /// two full windows are removed — an active tenant can never lose its window.
    /// </summary>
    private void SweepIfDue(DateTime now)
    {
        if (_windows.Count <= SoftCap)
            return;

        var nowTicks = now.Ticks;
        var lastSweep = Interlocked.Read(ref _lastSweepTicks);
        if (nowTicks - lastSweep < TimeSpan.TicksPerSecond)
            return;

        // Only one thread performs the sweep for this second
        if (Interlocked.CompareExchange(ref _lastSweepTicks, nowTicks, lastSweep) != lastSweep)
            return;

        var cutoff = now - IdleEvictionAge;
        foreach (var pair in _windows)
        {
            bool idle;
            lock (pair.Value)
            {
                idle = pair.Value.LastActivity < cutoff;
            }

            if (idle)
                _windows.TryRemove(pair.Key, out _);
        }
    }

    private class TenantWindow
    {
        public List<DateTime> Timestamps { get; } = new();

        /// <summary>Last time this tenant was seen (allowed or rejected).</summary>
        public DateTime LastActivity { get; set; }

        public void CleanExpired(DateTime now)
        {
            var cutoff = now.AddMinutes(-1);
            Timestamps.RemoveAll(t => t < cutoff);
        }
    }
}

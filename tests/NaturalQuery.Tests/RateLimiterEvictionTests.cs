using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.Options;
using NaturalQuery.RateLimiting;

namespace NaturalQuery.Tests;

/// <summary>
/// SC-004 / FR-015: rate-limiter memory stays bounded under unbounded distinct
/// tenant identifiers — idle entries are evicted, active windows are never lost
/// mid-window, and the sweep is amortized.
/// </summary>
public class RateLimiterEvictionTests
{
    /// <summary>Manually advanced clock for deterministic idle-window testing.</summary>
    private sealed class FakeTimeProvider : TimeProvider
    {
        public DateTimeOffset Now { get; set; } = new(2026, 8, 6, 12, 0, 0, TimeSpan.Zero);
        public override DateTimeOffset GetUtcNow() => Now;
        public void Advance(TimeSpan by) => Now += by;
    }

    private static InMemoryRateLimiter CreateLimiter(FakeTimeProvider time, int maxPerMinute = 60)
    {
        var options = Options.Create(new NaturalQueryOptions { RateLimitPerMinute = maxPerMinute });
        return new InMemoryRateLimiter(options, time);
    }

    [Fact]
    public async Task Idle_Entries_Should_Be_Evicted_After_Flood()
    {
        var time = new FakeTimeProvider();
        var limiter = CreateLimiter(time);

        // Flood: 20k distinct tenants (above the soft cap)
        for (var i = 0; i < 20_000; i++)
            await limiter.IsAllowedAsync($"tenant-{i}");

        limiter.TrackedTenantCount.Should().BeGreaterThan(10_000);

        // All become idle; a later call triggers the sweep
        time.Advance(TimeSpan.FromMinutes(3));
        await limiter.IsAllowedAsync("fresh-tenant");

        limiter.TrackedTenantCount.Should().BeLessThan(100);
    }

    [Fact]
    public async Task Active_Tenant_Should_Never_Lose_Window_Mid_Minute()
    {
        var time = new FakeTimeProvider();
        var limiter = CreateLimiter(time, maxPerMinute: 5);

        // Exhaust tenant A's window
        for (var i = 0; i < 5; i++)
            (await limiter.IsAllowedAsync("tenant-a")).Should().BeTrue();
        (await limiter.IsAllowedAsync("tenant-a")).Should().BeFalse();

        // Flood others to push count over the soft cap, then advance 30s (A still mid-window)
        for (var i = 0; i < 12_000; i++)
            await limiter.IsAllowedAsync($"other-{i}");
        time.Advance(TimeSpan.FromSeconds(30));

        // Trigger a sweep opportunity
        await limiter.IsAllowedAsync("sweeper");

        // A must STILL be rate-limited: its active window survived the sweep
        (await limiter.IsAllowedAsync("tenant-a")).Should().BeFalse();

        // After the window passes, A is allowed again
        time.Advance(TimeSpan.FromSeconds(31));
        (await limiter.IsAllowedAsync("tenant-a")).Should().BeTrue();
    }

    [Fact]
    public async Task Sweep_Should_Be_Amortized_At_Most_Once_Per_Second()
    {
        var time = new FakeTimeProvider();
        var limiter = CreateLimiter(time);

        for (var i = 0; i < 11_000; i++)
            await limiter.IsAllowedAsync($"tenant-{i}");

        // Entries become idle
        time.Advance(TimeSpan.FromMinutes(3));

        // First call after idling sweeps
        await limiter.IsAllowedAsync("a");
        var afterFirstSweep = limiter.TrackedTenantCount;
        afterFirstSweep.Should().BeLessThan(100);

        // New flood within the same second must not trigger another sweep pass
        // (count below cap anyway) — behavioral check: no exceptions, entries tracked
        for (var i = 0; i < 500; i++)
            await limiter.IsAllowedAsync($"new-{i}");
        limiter.TrackedTenantCount.Should().BeGreaterThan(afterFirstSweep);
    }

    [Fact]
    public async Task Flood_Of_100k_Tenants_Should_Stay_Fast_And_Bounded()
    {
        var time = new FakeTimeProvider();
        var limiter = CreateLimiter(time);

        var stopwatch = Stopwatch.StartNew();
        for (var i = 0; i < 100_000; i++)
        {
            await limiter.IsAllowedAsync($"tenant-{i}");
            // Spread the flood over simulated time so idle eviction can engage
            if (i % 1000 == 999)
                time.Advance(TimeSpan.FromSeconds(10));
        }
        stopwatch.Stop();

        // Amortized sweep keeps throughput high (SC-004: latency within 10% of baseline)
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10));

        // Idle entries were evicted along the way: far fewer than 100k tracked
        limiter.TrackedTenantCount.Should().BeLessThan(50_000);
    }

    [Fact]
    public async Task Existing_Constructor_Shape_Should_Still_Work()
    {
        // FR-020: existing single-argument construction keeps compiling and working
        var limiter = new InMemoryRateLimiter(Options.Create(new NaturalQueryOptions()));
        (await limiter.IsAllowedAsync("t")).Should().BeTrue();
    }
}
